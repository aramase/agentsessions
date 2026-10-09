package e2e

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Copy of controller/testdata/v0.1.2/crashed-output.json, embedded for the in-cluster test binary.
// The records were written by the tagged v0.1.2 controller, not synthesized by this test.
//
//go:embed testdata/v012-crashed-output.json
var legacyCrashedOutput []byte

// Exact copies of the corresponding tagged-controller fixtures in controller/testdata/v0.1.2.
//
//go:embed testdata/v012-model-error.json
var legacyModelError []byte

//go:embed testdata/v012-crashed-model.json
var legacyCrashedModel []byte

// TestStatelessReplayOnGVisor is the stateless tier on real substrate: place the echo harness
// (ResumeActor{boot:true}), drive one turn over a direct dial to the actor's PodIP, then replay the
// journal through the same harness and require it byte-identical with the model never invoked (I1).
//
// This is the claim that a session is reconstructible from its log alone, made against real compute
// rather than an in-process fake.
func TestStatelessReplayOnGVisor(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-replay"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(echoTemplate, desc)
	session := uniqueUID("replay")
	log := journal(t).Session(session)
	p := placement.New(backend, echoagent.Model)

	inc, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "hi")}, 0)
	if inc.ID != "" {
		defer stopQuietly(t, backend, inc.ID)
	}
	if err != nil {
		t.Fatalf("placed exec: %v", err)
	}
	t.Logf("placed actor: address=%s worker=%s", inc.Address, inc.Worker)

	live := lastOutput(t, log)
	if live == "" {
		t.Fatal("placed turn produced no output")
	}

	har, closeHar, err := dialActor(inc.Address)
	if err != nil {
		t.Fatalf("replay dial %s: %v", inc.Address, err)
	}
	defer func() { _ = closeHar() }()

	c, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.Replay(ctx, har)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if want := []string{live}; !reflect.DeepEqual(replayed, want) {
		t.Fatalf("replay diverged: got %v want %v", replayed, want)
	}
	if n := c.ModelInvocations(); n != 0 {
		t.Fatalf("replay invoked the model %d times, want 0 (I1)", n)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("chain verify: %v", err)
	}
	t.Log("replay byte-identical, model invocations=0, chain verified")

	t.Run("recorded-harness-version", func(t *testing.T) {
		observed, err := har.Describe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if observed.Version == "" {
			t.Fatal("reference actor did not advertise its version over the wire")
		}
		records, err := log.Read(1)
		if err != nil {
			t.Fatal(err)
		}
		var invocation *api.ExecutionStart
		var prefix []eventlog.Record
		for _, record := range records {
			if record.Event.Kind == api.EventExecutionStart {
				invocation = record.Event.ExecutionStart
			}
			if record.Event.Kind == api.EventEnd {
				break
			}
			prefix = append(prefix, record)
		}
		if invocation == nil || invocation.Harness != observed.ID || invocation.HarnessVersion != observed.Version {
			t.Fatalf("placed marker = %#v, actor descriptor = %#v", invocation, observed)
		}
		if len(prefix) == len(records) {
			t.Fatal("placed execution has no terminal cut")
		}
		for _, version := range []string{"", observed.Version + "-changed"} {
			for _, resume := range []bool{false, true} {
				original := records
				if resume {
					original = prefix
				}
				copyLog := eventlog.AsStore(eventlog.NewFrom(original))
				guarded, err := controller.New(copyLog, echoagent.Model)
				if err != nil {
					t.Fatal(err)
				}
				changed := &versionOverrideHarness{Harness: har, version: version}
				if resume {
					resumed, err := guarded.Resume(ctx, changed)
					if resumed || !errors.Is(err, controller.ErrHarnessVersionMismatch) {
						t.Fatalf("version %q Resume = %v, %v", version, resumed, err)
					}
				} else if _, err := guarded.Replay(ctx, changed); !errors.Is(err, controller.ErrHarnessVersionMismatch) {
					t.Fatalf("version %q Replay = %v", version, err)
				}
				if changed.runs != 0 || guarded.ModelInvocations() != 0 {
					t.Fatal("mismatched version reached actor/model execution")
				}
				after, err := copyLog.Read(1)
				if err != nil || !reflect.DeepEqual(after, original) {
					t.Fatalf("version rejection changed journal: %v", err)
				}
				if err := copyLog.Verify(); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Log("actor's advertised version recorded; known-version mismatches rejected before Run or writes")
	})
	t.Run("v0.1.2-recovery", func(t *testing.T) {
		testLegacyRecoveryOnActor(t, ctx, har)
	})
	t.Run("v0.1.2-superseded-tail", func(t *testing.T) {
		for _, fixture := range []struct {
			name     string
			data     []byte
			finished bool
		}{
			{"errored", legacyModelError, true},
			{"unfinished", legacyCrashedModel, false},
		} {
			t.Run(fixture.name, func(t *testing.T) { testLegacySupersededTailOnActor(t, ctx, har, fixture.data, fixture.finished) })
		}
	})
}

func testLegacySupersededTailOnActor(t *testing.T, ctx context.Context, har api.Harness, data []byte, finished bool) {
	t.Helper()
	var original []eventlog.Record
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy-tail.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	uid := uniqueUID("legacy-tail")
	log := store.Session(uid)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range original {
		got, err := log.Append(record.Seq-1, fence, record.Event)
		if err != nil || !reflect.DeepEqual(got, record) {
			t.Fatalf("copy released record %d: %#v, %v", record.Seq, got, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	log = store.Session(uid)
	modelCalls := 0
	c, err := controller.New(log, func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		modelCalls++
		return echoagent.Model(ctx, req)
	}, controller.WithSessionUID(uid))
	if err != nil {
		t.Fatal(err)
	}
	if finished {
		if resumed, err := c.Resume(ctx, har); resumed || err != nil {
			t.Fatalf("ERROR-finished legacy Resume: %v, %v", resumed, err)
		}
	}
	if err := c.Exec(ctx, har, []api.Message{*api.TextMessage("user", "new")}, int64(len(original))); err != nil {
		t.Fatalf("new Exec after legacy tail: %v", err)
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, before[:len(original)]) {
		t.Fatal("new Exec rewrote released tail")
	}
	if out, err := c.Replay(ctx, har); err != nil || !reflect.DeepEqual(out, []string{"echo:new"}) {
		t.Fatalf("superseded tail Replay over actor wire: %v, %v", out, err)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("superseded-tail Replay changed records: %v", err)
	}
	if modelCalls != 1 {
		t.Fatalf("only new Exec should invoke model: calls=%d", modelCalls)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
	t.Log("released tail preserved; new marker supersedes it; actor-wire replay exact with no extra model call")
}

func testLegacyRecoveryOnActor(t *testing.T, ctx context.Context, har api.Harness) {
	t.Helper()
	var original []eventlog.Record
	if err := json.Unmarshal(legacyCrashedOutput, &original); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	uid := uniqueUID("legacy-replay")
	log := store.Session(uid)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range original {
		got, err := log.Append(record.Seq-1, fence, record.Event)
		if err != nil || !reflect.DeepEqual(got, record) {
			t.Fatalf("copy released record %d: %#v, %v", record.Seq, got, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	log = store.Session(uid)
	modelCalls := 0
	c, err := controller.New(log, func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		modelCalls++
		return echoagent.Model(ctx, req)
	}, controller.WithSessionUID(uid))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(ctx, har); !resumed || err != nil {
		t.Fatalf("released journal Resume over actor wire: %v, %v", resumed, err)
	}
	recovered, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, recovered[:len(original)]) {
		t.Fatal("legacy recovery rewrote released records")
	}
	for _, record := range recovered {
		if record.Event.ExecutionID != "" {
			t.Fatalf("compatibility ID leaked from wire into record %d", record.Seq)
		}
	}
	if modelCalls != 0 {
		t.Fatalf("served legacy recovery invoked model %d times", modelCalls)
	}
	if out, err := c.Replay(ctx, har); err != nil || !reflect.DeepEqual(out, []string{"echo:hello"}) {
		t.Fatalf("released journal Replay over actor wire: %v, %v", out, err)
	}
	if err := c.Exec(ctx, har, []api.Message{*api.TextMessage("user", "new")}, int64(len(recovered))); err != nil {
		t.Fatalf("modern Exec after legacy recovery: %v", err)
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.Replay(ctx, har); err != nil || !reflect.DeepEqual(out, []string{"echo:hello", "echo:new"}) {
		t.Fatalf("mixed journal Replay over actor wire: %v, %v", out, err)
	}
	if modelCalls != 1 {
		t.Fatalf("only new Exec should invoke model: calls=%d", modelCalls)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("mixed Replay changed journal: %v", err)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
	t.Log("released SQLite prefix recovered over actor wire, compatibility ID not journaled, mixed replay exact")
}

// Keep the real wire-backed harness beneath this adapter; changing its advertised contract must
// prevent both completed replay and interrupted recovery from invoking that actor.
type versionOverrideHarness struct {
	api.Harness
	version string
	runs    int
}

func (h *versionOverrideHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	desc, err := h.Harness.Describe(ctx)
	desc.Version = h.version
	return desc, err
}

func (h *versionOverrideHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.runs++
	return h.Harness.Run(ctx, start, sink)
}
