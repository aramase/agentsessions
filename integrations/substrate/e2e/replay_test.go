package e2e

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

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
