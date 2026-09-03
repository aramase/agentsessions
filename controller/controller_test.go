package controller_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

// echoHarness is a minimal STATELESS_REPLAY harness: it feeds the last input to the model via the
// host-mediated sink and lets the recorded completion be the output. When flaky, it sends a
// different model input on replay, which must trip the I0 hash check.
type echoHarness struct{ flaky bool }

func (h *echoHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID:           "echo",
		Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay, ForkSafe: true},
	}, nil
}

func (h *echoHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	text := ""
	if n := len(s.Inputs); n > 0 {
		text = s.Inputs[n-1].Text()
	}
	if h.flaky {
		text += "-DIFFERENT"
	}
	_, err := sink.Model(ctx, api.ModelRequest{
		Model:    "echo",
		Messages: []api.Message{*api.TextMessage("user", text)},
	})
	return err
}

func echoModel(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	last := ""
	if n := len(req.Messages); n > 0 {
		last = req.Messages[n-1].Text()
	}
	return api.ModelResponse{Message: *api.TextMessage("assistant", "echo:"+last)}, nil
}

func memStore(t *testing.T) eventlog.Store {
	t.Helper()
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s.Session("s")
}

func msg(text string) api.Message { return *api.TextMessage("user", text) }

// TestExecReplayIdentityAcrossRestart is the load-bearing proof at the controller level: a turn is
// executed and persisted by one controller ("pod A"), the process is torn down, and a fresh
// controller ("pod B") reopens the durable journal and replays it byte-identically with ZERO model
// invocations (I1). This is suspend-A / resume-B with real persistence + real mediation.
func TestExecReplayIdentityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")

	s1, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := controller.New(s1.Session("sess"), echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	liveOut, err := c1.Outputs()
	if err != nil {
		t.Fatal(err)
	}
	s1.Close() // pod A dies

	s2, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	log2 := s2.Session("sess")
	c2, err := controller.New(log2, echoModel) // new incarnation: fence advances
	if err != nil {
		t.Fatal(err)
	}
	replayOut, err := c2.Replay(context.Background(), &echoHarness{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !reflect.DeepEqual(liveOut, replayOut) {
		t.Fatalf("replay != live: %v vs %v", replayOut, liveOut)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatalf("replay invoked the model %d times (I1 violated)", c2.ModelInvocations())
	}
	if err := log2.Verify(); err != nil {
		t.Fatalf("verify after restart+replay: %v", err)
	}
	if len(liveOut) != 1 || liveOut[0] != "echo:hi" {
		t.Fatalf("unexpected output: %v", liveOut)
	}
}

// TestInMemoryBackend proves the controller runs identically on the in-memory log via the
// eventlog.AsStore adapter — the eventlog/sqlitelog unification behind one interface.
func TestInMemoryBackend(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	live, _ := c.Outputs()

	c2, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := c2.Replay(context.Background(), &echoHarness{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live, rep) {
		t.Fatalf("in-memory replay != live: %v vs %v", rep, live)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatal("in-memory replay invoked the model (I1)")
	}
}

// TestReplayI0Mismatch: a harness that sends different model input on replay must be caught by the
// recorded-input-hash check, not silently produce wrong state.
func TestReplayI0Mismatch(t *testing.T) {
	log := memStore(t)
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	c2, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Replay(context.Background(), &echoHarness{flaky: true}); err == nil || !strings.Contains(err.Error(), "I0") {
		t.Fatalf("want I0 mismatch, got %v", err)
	}
}

// TestWithFence proves the placement layer's fence binding: a controller built WithFence(k) appends
// under the SUPPLIED fence k rather than minting its own, so once a later incarnation mints k+1 via
// NewFence(), the k-bound controller is fenced out — the fresh-pod-supersedes-zombie guarantee,
// driven by the caller's fence instead of controller.New's self-mint.
func TestWithFence(t *testing.T) {
	log := memStore(t)

	// Incarnation A: the placer mints a fence and binds controller A to it.
	fA, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	cA, err := controller.New(log, echoModel, controller.WithFence(fA))
	if err != nil {
		t.Fatal(err)
	}
	if err := cA.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatalf("controller bound to the current fence should append: %v", err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	// Incarnation B supersedes A: a fresh fence (fA+1) is minted from the log.
	if _, err := log.NewFence(); err != nil {
		t.Fatal(err)
	}
	// A controller still bound to the stale fence fA must be fenced out on its next append. The
	// expected_last_seq is correct (head), so the failure isolates the FENCE, not the CAS.
	cStale, err := controller.New(log, echoModel, controller.WithFence(fA))
	if err != nil {
		t.Fatal(err)
	}
	if err := cStale.Exec(context.Background(), &echoHarness{}, []api.Message{msg("again")}, head); !errors.Is(err, eventlog.ErrFenced) {
		t.Fatalf("a controller bound to a superseded fence must be fenced out, got %v", err)
	}
}

// TestSingleWriterCAS: a second turn with a stale expected_last_seq is rejected at the INPUT append.
func TestSingleWriterCAS(t *testing.T) {
	log := memStore(t)
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &echoHarness{}, []api.Message{msg("again")}, 0); !errors.Is(err, eventlog.ErrConflict) {
		t.Fatalf("want ErrConflict on stale expected_last_seq, got %v", err)
	}
}

// TestForkSharesPrefixDivergesAfter: a fork copies the parent prefix with identical hashes (the
// hash-tree property) and links its first new event to parent@head.
func TestForkSharesPrefixDivergesAfter(t *testing.T) {
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := s.Session("parent")
	c, err := controller.New(parent, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &echoHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	head, err := parent.Head()
	if err != nil {
		t.Fatal(err)
	}

	child := s.Session("child")
	if err := controller.Fork(parent, child, head); err != nil {
		t.Fatalf("fork: %v", err)
	}
	pRecs, _ := parent.Read(1)
	cRecs, _ := child.Read(1)
	for i := 0; i < int(head); i++ {
		if cRecs[i].Hash != pRecs[i].Hash {
			t.Fatalf("fork prefix hash diverged at seq %d: %s != %s", i+1, cRecs[i].Hash, pRecs[i].Hash)
		}
	}
	if int64(len(cRecs)) != head+1 {
		t.Fatalf("child length = %d, want %d", len(cRecs), head+1)
	}
	marker := cRecs[head].Event
	if marker.Kind != api.EventLifecycle || marker.Lifecycle == nil || marker.Lifecycle.Kind != api.LifecycleFork {
		t.Fatalf("expected LIFECYCLE_FORK marker, got %+v", marker)
	}
	if cRecs[head].PrevHash != pRecs[head-1].Hash {
		t.Fatalf("fork marker prev_hash does not link to parent@%d", head)
	}
	if err := child.Verify(); err != nil {
		t.Fatalf("child verify: %v", err)
	}
}

// TestCanPlace: the load-bearing placement rule — a memory-snapshot harness cannot run on a plain
// pod, a stateless-replay harness runs anywhere.
func TestCanPlace(t *testing.T) {
	pod := api.RuntimeCapabilities{MemorySnapshot: false}
	microvm := api.RuntimeCapabilities{MemorySnapshot: true}
	stateless := api.Capabilities{Resumability: api.ResumabilityStatelessReplay}
	stateful := api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}

	if !controller.CanPlace(stateless, pod) {
		t.Fatal("stateless-replay harness must run on a plain pod")
	}
	if controller.CanPlace(stateful, pod) {
		t.Fatal("memory-snapshot harness must NOT run on a plain pod")
	}
	if !controller.CanPlace(stateful, microvm) {
		t.Fatal("memory-snapshot harness must run on a snapshot-capable runtime")
	}
}

// divergentHarness optionally makes NO effect calls, to exercise replay under-consumption.
type divergentHarness struct{ skipModel bool }

func (h *divergentHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "divergent", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h *divergentHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if h.skipModel {
		return nil // diverge: consume nothing on replay
	}
	text := ""
	if n := len(s.Inputs); n > 0 {
		text = s.Inputs[n-1].Text()
	}
	_, err := sink.Model(ctx, api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", text)}})
	return err
}

// TestReplayUnderConsumptionDetected: a harness that requests fewer effects on replay than were
// journaled (took a shorter path) must be caught, not silently return truncated outputs.
func TestReplayUnderConsumptionDetected(t *testing.T) {
	log := memStore(t)
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &divergentHarness{}, []api.Message{msg("hi")}, 0); err != nil {
		t.Fatal(err)
	}
	c2, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Replay(context.Background(), &divergentHarness{skipModel: true}); !errors.Is(err, controller.ErrReplayDiverged) {
		t.Fatalf("want ErrReplayDiverged on under-consumption, got %v", err)
	}
}

// outputHarness streams a fixed string directly via Output (not through the model).
type outputHarness struct{ text string }

func (h *outputHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "output", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay, Streaming: true}}, nil
}

func (h *outputHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	return sink.Output(ctx, h.text)
}

// TestReplayOutputDivergenceDetected: output emitted directly via Output (not the model) must match
// the journal on replay — symmetric with the Model input-hash check — and identical output replays
// cleanly.
func TestReplayOutputDivergenceDetected(t *testing.T) {
	log := memStore(t)
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), &outputHarness{text: "hello"}, nil, 0); err != nil {
		t.Fatal(err)
	}

	c2, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Replay(context.Background(), &outputHarness{text: "goodbye"}); err == nil || !strings.Contains(err.Error(), "output mismatch") {
		t.Fatalf("want output mismatch on divergent replay, got %v", err)
	}

	c3, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c3.Replay(context.Background(), &outputHarness{text: "hello"})
	if err != nil {
		t.Fatalf("faithful replay failed: %v", err)
	}
	if len(out) != 1 || out[0] != "hello" {
		t.Fatalf("unexpected replay output: %v", out)
	}
}
