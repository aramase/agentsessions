package conformance_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/sqlitelog"
)

// countModel is a genuinely NONDETERMINISTIC model: every invocation returns a different answer
// (a per-call counter), so re-running it never reproduces a prior response. This is what makes the
// replay guarantee non-trivial — replay must serve the RECORDED answer, not a fresh one.
type countModel struct{ n int }

func (m *countModel) call(req api.ModelRequest) (api.ModelResponse, error) {
	m.n++
	last := ""
	if k := len(req.Messages); k > 0 {
		last = req.Messages[k-1].Text()
	}
	return api.ModelResponse{Message: *api.TextMessage("assistant", fmt.Sprintf("resp#%d:%s", m.n, last))}, nil
}

func openFile(t *testing.T) (*sqlitelog.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conf.db")
	s, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func outputsOf(recs []eventlog.Record) []string {
	var out []string
	for _, r := range recs {
		if r.Event.Kind == api.EventOutput && r.Event.Message != nil {
			out = append(out, r.Event.Message.Text())
		}
	}
	return out
}

// 1. Record/replay identity under genuine nondeterminism: the fresh controller must reconstruct the
// RECORDED output (not a new model answer) and invoke the model zero times (I1/I5).
func TestReplayReconstructsNondeterministicOutput(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	log := s.Session("s")
	m := &countModel{}

	c, err := controller.New(log, m.call)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}
	live, _ := c.Outputs()

	// Prove the model is actually nondeterministic: a direct re-call differs from what was recorded.
	again, _ := m.call(api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", "hi")}})
	if len(live) != 1 || again.Message.Text() == live[0] {
		t.Fatalf("model is not nondeterministic (live=%v again=%q); test can't prove replay matters", live, again.Message.Text())
	}

	c2, err := controller.New(log, m.call) // fresh incarnation
	if err != nil {
		t.Fatal(err)
	}
	replay, err := c2.Replay(context.Background(), echoagent.Harness{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live, replay) {
		t.Fatalf("replay served a fresh nondeterministic answer instead of the recorded one: %v vs %v", replay, live)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatalf("replay invoked the model %d times (I1)", c2.ModelInvocations())
	}
}

// 2. Multi-turn resume-then-continue: each turn runs on a FRESH incarnation (new fence) continuing
// the same durable journal — a session survives "pod restarts" between turns and keeps going.
func TestMultiTurnResumeContinue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.db")

	inputs := []string{"one", "two", "three"}
	var head int64
	for i, in := range inputs {
		s, err := sqlitelog.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		c, err := controller.New(s.Session("s"), echoagent.Model)
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		if err := c.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", in)}, head); err != nil {
			s.Close()
			t.Fatalf("turn %d (%q): %v", i, in, err)
		}
		h, _ := s.Session("s").Head()
		head = h
		s.Close()
	}

	s, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	log := s.Session("s")
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after 3 turns across 3 incarnations: %v", err)
	}
	recs, _ := log.Read(1)
	got := outputsOf(recs)
	want := []string{"echo:one", "echo:two", "echo:three"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("multi-turn outputs=%v want %v", got, want)
	}
}

// 3. Crash-mid-turn recovery (I3/I4): a turn recorded its model result but crashed before END.
// Resume must complete the turn WITHOUT re-invoking the already-recorded model call (at-most-once),
// leaving a single output and a verifiable chain.
func TestCrashMidTurnRedriveAtMostOnce(t *testing.T) {
	s, path := openFile(t)
	m1 := &countModel{}
	c1, err := controller.New(s.Session("s"), m1.call)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}
	recorded, _ := c1.Outputs()
	s.Close()

	// Simulate a crash that lost only the END record (INPUT, MODEL_CALL, OUTPUT are durable).
	truncateFrom(t, path, "s", 4)

	s2, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	log := s2.Session("s")
	m2 := &countModel{}
	c2, err := controller.New(log, m2.call)
	if err != nil {
		t.Fatal(err)
	}
	redrove, err := c2.Resume(context.Background(), echoagent.Harness{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !redrove {
		t.Fatal("expected Resume to re-drive the interrupted turn")
	}
	if c2.ModelInvocations() != 0 {
		t.Fatalf("resume re-invoked the recorded model call %d times (at-most-once / I3 violated)", c2.ModelInvocations())
	}
	recs, _ := log.Read(1)
	if recs[len(recs)-1].Event.Kind != api.EventEnd {
		t.Fatal("resume did not complete the turn (no END)")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after resume: %v", err)
	}
	got := outputsOf(recs)
	if !reflect.DeepEqual(got, recorded) || len(got) != 1 {
		t.Fatalf("resume produced %v want the single recorded output %v (double-execution?)", got, recorded)
	}
}

// 4. Fork equivalence: a fork shares the parent prefix hashes exactly (hash-tree) and both branches
// verify independently on the persistent backend.
func TestForkEquivalence(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	parent := s.Session("parent")
	c, err := controller.New(parent, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	child := s.Session("child")
	if err := controller.Fork(parent, child, head); err != nil {
		t.Fatalf("fork: %v", err)
	}
	pRecs, _ := parent.Read(1)
	cRecs, _ := child.Read(1)
	for i := 0; i < int(head); i++ {
		if cRecs[i].Hash != pRecs[i].Hash {
			t.Fatalf("fork prefix hash diverged at seq %d", i+1)
		}
	}
	if err := parent.Verify(); err != nil {
		t.Fatalf("parent verify: %v", err)
	}
	if err := child.Verify(); err != nil {
		t.Fatalf("child verify: %v", err)
	}
}

// 5. Single-writer: a second incarnation with a stale expected_last_seq is rejected — exactly one
// writer advances the log.
func TestSingleWriter(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	log := s.Session("s")

	c1, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "one")}, 0); err != nil {
		t.Fatal(err)
	}
	c2, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "two")}, 0); err == nil {
		t.Fatal("expected a stale expected_last_seq to be rejected")
	}
}

// truncateFrom deletes records with seq >= fromSeq via a raw connection, simulating a crash that
// lost the tail of the journal (records not yet durably written).
func truncateFrom(t *testing.T, path, session string, fromSeq int64) {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`DELETE FROM events WHERE session = ? AND seq >= ?`, session, fromSeq); err != nil {
		t.Fatal(err)
	}
}

// 6. Language-neutral integrity: an INDEPENDENT, non-Go implementation must reproduce the chain's
// content_hash from the same proto3-JSON + RFC 8785 JCS spec. This is the "any auditor can verify
// the log" provenance wedge — the thing ax (protojson rows, no hash) cannot offer.
func TestIntegrityGoldenVectorCrossImpl(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available; skipping the cross-implementation integrity verifier")
	}
	// go test runs with the working directory set to this package (conformance/).
	out, err := exec.Command(py, filepath.Join("..", "hack", "verify_chain.py")).CombinedOutput()
	if err != nil {
		t.Fatalf("non-Go verifier failed:\n%s", out)
	}
	if !strings.Contains(string(out), "OK:") {
		t.Fatalf("non-Go verifier did not confirm the hash:\n%s", out)
	}
}

// idempotentTool models an external side-effecting tool with tool-side dedup: it counts real
// effects per idempotency key and returns the cached result for a repeated key. Its state stands in
// for the tool's OWN durable dedup (e.g. a payment API keyed on the idempotency key), so — like the
// external effect — it survives the simulated controller crash.
type idempotentTool struct {
	effects map[string]int
	results map[string]api.ToolResult
}

func newIdempotentTool() *idempotentTool {
	return &idempotentTool{effects: map[string]int{}, results: map[string]api.ToolResult{}}
}

func (t *idempotentTool) exec(tc api.ToolCall) (api.ToolResult, error) {
	if r, ok := t.results[tc.IdempotencyKey]; ok {
		return r, nil // deduped: the effect already ran under this key
	}
	t.effects[tc.IdempotencyKey]++
	r := api.ToolResult{ID: tc.ID, Output: map[string]any{"ran": true}}
	t.results[tc.IdempotencyKey] = r
	return r, nil
}

// toolHarness calls one CONTROLLER_MEDIATED tool (fixed idempotency key) then finishes. Being
// deterministic, it re-emits the identical ToolCall on resume.
type toolHarness struct{}

func (toolHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "tool"}, nil
}

func (toolHarness) Run(_ context.Context, _ *api.Start, sink api.EventSink) error {
	_, err := sink.ToolCall(api.ToolCall{
		ID:             "t1",
		Tool:           "charge",
		Mediation:      api.MediationControllerMediated,
		IdempotencyKey: "k1",
	})
	return err
}

// 7. Crash between a tool's execute and its result-append (I3): the host-mediated tool executor
// re-drives the recorded TOOL_CALL under the same idempotency key on resume; an idempotent tool
// runs the external effect AT MOST ONCE. Symmetric to the model re-drive, this closes the tool half
// of the record-before-effect invariant.
func TestCrashMidToolCallAtMostOnce(t *testing.T) {
	s, path := openFile(t)
	tool := newIdempotentTool()
	c1, err := controller.New(s.Session("s"), (&countModel{}).call, controller.WithToolExecutor(tool.exec))
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Exec(context.Background(), toolHarness{}, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatal(err)
	}
	// Journal now: INPUT(1) TOOL_CALL(2) TOOL_RESULT(3) END(4); the effect ran exactly once.
	if tool.effects["k1"] != 1 {
		t.Fatalf("live: effect ran %d times, want 1", tool.effects["k1"])
	}
	s.Close()

	// Crash lost the TOOL_RESULT and END: the intent (TOOL_CALL, seq 2) is durable, the result is
	// not — the exact window §3 protects against.
	truncateFrom(t, path, "s", 3)

	s2, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	log := s2.Session("s")
	// Same tool instance: its dedup state is the external tool's, which the controller crash does
	// not erase.
	c2, err := controller.New(log, (&countModel{}).call, controller.WithToolExecutor(tool.exec))
	if err != nil {
		t.Fatal(err)
	}
	redrove, err := c2.Resume(context.Background(), toolHarness{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !redrove {
		t.Fatal("expected Resume to re-drive the interrupted turn")
	}
	// The executor is CALLED again on re-drive, but the same key dedups it — the external effect
	// fired exactly once across the crash (at-most-once / I3).
	if tool.effects["k1"] != 1 {
		t.Fatalf("resume re-ran the side effect: %d times, want 1 (I3 violated)", tool.effects["k1"])
	}
	recs, _ := log.Read(1)
	if recs[len(recs)-1].Event.Kind != api.EventEnd {
		t.Fatal("resume did not complete the turn (no END)")
	}
	results := 0
	for _, r := range recs {
		if r.Event.Kind == api.EventToolResult {
			results++
		}
	}
	if results != 1 {
		t.Fatalf("journal has %d TOOL_RESULTs, want exactly 1", results)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after resume: %v", err)
	}
}
