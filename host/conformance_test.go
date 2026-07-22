package host_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echo"
	"github.com/aramase/agentsessions/host"
)

func echoModel(r host.ModelRequest) host.ModelResponse {
	return host.ModelResponse{Text: "echo:" + r.Prompt}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// §9.1 — Record/replay identity + I1: replay reconstructs the recorded outputs and
// invokes the model zero times.
func TestRecordReplayIdentity(t *testing.T) {
	h := host.New(echoModel)
	if err := h.Exec(0, echo.Harness{}, "hello"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	recorded := h.Outputs()
	invAfterExec := h.ModelInvocations()
	if invAfterExec != 1 {
		t.Fatalf("live model invocations after exec = %d, want 1", invAfterExec)
	}

	out, err := h.Replay(echo.Harness{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if h.ModelInvocations() != invAfterExec {
		t.Fatalf("replay invoked the model (I1 violated): %d -> %d", invAfterExec, h.ModelInvocations())
	}
	if !equal(out, recorded) {
		t.Fatalf("replay reconstruction != recorded: %v vs %v", out, recorded)
	}
}

// §9.1 — I0 check: a harness that is NOT a deterministic function of its recorded inputs
// is caught on replay via the model input-hash mismatch, rather than silently producing
// wrong state.
func TestReplayDetectsNondeterminism(t *testing.T) {
	n := 0
	h := host.New(echoModel)
	if err := h.Exec(0, &flaky{&n}, "hello"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	_, err := h.Replay(&flaky{&n})
	if err == nil || !strings.Contains(err.Error(), "I0") {
		t.Fatalf("want an I0 violation on replay, got: %v", err)
	}
}

// flaky changes its model input across runs (reads external state), violating I0.
type flaky struct{ n *int }

func (f *flaky) Run(e host.Effects, input string) error {
	*f.n++
	resp, err := e.Model(host.ModelRequest{Prompt: fmt.Sprintf("%s#%d", input, *f.n)})
	if err != nil {
		return err
	}
	return e.Emit(resp.Text)
}

// §5 — single-writer: a second Exec with a stale expected_last_seq is rejected.
func TestSingleWriterExecCAS(t *testing.T) {
	h := host.New(echoModel)
	if err := h.Exec(0, echo.Harness{}, "a"); err != nil {
		t.Fatalf("first exec: %v", err)
	}
	err := h.Exec(0, echo.Harness{}, "b") // still believes head == 0
	if !errors.Is(err, eventlog.ErrConflict) {
		t.Fatalf("stale expected_last_seq: want ErrConflict, got %v", err)
	}
}

// §9.3(a) — fork creation-state equivalence: a replay-fork at R reconstructs the parent's
// state at R, and continues independently without mutating the parent.
func TestForkCreationEquivalence(t *testing.T) {
	h := host.New(echoModel)
	if err := h.Exec(0, echo.Harness{}, "hello"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	parentOut := h.Outputs()
	R := h.Head()

	child := h.Fork(R)
	childOut, err := child.Replay(echo.Harness{})
	if err != nil {
		t.Fatalf("child replay: %v", err)
	}
	if !equal(childOut, parentOut) {
		t.Fatalf("fork creation-state != parent@R: %v vs %v", childOut, parentOut)
	}

	// Child continues on its own branch; parent is unaffected.
	if err := child.Exec(child.Head(), echo.Harness{}, "world"); err != nil {
		t.Fatalf("child continuation: %v", err)
	}
	if len(h.Outputs()) != len(parentOut) {
		t.Fatalf("parent was mutated by child fork")
	}
}
