package counteragent

import (
	"context"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
)

// captureSink records Output deltas; the counter only ever calls Output.
type captureSink struct{ outputs []string }

func (s *captureSink) Model(api.ModelRequest) (api.ModelResponse, error) {
	return api.ModelResponse{}, nil
}
func (s *captureSink) Output(delta string) error                     { s.outputs = append(s.outputs, delta); return nil }
func (s *captureSink) ToolCall(api.ToolCall) (api.ToolResult, error) { return api.ToolResult{}, nil }
func (s *captureSink) Report(api.ToolResult) error                   { return nil }
func (s *captureSink) Usage(api.Usage) error                         { return nil }

func TestCounterIncrementsInRAM(t *testing.T) {
	h := &Harness{}
	sink := &captureSink{}
	for i := 0; i < 3; i++ {
		if err := h.Run(context.Background(), &api.Start{}, sink); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"1", "2", "3"}; !reflect.DeepEqual(sink.outputs, want) {
		t.Fatalf("outputs=%v want %v (count must persist across turns in RAM)", sink.outputs, want)
	}
}

// TestMemoryRestoreDoesNotReinitialize is the harness-side I4 contract: a memory-restored sandbox
// receives an EMPTY Start.History, and the harness must continue from its in-RAM count, NOT reset to
// zero. The restore is modeled by reusing the SAME instance (its fields = the snapshot-preserved RAM)
// and driving a turn with empty History — the count must continue, not restart. A naive
// `if len(History)==0 { count=0 }` would fail this.
func TestMemoryRestoreDoesNotReinitialize(t *testing.T) {
	h := &Harness{}
	sink := &captureSink{}
	_ = h.Run(context.Background(), &api.Start{}, sink) // count -> 1
	_ = h.Run(context.Background(), &api.Start{}, sink) // count -> 2 (state before a suspend)
	// Memory-restore: same instance (RAM preserved), empty History, one more turn.
	if err := h.Run(context.Background(), &api.Start{History: nil}, sink); err != nil {
		t.Fatal(err)
	}
	if got := sink.outputs[len(sink.outputs)-1]; got != "3" {
		t.Fatalf("post-restore output=%q want %q (empty History must NOT reset the in-RAM count)", got, "3")
	}
}

// TestNonEmptyHistoryIsNotAppliedToState guards the other failure: even a (mistaken) non-empty
// History must not be replayed into the count — the count comes from RAM, not from History. A naive
// `count += len(History)` would double-apply and fail this.
func TestNonEmptyHistoryIsNotAppliedToState(t *testing.T) {
	h := &Harness{}
	sink := &captureSink{}
	_ = h.Run(context.Background(), &api.Start{}, sink) // count -> 1
	if err := h.Run(context.Background(), &api.Start{History: make([]api.Event, 5)}, sink); err != nil {
		t.Fatal(err)
	}
	if got := sink.outputs[len(sink.outputs)-1]; got != "2" {
		t.Fatalf("output=%q want %q (History must NOT be applied to the in-RAM count)", got, "2")
	}
}

func TestCounterRequiresMemorySnapshot(t *testing.T) {
	d, err := (&Harness{}).Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		t.Fatalf("resumability=%q want REQUIRES_MEMORY_SNAPSHOT", d.Capabilities.Resumability)
	}
}
