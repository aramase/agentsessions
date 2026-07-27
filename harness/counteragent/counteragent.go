// Package counteragent is a REQUIRES_MEMORY_SNAPSHOT api.Harness: it holds an in-RAM counter whose
// state is, by design, NOT reconstructable from the event log. It exists to prove axis-2 — memory
// continuity across suspend/restore that STATELESS_REPLAY cannot provide, and that a plain pod (a
// filesystem-only runtime) must honestly refuse via CanPlace. It is the counterpart to echoagent.
package counteragent

import (
	"context"
	"strconv"
	"sync"

	"github.com/aramase/agentsessions/api"
)

// Harness is the in-RAM counter BYOH. Its state lives ONLY in process memory, so it survives
// suspend/resume solely via a memory snapshot (ResumeActor{boot:false}), never via journal replay.
// Use a pointer (the count must persist across turns, and be captured by the memory snapshot).
type Harness struct {
	mu    sync.Mutex
	count int
}

// Describe declares REQUIRES_MEMORY_SNAPSHOT: the harness holds in-process state the runtime must be
// able to snapshot. CanPlace refuses it on a filesystem-only backend (runtime/local, a plain pod);
// substrate accepts it (MemorySnapshot:true). Not ForkSafe: in-RAM state is not replay-reconstructable.
func (h *Harness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID: "counter",
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityRequiresMemorySnapshot,
			ForkSafe:     false,
		},
	}, nil
}

// Run performs one turn: increment the in-RAM counter and emit its value.
//
// I4 contract (the load-bearing rule for axis-2): the counter is NEVER re-initialized from
// s.History. History is empty on a memory-restored sandbox (ResumeActor{boot:false}); reconstructing
// the count from an empty History, or resetting it to zero because History is empty, would DESTROY
// the continuity the snapshot preserved (reset to 0 / double-apply). The count is pure process RAM:
// the Go zero value (0) on a fresh boot, the snapshot-restored value after a memory-restore. So we
// only ever increment — we do not read s.History for state. That a fresh boot starts at 0 (state
// lost, not replayed) is the honest reason this harness needs a memory snapshot at all.
func (h *Harness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	h.mu.Lock()
	h.count++
	n := h.count
	h.mu.Unlock()
	return sink.Output(strconv.Itoa(n))
}
