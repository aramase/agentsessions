// Package placement wires the Sessions layer to the Runtime compute SPI. The Placer owns the
// incarnation lifecycle for a session: it drives Runtime.Create, mints the fence from the log (the
// single fence authority), stamps it on the incarnation, and binds a controller to that fence. Keeping
// this seam out of session.Service keeps the gRPC layer thin and the SPI logic unit-testable, and lets
// cmd/agentnode reuse it.
package placement

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrUnplaceable is returned when a harness requires a capability the chosen backend cannot provide
// (e.g. REQUIRES_MEMORY_SNAPSHOT on a filesystem-only pod). The gate runs BEFORE Create, so an
// unplaceable harness never provisions compute or writes to the log; session.Service surfaces it as
// codes.FailedPrecondition.
var ErrUnplaceable = errors.New("placement: harness cannot be placed on this runtime")

// Backend is the compute Runtime the Placer drives. In the in-process M0 path it also provides the
// harness handle directly via Harness(); the socket variant (design note §10 step 6) will instead have
// the Placer dial Incarnation.Address to build a ClientHarness. runtime/local satisfies this.
type Backend interface {
	api.Runtime
	Harness() api.Harness
}

// Placer owns the incarnation lifecycle: Create the compute, mint+bind the fence, drive the controller.
type Placer struct {
	backend Backend
	model   controller.ModelFunc
}

// New builds a Placer over a compute backend and the live model.
func New(backend Backend, model controller.ModelFunc) *Placer {
	return &Placer{backend: backend, model: model}
}

// Exec places one turn: Create the incarnation, mint the fence from the log and stamp it on the
// incarnation, bind a controller to that same token, and drive the (placed) harness. The log stays the
// single fence authority; the returned incarnation carries the fence for Suspend/Resume (step 5).
func (p *Placer) Exec(ctx context.Context, log eventlog.Store, sessionUID string, inputs []api.Message, expectedLastSeq int64) (api.Incarnation, error) {
	// Placement gate (honest degradation): refuse a harness the backend cannot host BEFORE
	// provisioning any compute or writing to the log — e.g. a REQUIRES_MEMORY_SNAPSHOT harness on a
	// filesystem-only backend — so it fails fast at the API instead of mid-run.
	desc, err := p.backend.Harness().Describe(ctx)
	if err != nil {
		return api.Incarnation{}, err
	}
	if !controller.CanPlace(desc.Capabilities, p.backend.Capabilities()) {
		return api.Incarnation{}, fmt.Errorf("%w: harness %q needs %s but the runtime provides MemorySnapshot=%v",
			ErrUnplaceable, desc.ID, desc.Capabilities.Resumability, p.backend.Capabilities().MemorySnapshot)
	}
	inc, err := p.backend.Create(ctx, &api.SessionSpec{SessionUID: sessionUID})
	if err != nil {
		return api.Incarnation{}, err
	}
	fence, err := log.NewFence()
	if err != nil {
		return api.Incarnation{}, err
	}
	inc.FenceToken = fence // Placer-owned: the incarnation carries the token Suspend/Resume will need
	c, err := controller.New(log, p.model, controller.WithFence(fence))
	if err != nil {
		return api.Incarnation{}, err
	}
	if err := c.Exec(ctx, p.backend.Harness(), inputs, expectedLastSeq); err != nil {
		return inc, err
	}
	return inc, nil
}

// Suspend snapshots the incarnation to external storage, records the SnapshotRef in a SUSPEND
// lifecycle event so Resume can recover it from the tamper-evident chain (§5.1), then frees the
// worker via Stop. Snapshot and Stop key on the session id, so a minimal incarnation suffices.
func (p *Placer) Suspend(ctx context.Context, log eventlog.Store, sessionUID string) (api.SnapshotRef, error) {
	inc := api.Incarnation{ID: sessionUID}
	ref, err := p.backend.Snapshot(ctx, inc, api.SnapshotExternal)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	if err := appendLifecycle(log, api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}); err != nil {
		return api.SnapshotRef{}, err
	}
	if err := p.backend.Stop(ctx, inc); err != nil {
		return api.SnapshotRef{}, err
	}
	return ref, nil
}

// Resume recovers the recorded SnapshotRef, Restores an incarnation, mints a NEW fence (superseding
// any zombie writer), binds a controller to it, re-drives any interrupted turn (replay for a
// filesystem-only backend), and records a RESUME marker. A session with no prior SUSPEND (crash mid
// turn) falls back to re-provisioning from the session handle.
func (p *Placer) Resume(ctx context.Context, log eventlog.Store, sessionUID string) error {
	ref, err := lastSuspendRef(log, sessionUID)
	if err != nil {
		return err
	}
	inc, err := p.backend.Restore(ctx, ref)
	if err != nil {
		return err
	}
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	inc.FenceToken = fence
	c, err := controller.New(log, p.model, controller.WithFence(fence))
	if err != nil {
		return err
	}
	if _, err := c.Resume(ctx, p.backend.Harness()); err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	// RESUME marker under the incarnation's fence (controller.Resume used the same token).
	_, err = log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleResume}})
	return err
}

// Fork provisions the child's compute (replay-fork for filesystem-only backends) and copies the
// parent's log prefix up to atSeq so the child shares the parent's hash chain.
func (p *Placer) Fork(ctx context.Context, parent, child eventlog.Store, parentUID, childUID string, atSeq int64) error {
	if _, err := p.backend.Fork(ctx, api.SnapshotRef{Local: parentUID}, api.ForkOpts{ChildSessionUID: childUID}); err != nil {
		return err
	}
	return controller.Fork(parent, child, atSeq)
}

// appendLifecycle mints a fresh fence (superseding any prior writer) and records a lifecycle event.
func appendLifecycle(log eventlog.Store, lc api.Lifecycle) error {
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	_, err = log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &lc})
	return err
}

// lastSuspendRef returns the SnapshotRef from the most recent SUSPEND event, or — when there is none
// (a crash mid-turn, not a clean suspend) — a trivial ref naming the session so a filesystem-only
// backend re-provisions and replays the journal.
func lastSuspendRef(log eventlog.Store, sessionUID string) (api.SnapshotRef, error) {
	recs, err := log.Read(1)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	for i := len(recs) - 1; i >= 0; i-- {
		ev := recs[i].Event
		if ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleSuspend && ev.Lifecycle.Snapshot != nil {
			return *ev.Lifecycle.Snapshot, nil
		}
	}
	return api.SnapshotRef{Local: sessionUID}, nil
}
