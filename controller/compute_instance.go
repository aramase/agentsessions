package controller

import (
	"fmt"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/eventlog"
)

// WithComputeInstance binds the controller to the compute instance the caller placed the session on
// (api.Incarnation.ComputeInstance). Exec records it in the turn's EXECUTION_START. Exec and Resume
// first compare it with the latest instance the journal recorded for the same harness since the
// session's FORK marker, and refuse with api.ErrMemoryStateLost when the two differ, before they
// append anything or run the harness.
//
// sameHarness reports whether a turn recorded under a harness name ran on the harness this controller
// drives (the Placer resolves names through its registry); nil treats every recorded name as this
// harness. An empty instance records nothing and skips the comparison.
//
// Exec compares against the journal it then appends to under its compare-and-swap, so a turn another
// host journaled before this one cannot be missed: either it is in the history the comparison read,
// or the append fails.
func WithComputeInstance(instance string, sameHarness func(recorded string) bool) Option {
	return func(c *Controller) {
		c.computeInstance = instance
		c.sameHarness = sameHarness
	}
}

// LatestComputeInstance returns the compute instance the latest turn in recs recorded (its
// EXECUTION_START.compute_instance) and that record's seq, counting only turns sameHarness
// attributes to the caller's harness (nil counts every turn) and only turns after the last FORK
// marker. Turns that recorded no instance are skipped. It returns "" when no counted turn has one.
//
// A forked child's own compute is a clone, not the parent's instance, so instances its copied
// prefix recorded do not count. Exec and Resume compare the instance they were placed on with it,
// and a stateful Fork passes it to Runtime.Snapshot as the instance the parent must still be.
func LatestComputeInstance(recs []eventlog.Record, sameHarness func(recorded string) bool) (instance string, seq int64) {
	for i := range recs {
		ev := recs[i].Event
		switch {
		case ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleFork:
			instance, seq = "", 0
		case ev.Kind == api.EventExecutionStart && ev.ExecutionStart != nil && ev.ExecutionStart.ComputeInstance != "":
			if sameHarness == nil || sameHarness(ev.ExecutionStart.Harness) {
				instance, seq = ev.ExecutionStart.ComputeInstance, recs[i].Seq
			}
		}
	}
	return instance, seq
}

// checkComputeInstance returns an error wrapping api.ErrMemoryStateLost when recs record, for this
// controller's harness, a latest compute instance (LatestComputeInstance) other than
// c.computeInstance.
func (c *Controller) checkComputeInstance(recs []eventlog.Record) error {
	if c.computeInstance == "" {
		return nil
	}
	last, seq := LatestComputeInstance(recs, c.sameHarness)
	if last == "" || last == c.computeInstance {
		return nil
	}
	return fmt.Errorf("%w: session %q was placed on compute instance %q, but its turn at seq %d ran on instance %q, "+
		"which has been replaced, so its in-RAM state cannot be reconstructed; nothing was journaled",
		api.ErrMemoryStateLost, c.sessionUID, c.computeInstance, seq, last)
}
