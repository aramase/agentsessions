package controller

import (
	"fmt"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/eventlog"
)

// WithComputeInstance binds the controller to the compute instance the caller placed the session on
// (api.Incarnation.ComputeInstance). Exec records it in the turn's EXECUTION_START. Exec and Resume
// first compare it with the latest instance the journal recorded for the same harness since the
// session's FORK marker, or the clone that marker recorded (LatestComputeInstance), and refuse with
// api.ErrMemoryStateLost when the two differ, before they append anything or run the harness.
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

// LatestComputeInstance returns the compute instance the session's latest record of one names, and
// that record's seq. Only records after the last FORK marker count, plus the marker itself:
//
//   - a turn's EXECUTION_START.compute_instance, counting only turns sameHarness attributes to the
//     caller's harness (nil counts every turn);
//   - the FORK marker's Lifecycle.compute_instance, the clone a stateful fork made for the
//     child. It is attributed like a turn with no recorded harness name (sameHarness("")),
//     since the clone runs the session's own harness.
//
// Records that name no instance are skipped. It returns "" when no counted record names one: a
// session with no memory-tier turn, or a fork child whose marker predates the field (or a stateless
// fork) and that has not run a turn yet.
//
// A forked child's own compute is a clone, not the parent's instance, so instances its copied
// prefix recorded do not count. Exec and Resume compare the instance they were placed on with it,
// and Suspend and a stateful Fork pass it to Runtime.Snapshot as the instance the session must
// still be.
func LatestComputeInstance(recs []eventlog.Record, sameHarness func(recorded string) bool) (instance string, seq int64) {
	instance, seq, _ = latestComputeInstance(recs, sameHarness)
	return instance, seq
}

// latestComputeInstance is LatestComputeInstance, and also reports whether the instance came from
// the FORK marker rather than a turn.
func latestComputeInstance(recs []eventlog.Record, sameHarness func(string) bool) (
	instance string, seq int64, fork bool,
) {
	counts := func(recorded string) bool { return sameHarness == nil || sameHarness(recorded) }
	for i := range recs {
		ev := recs[i].Event
		switch {
		case ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleFork:
			instance, seq, fork = "", 0, false
			if ev.Lifecycle.ComputeInstance != "" && counts("") {
				instance, seq, fork = ev.Lifecycle.ComputeInstance, recs[i].Seq, true
			}
		case ev.Kind == api.EventExecutionStart && ev.ExecutionStart != nil && ev.ExecutionStart.ComputeInstance != "":
			if counts(ev.ExecutionStart.Harness) {
				instance, seq, fork = ev.ExecutionStart.ComputeInstance, recs[i].Seq, false
			}
		}
	}
	return instance, seq, fork
}

// checkComputeInstance returns an error wrapping api.ErrMemoryStateLost when recs record, for this
// controller's harness, a latest compute instance (LatestComputeInstance) other than
// c.computeInstance.
func (c *Controller) checkComputeInstance(recs []eventlog.Record) error {
	if c.computeInstance == "" {
		return nil
	}
	last, seq, fork := latestComputeInstance(recs, c.sameHarness)
	if last == "" || last == c.computeInstance {
		return nil
	}
	recorded := fmt.Sprintf("its turn at seq %d ran on instance %q", seq, last)
	if fork {
		recorded = fmt.Sprintf("its FORK marker at seq %d recorded the clone %q it was forked onto",
			seq, last)
	}
	return fmt.Errorf("%w: session %q was placed on compute instance %q, but %s, "+
		"which has been replaced, so its in-RAM state cannot be reconstructed; nothing was journaled",
		api.ErrMemoryStateLost, c.sessionUID, c.computeInstance, recorded)
}
