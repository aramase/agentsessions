package session_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// countingHarness is echoagent that counts the turns it runs.
type countingHarness struct {
	echoagent.Harness
	runs *atomic.Int32
}

func (h countingHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	return h.Harness.Run(ctx, start, sink)
}

// forkMarkerInstance returns the compute instance the session's last FORK marker recorded.
func forkMarkerInstance(t *testing.T, store *sqlitelog.Store, sess string) string {
	t.Helper()
	recs, err := store.Session(sess).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(recs) - 1; i >= 0; i-- {
		if lc := recs[i].Event.Lifecycle; lc != nil && lc.Kind == api.LifecycleFork {
			return lc.ComputeInstance
		}
	}
	t.Fatalf("session %q has no FORK marker", sess)
	return ""
}

// lastStartInstance returns the compute instance the session's last EXECUTION_START recorded.
func lastStartInstance(t *testing.T, store *sqlitelog.Store, sess string) string {
	t.Helper()
	recs, err := store.Session(sess).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(recs) - 1; i >= 0; i-- {
		if start := recs[i].Event.ExecutionStart; start != nil {
			return start.ComputeInstance
		}
	}
	t.Fatalf("session %q has no EXECUTION_START", sess)
	return ""
}

// A memory-tier fork child records the UID of the actor it was cloned onto on its FORK marker, so
// its compute is compared before its first turn as it is after one. When the clone is deleted out of
// band and another actor is created under the child's name before that turn, the replacement holds
// none of the cloned RAM: Exec, Resume, Suspend and Fork of the child are FAILED_PRECONDITION, naming
// the child, and journal, snapshot and create nothing; the refused Exec and Resume never reach the
// harness. A sibling whose clone is untouched runs its first turn on the clone, unchanged.
func TestForkChildWhoseCloneIsReplacedBeforeItsFirstTurnIsRefused(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"exec", "resume", "suspend", "fork"} {
		t.Run(op, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			var runs atomic.Int32
			c := newMemoryHost(t, ctl, store, countingHarness{runs: &runs})
			parent := mustCreate(t, c)
			if err := execHi(c, parent); err != nil {
				t.Fatalf("parent turn: %v", err)
			}
			resp, err := c.Fork(ctx, &v1.ForkRequest{Session: parent, Count: 2})
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			replaced := resp.GetChildren()[0].GetMetadata().GetUid()
			sibling := resp.GetChildren()[1].GetMetadata().GetUid()
			ctl.mu.Lock()
			clone, siblingClone := ctl.uids[replaced], ctl.uids[sibling]
			ctl.mu.Unlock()
			if got := forkMarkerInstance(t, store, replaced); got == "" || got != clone {
				t.Fatalf("child FORK marker records instance %q, want its clone's UID %q", got, clone)
			}

			ref := substrate.ActorRef{Atespace: "space", Name: replaced}
			if err := ctl.DeleteActor(ctx, ref); err != nil {
				t.Fatal(err)
			}
			if err := ctl.CreateActor(ctx, ref, substrate.ObjectRef{Atespace: "space", Name: "mem"}); err != nil {
				t.Fatal(err)
			}
			ctl.mu.Lock()
			created, suspends, replacement := ctl.created, ctl.suspends, ctl.uids[replaced]
			ctl.mu.Unlock()
			if replacement == clone {
				t.Fatalf("replacement actor reports the clone's UID %q; the test needs a new one", clone)
			}
			head, err := store.Session(replaced).Head()
			if err != nil {
				t.Fatal(err)
			}
			runsBefore := runs.Load()

			switch op {
			case "exec":
				err = execHi(c, replaced)
			case "resume":
				_, err = c.Resume(ctx, &v1.ResumeRequest{Session: replaced})
			case "suspend":
				_, err = c.Suspend(ctx, &v1.SuspendRequest{Session: replaced})
			case "fork":
				_, err = c.Fork(ctx, &v1.ForkRequest{Session: replaced, Count: 1})
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("%s of a fork child whose clone was replaced before its first turn: want FailedPrecondition, got %v", op, err)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, replaced) || !strings.Contains(msg, "replaced") {
				t.Errorf("error %q does not name the child and the replaced instance", msg)
			}
			if got, err := store.Session(replaced).Head(); err != nil || got != head {
				t.Errorf("child journal head after the refused %s = %d (err %v), want %d: nothing may be journaled", op, got, err, head)
			}
			if got := runs.Load(); got != runsBefore {
				t.Errorf("harness ran %d turns on the replacement actor, want none", got-runsBefore)
			}
			ctl.mu.Lock()
			gotCreated, gotSuspends := ctl.created, ctl.suspends
			ctl.mu.Unlock()
			if gotCreated != created || gotSuspends != suspends {
				t.Errorf("actors created %d->%d, suspends %d->%d: the refused %s may create or snapshot nothing",
					created, gotCreated, suspends, gotSuspends, op)
			}

			// The sibling's clone was not touched: its first turn runs on it and records its UID.
			if err := execHi(c, sibling); err != nil {
				t.Fatalf("first turn of the untouched sibling: %v", err)
			}
			if got := lastStartInstance(t, store, sibling); got != siblingClone {
				t.Errorf("sibling's first turn recorded instance %q, want its clone's UID %q", got, siblingClone)
			}
		})
	}
}

// A FORK marker written before Lifecycle.compute_instance existed (or by a stateless fork) records
// no clone, so the child's first turn has nothing to compare with: an actor under the child's name
// is attached to as before, and the turn records that actor's UID for the turns after it.
func TestForkChildWithoutARecordedCloneIsNotCompared(t *testing.T) {
	ctx := context.Background()
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := newMemoryHost(t, ctl, store, echoagent.Harness{})
	parent := mustCreate(t, c)
	if err := execHi(c, parent); err != nil {
		t.Fatalf("parent turn: %v", err)
	}
	head, err := store.Session(parent).Head()
	if err != nil {
		t.Fatal(err)
	}
	// The child journal a build without the field wrote: the parent's prefix and a FORK marker
	// that names no instance.
	child := mustCreate(t, c)
	if err := controller.Fork(store.Session(parent), store.Session(child), head); err != nil {
		t.Fatal(err)
	}
	if got := forkMarkerInstance(t, store, child); got != "" {
		t.Fatalf("legacy FORK marker records instance %q, want none", got)
	}
	ref := substrate.ActorRef{Atespace: "space", Name: child}
	if err := ctl.CreateActor(ctx, ref, substrate.ObjectRef{Atespace: "space", Name: "mem"}); err != nil {
		t.Fatal(err)
	}
	ctl.mu.Lock()
	actor := ctl.uids[child]
	ctl.mu.Unlock()

	if err := execHi(c, child); err != nil {
		t.Fatalf("first turn of a child whose FORK marker records no clone: %v", err)
	}
	if got := lastStartInstance(t, store, child); got != actor {
		t.Errorf("child's first turn recorded instance %q, want the actor it attached to, %q", got, actor)
	}
}
