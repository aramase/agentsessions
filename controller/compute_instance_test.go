package controller_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
)

// startOn is a completed turn that recorded harness and compute instance.
func startOn(id, harness, instance string) []api.Event {
	return []api.Event{
		{ExecutionID: id, Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0), Harness: harness, ComputeInstance: instance}},
		{ExecutionID: id, Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	}
}

func joinEvents(parts ...[]api.Event) []api.Event {
	var out []api.Event
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

var forkMarker = []api.Event{{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: "parent@2"}}}

// Exec compares the compute instance it was placed on with the latest one the journal recorded for
// its harness since the last FORK marker. A different instance is refused with
// api.ErrMemoryStateLost before anything is appended or the harness runs; otherwise the turn records
// its instance in EXECUTION_START.
func TestExecComparesTheComputeInstance(t *testing.T) {
	onlyMem := func(recorded string) bool { return recorded == "mem" }
	for _, tc := range []struct {
		name        string
		journal     []api.Event
		instance    string
		sameHarness func(string) bool
		refuse      bool
	}{
		{name: "new session records its instance", instance: "a"},
		{name: "same instance", journal: startOn("t1", "mem", "a"), instance: "a"},
		{name: "replaced instance", journal: startOn("t1", "mem", "b"), instance: "a", refuse: true},
		{name: "the latest instance counts", journal: joinEvents(startOn("t1", "mem", "a"), startOn("t2", "mem", "b")), instance: "a", refuse: true},
		{name: "a turn that recorded none is skipped", journal: joinEvents(startOn("t1", "mem", "b"), startOn("t2", "mem", "")), instance: "a", refuse: true},
		{name: "only turns without an instance", journal: startOn("t1", "mem", ""), instance: "a"},
		{name: "a fork child's own compute is not the parent's", journal: joinEvents(startOn("t1", "mem", "parent"), forkMarker), instance: "child"},
		{name: "a fork child's own turns count", journal: joinEvents(startOn("t1", "mem", "parent"), forkMarker, startOn("t2", "mem", "child")), instance: "other", refuse: true},
		{name: "another harness's instance is not compared", journal: joinEvents(startOn("t1", "mem", "a"), startOn("t2", "other", "z")), instance: "a", sameHarness: onlyMem},
		{name: "no sameHarness compares every harness", journal: joinEvents(startOn("t1", "mem", "a"), startOn("t2", "other", "z")), instance: "a", refuse: true},
		{name: "no instance compares nothing", journal: startOn("t1", "mem", "b"), instance: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := identityLog(t, tc.journal...)
			head, err := log.Head()
			if err != nil {
				t.Fatal(err)
			}
			ran := false
			har := identityHarness{desc: api.Descriptor{ID: "mem"}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				ran = true
				return sink.Output(ctx, "reply")
			}}
			c, err := controller.New(log, nil, controller.WithHarness("mem"), controller.WithSessionUID("sess"),
				controller.WithComputeInstance(tc.instance, tc.sameHarness))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Exec(context.Background(), har, nil, head)
			if tc.refuse {
				if !errors.Is(err, api.ErrMemoryStateLost) || !strings.Contains(err.Error(), `"sess"`) {
					t.Fatalf("Exec = %v, want api.ErrMemoryStateLost naming the session", err)
				}
				if ran {
					t.Error("the harness ran on a replaced instance")
				}
				if got, _ := log.Head(); got != head {
					t.Errorf("head = %d after the refusal, want %d: nothing may be journaled", got, head)
				}
				return
			}
			if err != nil {
				t.Fatalf("Exec = %v", err)
			}
			recs, err := log.Read(head + 1)
			if err != nil || len(recs) == 0 || recs[0].Event.ExecutionStart == nil {
				t.Fatalf("no EXECUTION_START after the turn: %v %v", recs, err)
			}
			if got := recs[0].Event.ExecutionStart.ComputeInstance; got != tc.instance {
				t.Fatalf("recorded ComputeInstance = %q, want %q", got, tc.instance)
			}
		})
	}
}

// The comparison reads the journal the EXECUTION_START append extends. A turn another host journals
// after this controller's caller read the head is either in that history, and compared, or makes the
// append fail; it cannot be skipped.
func TestExecComparesTheJournalItAppendsTo(t *testing.T) {
	log := identityLog(t)
	staleHead, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	// Another host's turn on instance "b" lands after this caller read the head.
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range startOn("t1", "mem", "b") {
		if _, err := log.Append(staleHead+int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
	}
	c, err := controller.New(log, nil, controller.WithHarness("mem"), controller.WithComputeInstance("a", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), identityHarness{desc: api.Descriptor{ID: "mem"}}, nil, staleHead); !errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("Exec on a stale head after another host's turn on another instance = %v, want api.ErrMemoryStateLost", err)
	}
}

// Resume restores the session's compute whether or not a turn is pending, so it refuses a replaced
// instance even with nothing to re-drive, before running the harness or appending anything.
func TestResumeComparesTheComputeInstance(t *testing.T) {
	pending := []api.Event{
		{ExecutionID: "t2", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0), Harness: "mem", ComputeInstance: "b"}},
	}
	for _, tc := range []struct {
		name    string
		journal []api.Event
	}{
		{name: "completed journal", journal: startOn("t1", "mem", "b")},
		{name: "pending turn", journal: joinEvents(startOn("t1", "mem", "b"), pending)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := identityLog(t, tc.journal...)
			head, err := log.Head()
			if err != nil {
				t.Fatal(err)
			}
			ran := false
			har := identityHarness{desc: api.Descriptor{ID: "mem"}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				ran = true
				return nil
			}}
			c, err := controller.New(log, nil, controller.WithHarness("mem"), controller.WithComputeInstance("a", nil))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Resume(context.Background(), har); !errors.Is(err, api.ErrMemoryStateLost) {
				t.Fatalf("Resume on a replaced instance = %v, want api.ErrMemoryStateLost", err)
			}
			if ran {
				t.Error("the harness ran on a replaced instance")
			}
			if got, _ := log.Head(); got != head {
				t.Errorf("head = %d after the refusal, want %d", got, head)
			}

			same, err := controller.New(log, nil, controller.WithHarness("mem"), controller.WithComputeInstance("b", nil))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := same.Resume(context.Background(), har); err != nil {
				t.Fatalf("Resume on the recorded instance = %v", err)
			}
		})
	}
}
