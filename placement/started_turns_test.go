package placement_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/eventlog"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
)

// specRecorder asks every Create for the started-turn count, as a memory-tier backend does, and
// records it. A count that fails fails the Create, which then creates nothing.
type specRecorder struct {
	*local.Backend
	counts []int
}

func (b *specRecorder) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	n, err := s.StartedTurns()
	if err != nil {
		return api.Incarnation{}, err
	}
	b.counts = append(b.counts, n)
	return b.Backend.Create(ctx, s)
}

// failingRead fails every journal read, so the started-turn count cannot be taken.
type failingRead struct{ eventlog.Store }

var errRead = errors.New("read failed")

func (failingRead) Read(int64) ([]eventlog.Record, error) { return nil, errRead }

// The backend tells a new session from one whose compute is gone by the started-turn count the
// Placer offers Create: 0 before the first turn, then one more per turn.
func TestExecOffersStartedTurnsToCreate(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	b := &specRecorder{Backend: inner}
	p := placement.New(b, echoagent.Model)
	log := eventlog.AsStore(eventlog.New())
	for i := 0; i < 3; i++ {
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(context.Background(), log, "sess", []api.Message{*api.TextMessage("user", "hi")}, head); err != nil {
			t.Fatalf("turn %d: %v", i+1, err)
		}
	}
	if got, want := b.counts, []int{0, 1, 2}; !slices.Equal(got, want) {
		t.Fatalf("StartedTurns per Create = %v, want %v", got, want)
	}
}

// A journal the Placer cannot read gives no turn count, so it must not guess zero (which would let a
// memory-tier session be cold-created): the count fails, and so does Exec, with nothing created.
func TestExecRefusesWhenTheJournalCannotBeRead(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	b := &specRecorder{Backend: inner}
	p := placement.New(b, echoagent.Model)
	log := failingRead{eventlog.AsStore(eventlog.New())}
	_, err := p.Exec(context.Background(), log, "sess", []api.Message{*api.TextMessage("user", "hi")}, 0)
	if !errors.Is(err, errRead) {
		t.Fatalf("Exec error = %v, want the read error", err)
	}
	if len(b.counts) != 0 {
		t.Fatalf("Create went ahead %d times after a failed journal read", len(b.counts))
	}
}

// createCounts returns the StartedTurns of every Create b has seen, in order.
func createCounts(b *specRecorder) []int { return b.counts }

// execAs runs one turn on p, recorded under the harness name, the way session.Service routes it.
func execAs(t *testing.T, p *placement.Placer, log eventlog.Store, name string) {
	t.Helper()
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), log, "sess", []api.Message{*api.TextMessage("user", "hi")}, head, placement.WithHarness(name)); err != nil {
		t.Fatalf("turn on %q: %v", name, err)
	}
}

// A turn an ExecRequest.harness override sent to another harness left no state in this harness's
// compute, so it must not count toward this harness's turns: otherwise a memory-tier default that
// never ran for the session would be refused as if it had lost state. A name that aliases this
// Placer in the registry ran on the same compute, so its turns do count, and so does a turn recorded
// under a name this host does not serve, since nothing proves it ran elsewhere.
func TestStartedTurnsCountOnlyTurnsThisPlacerRan(t *testing.T) {
	newBackend := func() *specRecorder {
		inner := local.New(echoagent.Harness{})
		t.Cleanup(func() { _ = inner.Close() })
		return &specRecorder{Backend: inner}
	}
	memBackend, otherBackend := newBackend(), newBackend()
	mem := placement.New(memBackend, echoagent.Model)
	other := placement.New(otherBackend, echoagent.Model)
	if _, err := placement.NewRegistry("mem", map[string]*placement.Placer{"mem": mem, "alias": mem, "other": other}); err != nil {
		t.Fatal(err)
	}
	log := eventlog.AsStore(eventlog.New())

	execAs(t, other, log, "other")   // mem: 0
	execAs(t, mem, log, "mem")       // mem: 0 before, 1 after
	execAs(t, mem, log, "alias")     // mem: 1 before, 2 after
	execAs(t, other, log, "other")   // mem: 2
	execAs(t, mem, log, "elsewhere") // mem: 2 before, 3 after
	execAs(t, mem, log, "mem")       // mem: 3 before
	if got, want := createCounts(memBackend), []int{0, 1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("StartedTurns per Create on mem = %v, want %v", got, want)
	}
	// other's own two turns, not mem's or the alias's: the unknown name counts for every Placer.
	if got, want := createCounts(otherBackend), []int{0, 1}; !slices.Equal(got, want) {
		t.Errorf("StartedTurns per Create on other = %v, want %v", got, want)
	}
}

// A Placer outside a registry has one backend and cannot map a recorded name to another one, so a
// turn recorded under any name may have run on it: every turn counts, whatever name the current one
// runs under. Counting only the current name would let a memory-tier Create under a new name see 0
// turns and cold-create over the state the session's earlier turns left.
func TestStartedTurnsOnAStandalonePlacer(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	b := &specRecorder{Backend: inner}
	p := placement.New(b, echoagent.Model)
	log := eventlog.AsStore(eventlog.New())
	execAs(t, p, log, "a")
	execAs(t, p, log, "b")
	execAs(t, p, log, "a")
	if got, want := createCounts(b), []int{0, 1, 2}; !slices.Equal(got, want) {
		t.Errorf("StartedTurns per Create = %v, want %v", got, want)
	}
	if n, err := p.StartedTurns(log); err != nil || n != 3 {
		t.Errorf("StartedTurns = %d, %v; want 3", n, err)
	}
}

// instanceBackend places every session on the compute instance named by instance, as a backend
// that reports actor UIDs does; changing it stands in for the actor being replaced.
type instanceBackend struct {
	*local.Backend
	instance string
}

func (b *instanceBackend) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	inc, err := b.Backend.Create(ctx, s)
	inc.ComputeInstance = b.instance
	return inc, err
}

// A standalone Placer attributes the instance a turn recorded under another name to itself too, so
// a replaced compute instance is refused under a renamed harness, not adopted as a fresh start.
func TestStandalonePlacerRefusesALostInstanceUnderARenamedHarness(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	b := &instanceBackend{Backend: inner, instance: "uid-1"}
	p := placement.New(b, echoagent.Model)
	log := eventlog.AsStore(eventlog.New())
	execAs(t, p, log, "a")
	execAs(t, p, log, "b") // the same instance under another name is accepted

	b.instance = "uid-2"
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Exec(context.Background(), log, "sess", []api.Message{*api.TextMessage("user", "hi")}, head, placement.WithHarness("c"))
	if !errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("Exec under a renamed harness on a replaced instance = %v, want ErrMemoryStateLost", err)
	}
	if after, err := log.Head(); err != nil || after != head {
		t.Fatalf("head moved from %d to %d (%v): the refused turn journaled something", head, after, err)
	}
}

// Every started turn counts, not only completed ones: a turn that ended in ERROR or was interrupted
// (no END) may have changed the harness's RAM state. A turn whose EXECUTION_START recorded no harness
// name (written before names were recorded), and a legacy turn with no execution ID at all, cannot be
// attributed, so they count for every Placer: undercounting would let a memory-tier harness be
// cold-created over the state it held. A turn recorded under another harness the registry serves
// does not count.
func TestStartedTurnsCountsTurnsWithNoRecordedHarness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
		want   int
	}{
		{name: "legacy END with no execution ID", events: []api.Event{
			{Kind: api.EventEnd},
		}, want: 1},
		{name: "legacy turns: completed, failed, then an open tail", events: []api.Event{
			{Kind: api.EventInput}, {Kind: api.EventOutput}, {Kind: api.EventEnd},
			{Kind: api.EventLifecycle},
			{Kind: api.EventInput}, {Kind: api.EventError},
			{Kind: api.EventInput},
		}, want: 3},
		{name: "a turn that ended in ERROR", events: []api.Event{
			{Kind: api.EventExecutionStart, ExecutionID: "x1", ExecutionStart: &api.ExecutionStart{Harness: "mem"}},
			{Kind: api.EventInput, ExecutionID: "x1"},
			{Kind: api.EventError, ExecutionID: "x1"},
		}, want: 1},
		{name: "an interrupted turn with no END or ERROR", events: []api.Event{
			{Kind: api.EventExecutionStart, ExecutionID: "x1", ExecutionStart: &api.ExecutionStart{Harness: "mem"}},
			{Kind: api.EventInput, ExecutionID: "x1"},
		}, want: 1},
		{name: "a legacy tail superseded by an EXECUTION_START", events: []api.Event{
			{Kind: api.EventInput},
			{Kind: api.EventExecutionStart, ExecutionID: "x1", ExecutionStart: &api.ExecutionStart{Harness: "mem"}},
			{Kind: api.EventEnd, ExecutionID: "x1"},
		}, want: 2},
		{name: "EXECUTION_START with no harness name", events: []api.Event{
			{Kind: api.EventExecutionStart, ExecutionID: "x1", ExecutionStart: &api.ExecutionStart{}},
			{Kind: api.EventEnd, ExecutionID: "x1"},
		}, want: 1},
		{name: "another harness's turn is not counted", events: []api.Event{
			{Kind: api.EventExecutionStart, ExecutionID: "x1", ExecutionStart: &api.ExecutionStart{Harness: "other"}},
			{Kind: api.EventEnd, ExecutionID: "x1"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = inner.Close() })
			mem := placement.New(inner, echoagent.Model)
			other := placement.New(inner, echoagent.Model)
			if _, err := placement.NewRegistry("mem", map[string]*placement.Placer{"mem": mem, "other": other}); err != nil {
				t.Fatal(err)
			}
			log := eventlog.AsStore(eventlog.New())
			for i, ev := range tc.events {
				if _, err := log.Append(int64(i), 0, ev); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := mem.StartedTurns(log); err != nil || n != tc.want {
				t.Errorf("StartedTurns = %d, %v; want %d", n, err, tc.want)
			}
		})
	}
}
