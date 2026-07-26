package placement_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

// TestPlacerExecRoutesThroughRuntime proves a turn placed via the Placer runs through Runtime.Create
// and the backend-provided harness, binds the controller to a log-minted fence stamped on the
// incarnation, and produces a verifiable journal — the in-process realization of "the controller
// drives the Runtime SPI" instead of a co-located controller.
func TestPlacerExecRoutesThroughRuntime(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	p := placement.New(local.New(echoagent.Harness{}), echoagent.Model)
	inc, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0)
	if err != nil {
		t.Fatalf("placed exec: %v", err)
	}
	if inc.Runtime != "local" {
		t.Fatalf("turn must be placed on a runtime, got %q", inc.Runtime)
	}
	if inc.FenceToken == 0 {
		t.Fatal("the Placer must stamp the minted fence on the incarnation")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head == 0 {
		t.Fatal("the placed turn produced no records")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after placed exec: %v", err)
	}
}

// TestPlacerFenceBinding proves the fence the Placer stamps on the incarnation is the SAME token the
// controller appended under: a second placement supersedes the first, so a stale controller bound to
// the first incarnation's fence would be fenced out. Here we assert the returned fence is monotonic
// and matches what the log advanced to.
func TestPlacerFenceBinding(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")
	p := placement.New(local.New(echoagent.Harness{}), echoagent.Model)

	inc1, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "one")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := log.Head()
	inc2, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "two")}, head)
	if err != nil {
		t.Fatal(err)
	}
	if inc2.FenceToken <= inc1.FenceToken {
		t.Fatalf("each placement must mint a strictly newer fence: %d then %d", inc1.FenceToken, inc2.FenceToken)
	}
	// The stamped fence is REAL: a controller bound to the first (now superseded) incarnation's fence
	// is fenced out on append. expected_last_seq is current, so the failure isolates the fence.
	head2, _ := log.Head()
	stale, err := controller.New(log, echoagent.Model, controller.WithFence(inc1.FenceToken))
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "stale")}, head2); !errors.Is(err, eventlog.ErrFenced) {
		t.Fatalf("a controller bound to the superseded fence must be fenced out, got %v", err)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// memSnapshotHarness declares REQUIRES_MEMORY_SNAPSHOT — placeable on substrate, refused on a
// filesystem-only backend. Its Run is a no-op (the gate decision, not execution, is under test).
type memSnapshotHarness struct{}

func (memSnapshotHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}, nil
}
func (memSnapshotHarness) Run(context.Context, *api.Start, api.EventSink) error { return nil }

// stubControl is a no-op substrate.ControlClient so a substrate backend can be driven in tests
// without a real ate-api-server.
type stubControl struct{}

func (stubControl) CreateActor(context.Context, substrate.ActorRef, substrate.ObjectRef) error {
	return nil
}
func (stubControl) ResumeActor(context.Context, substrate.ActorRef, bool) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, nil
}
func (stubControl) SuspendActor(context.Context, substrate.ActorRef) (string, error) { return "", nil }
func (stubControl) DeleteActor(context.Context, substrate.ActorRef) error            { return nil }
func (stubControl) GetActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, nil
}

// placeableSubstrate wraps a substrate backend with an in-process harness so it satisfies
// placement.Backend for the gate test (real substrate dials the actor; here the harness is a stub).
type placeableSubstrate struct {
	*substrate.Backend
	harness api.Harness
}

func (p placeableSubstrate) Harness() api.Harness { return p.harness }

// TestNeutralityThroughPlacer is the spike §8 criterion driven end-to-end through the Placer's gate:
// a REQUIRES_MEMORY_SNAPSHOT harness is REFUSED on runtime/local (MemorySnapshot=false) with
// ErrUnplaceable — before any compute is provisioned or any record is written — and ACCEPTED on
// substrate (MemorySnapshot=true). Honest degradation is now an end-to-end property of the Placer,
// not a bare CanPlace unit assertion.
func TestNeutralityThroughPlacer(t *testing.T) {
	// local (filesystem-only) must refuse.
	store1, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()
	log1 := store1.Session("s")
	localPlacer := placement.New(local.New(memSnapshotHarness{}), echoagent.Model)
	if _, err := localPlacer.Exec(context.Background(), log1, "s", nil, 0); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("local (MemorySnapshot=false) must refuse a REQUIRES_MEMORY_SNAPSHOT harness, got %v", err)
	}
	if h, _ := log1.Head(); h != 0 {
		t.Fatalf("a refused placement must not provision or write to the log, head=%d", h)
	}

	// substrate (MemorySnapshot=true) must accept: the gate passes and the turn proceeds.
	store2, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	sub := placeableSubstrate{
		Backend: substrate.New(stubControl{}, "space", substrate.ObjectRef{Name: "echo"}),
		harness: memSnapshotHarness{},
	}
	subPlacer := placement.New(sub, echoagent.Model)
	if _, err := subPlacer.Exec(context.Background(), store2.Session("s"), "s", nil, 0); err != nil {
		t.Fatalf("substrate (MemorySnapshot=true) must accept a REQUIRES_MEMORY_SNAPSHOT harness, got %v", err)
	}
}

// TestSuspendResumeRoundtripThroughSPI proves the §5.1 round-trip: Suspend snapshots via the Runtime
// SPI and records the SnapshotRef in a SUSPEND event on the tamper-evident chain; Resume recovers
// that ref, Restores a fresh incarnation, re-drives with a new fence, and records a RESUME marker —
// no side table. For runtime/local the ref is the session handle (Memory:false).
func TestSuspendResumeRoundtripThroughSPI(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")
	p := placement.New(local.New(echoagent.Harness{}), echoagent.Model)

	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}

	ref, err := p.Suspend(context.Background(), log, "s")
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if ref.Local != "s" || ref.Memory {
		t.Fatalf("local suspend ref must be the session handle, filesystem-only: %+v", ref)
	}

	// The ref rides the chain: the SUSPEND event carries it (§5.1), no side table.
	recs, _ := log.Read(1)
	var carried *api.SnapshotRef
	for _, r := range recs {
		if r.Event.Kind == api.EventLifecycle && r.Event.Lifecycle != nil && r.Event.Lifecycle.Kind == api.LifecycleSuspend {
			carried = r.Event.Lifecycle.Snapshot
		}
	}
	if carried == nil || carried.Local != "s" {
		t.Fatal("the SUSPEND event must carry the SnapshotRef on the chain (§5.1)")
	}

	// Resume recovers the ref, restores, and records a RESUME marker; the chain stays verifiable.
	if err := p.Resume(context.Background(), log, "s"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	recs, _ = log.Read(1)
	last := recs[len(recs)-1].Event
	if last.Kind != api.EventLifecycle || last.Lifecycle == nil || last.Lifecycle.Kind != api.LifecycleResume {
		t.Fatalf("resume must record a RESUME marker, last=%v", last.Kind)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after suspend/resume: %v", err)
	}
}
