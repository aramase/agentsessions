package substrate_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/runtime/substrate"
)

// mockControl records the ordered control-plane calls and returns canned actor state, so tests can
// assert the Runtime→substrate mapping without a real ate-api-server.
//
// It models substrate's actor REGISTRY, not just its replies: an actor that was never created is
// reported as ErrActorNotFound. That fidelity matters — a mock which accepted every CreateActor is
// what let an unconditional create-then-cold-boot look correct here while failing on a real cluster
// the moment a session took a second turn.
type mockControl struct {
	calls  []string
	status substrate.ActorStatus
	info   substrate.ActorInfo
	actors map[string]substrate.ActorStatus
	// snapshots is each actor's current external snapshot handle, as GetActor reports it.
	snapshots map[string]string
	// afterGet, when set, runs after GetActor answers, so a test can move an actor's state in the
	// window between a check and the call it guards.
	afterGet func(name string)
}

// exists records an actor at a status, so GetActor reports it as already placed.
func (m *mockControl) exists(name string, st substrate.ActorStatus) {
	if m.actors == nil {
		m.actors = map[string]substrate.ActorStatus{}
	}
	m.actors[name] = st
}

func (m *mockControl) CreateActor(ctx context.Context, a substrate.ActorRef, _ substrate.ObjectRef) error {
	m.calls = append(m.calls, "create:"+a.Name)
	if _, ok := m.actors[a.Name]; ok {
		return fmt.Errorf("actor %q already exists", a.Name)
	}
	m.exists(a.Name, substrate.StatusRunning)
	return nil
}
func (m *mockControl) ResumeActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, "resume:"+a.Name)
	m.exists(a.Name, substrate.StatusRunning)
	return m.info, nil
}
func (m *mockControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.calls = append(m.calls, "suspend:"+a.Name)
	m.exists(a.Name, substrate.StatusSuspended)
	snap := "gs://snap/" + a.Name
	m.holds(a.Name, snap)
	return snap, nil
}

// holds records the external snapshot an actor currently holds.
func (m *mockControl) holds(name, snapshot string) {
	if m.snapshots == nil {
		m.snapshots = map[string]string{}
	}
	m.snapshots[name] = snapshot
}
func (m *mockControl) DeleteActor(ctx context.Context, a substrate.ActorRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.calls = append(m.calls, "delete:"+a.Name)
	delete(m.actors, a.Name)
	return nil
}
func (m *mockControl) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, "get:"+a.Name)
	if m.afterGet != nil {
		defer m.afterGet(a.Name)
	}
	if st, ok := m.actors[a.Name]; ok {
		return substrate.ActorInfo{Status: st, Worker: m.info.Worker, Snapshot: m.snapshots[a.Name]}, nil
	}
	if m.status != substrate.StatusUnknown { // canned state for the suspend/restore cases
		return substrate.ActorInfo{Status: m.status, Worker: m.info.Worker, Snapshot: m.snapshots[a.Name]}, nil
	}
	return substrate.ActorInfo{}, substrate.ErrActorNotFound
}

// running is what a successful ResumeActor reports: the actor on a worker.
func running(worker string) substrate.ActorInfo {
	return substrate.ActorInfo{Status: substrate.StatusRunning, Worker: worker}
}

func newBackend(m *mockControl, opts ...substrate.Option) *substrate.Backend {
	return substrate.New(m, "space", substrate.ObjectRef{Atespace: "tmpl", Name: "echo"},
		api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, opts...)
}

// mockCloner is a control client that ALSO supports substrate's Tag clone APIs. It is a separate
// type from mockControl so tests can cover a control client without them.
type mockCloner struct {
	*mockControl
	cloneErr error
}

func (m *mockCloner) TagActor(ctx context.Context, source substrate.ActorRef, tag substrate.SnapshotID) error {
	m.calls = append(m.calls, "tag:"+source.Name+"->"+tag.Name)
	return nil
}

func (m *mockCloner) CreateActorFromTag(ctx context.Context, a substrate.ActorRef, _ substrate.ObjectRef, tag substrate.SnapshotID) error {
	if m.cloneErr != nil {
		return m.cloneErr
	}
	m.calls = append(m.calls, "clone:"+a.Name+":from="+tag.Name)
	return nil
}

func (m *mockCloner) DeleteTag(ctx context.Context, tag substrate.SnapshotID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.calls = append(m.calls, "untag:"+tag.Name)
	return nil
}

// newMemoryBackend builds a backend for a REQUIRES_MEMORY_SNAPSHOT harness (the counter tier), whose
// live state exists only in RAM and therefore cannot be replay-forked.
func newMemoryBackend(ctl substrate.ControlClient) *substrate.Backend {
	return substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "tmpl", Name: "counter"},
		api.Descriptor{ID: "counter", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}})
}

func TestCreateBootsActor(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	in, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:sess-x", "create:sess-x", "resume:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("create calls=%v want %v", m.calls, want)
	}
	// Substrate's only ingress to an actor is the atenet-router: the incarnation addresses the router
	// and names the actor in the metadata the router routes on, never a worker address.
	want := api.Incarnation{
		ID:           "sess-x",
		Worker:       "worker-a",
		Address:      substrate.DefaultRouterAddress,
		CallMetadata: map[string]string{"ate-target-actor": "space/sess-x"},
		Runtime:      "substrate",
	}
	if !reflect.DeepEqual(in, want) {
		t.Fatalf("incarnation=%+v want %+v", in, want)
	}
}

func TestWithRouterOverridesTheIngressAddress(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	in, err := newBackend(m, substrate.WithRouter("router.example:8080")).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if in.Address != "router.example:8080" || in.CallMetadata[substrate.TargetActorHeader] != "space/sess-x" {
		t.Fatalf("incarnation=%+v want the configured router and the actor named in metadata", in)
	}
}

// A session is multi-turn by definition, and the Placer calls Create on every Exec. Substrate
// rejects a repeat CreateActor with AlreadyExists, so the second turn must ATTACH to the running
// actor rather than try to create it again.
func TestCreateAttachesToARunningActor(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	b := newBackend(m)
	if _, err := b.Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err != nil {
		t.Fatal(err)
	}
	m.calls = nil
	in, err := b.Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatalf("second turn on a live session: %v", err)
	}
	if want := []string{"get:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("second Create calls=%v want a bare attach %v (a re-create is AlreadyExists; a re-boot destroys live state)", m.calls, want)
	}
	if in.CallMetadata[substrate.TargetActorHeader] != "space/sess-x" {
		t.Fatalf("attach returned %+v, want the running actor's incarnation", in)
	}
}

// A forked child's actor is created from the parent's snapshot and resumed before its UID is ever
// handed out, so it is already RUNNING when the first Exec lands. Cold-booting it there would throw
// away the cloned RAM the fork exists to carry.
func TestCreateOnAForkedChildDoesNotBootOverTheClone(t *testing.T) {
	m := &mockControl{info: running("worker-b")}
	m.exists("child", substrate.StatusRunning)
	in, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:child"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want a bare attach %v (a re-create would discard the restored RAM)", m.calls, want)
	}
	if in.CallMetadata[substrate.TargetActorHeader] != "space/child" {
		t.Fatalf("attach returned %+v, want the clone's incarnation", in)
	}
}

// A session whose worker was freed by Suspend must come back through its snapshot, not a cold boot.
func TestCreateRestoresASuspendedActor(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	m.exists("sess-x", substrate.StatusSuspended)
	if _, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:sess-x", "resume:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want a restore %v (a re-create would discard the memory snapshot)", m.calls, want)
	}
}

func TestCreateRejectsAnActorNotRunningAfterResume(t *testing.T) {
	// A resume that does not leave the actor RUNNING must fail loudly. Handing back an incarnation
	// anyway would let the first harness call resume it through the router instead, hiding the
	// failure from the backend that owns placement.
	m := &mockControl{info: substrate.ActorInfo{Status: substrate.StatusSuspended}}
	if _, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err == nil {
		t.Fatal("expected an error when the actor is not running after resume, got nil")
	}
}

func TestCreateValidatesSessionSpec(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *api.SessionSpec
		want string
	}{
		{name: "nil spec", want: "session spec"},
		{name: "empty session uid", spec: &api.SessionSpec{}, want: "session uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockControl{}
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
			_, err := newBackend(m, substrate.WithLogger(logger)).Create(context.Background(), tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Create() error = %v, want it to contain %q", err, tc.want)
			}
			if len(m.calls) != 0 {
				t.Fatalf("Create() made control-plane calls for invalid input: %v", m.calls)
			}
			if !strings.Contains(output.String(), `"error_kind":"invalid_spec"`) ||
				!strings.Contains(output.String(), `"level":"ERROR"`) {
				t.Fatalf("Create() did not log invalid spec at ERROR: %s", output.String())
			}
		})
	}
}

func TestSuspendRestoreRoundtrip(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := newBackend(m, substrate.WithLogger(logger))
	ref, err := b.Snapshot(context.Background(), api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Memory || ref.Local != "sess-x" || ref.ExternalURI != "gs://snap/sess-x" {
		t.Fatalf("unexpected snapshot ref %+v", ref)
	}
	if !strings.Contains(output.String(), `"operation":"suspend_actor"`) ||
		!strings.Contains(output.String(), `"reason":"snapshot"`) {
		t.Fatalf("snapshot did not log its SuspendActor call: %s", output.String())
	}
	if _, err := b.Restore(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if want := []string{"suspend:sess-x", "resume:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("suspend/restore calls=%v want %v", m.calls, want)
	}
}

func TestSnapshotLocalUnsupported(t *testing.T) {
	if _, err := newBackend(&mockControl{}).Snapshot(context.Background(), api.Incarnation{ID: "s"}, api.SnapshotLocal); err == nil {
		t.Fatal("expected LOCAL (warm) snapshot to be unsupported on substrate")
	}
}

func TestStopSuspendsThenDeletes(t *testing.T) {
	m := &mockControl{}
	if err := newBackend(m).Stop(context.Background(), api.Incarnation{ID: "sess-x"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"suspend:sess-x", "delete:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("stop calls=%v want %v (delete requires SUSPENDED first)", m.calls, want)
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		status substrate.ActorStatus
		want   api.ComputeState
	}{
		{substrate.StatusRunning, api.ComputeLive},
		{substrate.StatusSuspended, api.ComputeCold},
		{substrate.StatusTerminated, api.ComputeTerminated},
	} {
		got, err := newBackend(&mockControl{status: tc.status}).Status(context.Background(), api.Incarnation{ID: "s"})
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("status %v -> %v, want %v", tc.status, got, tc.want)
		}
	}
}

// A STATELESS_REPLAY harness holds nothing the journal lacks, so its fork stays a replay-fork: a
// fresh cold child, no snapshot clone, and the parent is left running.
func TestForkIsReplayForkForStatelessHarness(t *testing.T) {
	m := &mockControl{info: running("worker-b")}
	if _, err := newBackend(m).Fork(context.Background(), api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:child", "create:child", "resume:child"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("fork calls=%v want a fresh cold child actor %v", m.calls, want)
	}
}

// newClonerWithSuspendedParent is a cloning control client whose "parent" actor is SUSPENDED holding
// snap-parent-1, the state a stateful fork finds right after it checkpoints the parent.
func newClonerWithSuspendedParent() *mockCloner {
	m := &mockCloner{mockControl: &mockControl{info: running("worker-c")}}
	m.exists("parent", substrate.StatusSuspended)
	m.holds("parent", "snap-parent-1")
	return m
}

// A REQUIRES_MEMORY_SNAPSHOT harness's state lives only in RAM, so its fork must CLONE the parent's
// snapshot: tag the suspended parent, create the child from that tag, and resume it, which restores
// the cloned RAM. The parent is checked on both sides of the tag, because a tag captures whatever
// snapshot the parent holds when it runs.
func TestForkClonesSnapshotForMemoryHarness(t *testing.T) {
	m := newClonerWithSuspendedParent()
	in, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"get:parent", "tag:parent->fork-child", "get:parent", "clone:child:from=fork-child", "resume:child"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("fork calls=%v want a snapshot clone %v", m.calls, want)
	}
	if in.CallMetadata[substrate.TargetActorHeader] != "space/child" {
		t.Fatalf("cloned child incarnation=%+v want it to target the child actor", in)
	}
}

// The router resumes an actor on any request addressed to it, so the parent can be woken and
// suspended again between the fork's checkpoint and its tag. The tag would then copy a newer snapshot
// than the journal prefix the children inherit. Such a fork must be refused before it tags anything.
func TestForkRefusesAParentThatMovedPastItsCheckpoint(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.holds("parent", "snap-parent-2") // suspended again after the fork's checkpoint
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrSnapshotSuperseded) {
		t.Fatalf("err=%v want ErrSnapshotSuperseded", err)
	}
	if want := []string{"get:parent"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the fork refused before tagging %v", m.calls, want)
	}
}

// The same race inside the window between the first check and the tag: the tag may have copied the
// newer snapshot, so it must be deleted and the fork refused.
func TestForkDeletesATagTakenAfterTheParentMoved(t *testing.T) {
	m := newClonerWithSuspendedParent()
	gets := 0
	m.afterGet = func(name string) {
		if name != "parent" {
			return
		}
		if gets++; gets == 1 {
			m.holds("parent", "snap-parent-2") // woken and suspended again right after the check
		}
	}
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrSnapshotSuperseded) {
		t.Fatalf("err=%v want ErrSnapshotSuperseded", err)
	}
	want := []string{"get:parent", "tag:parent->fork-child", "get:parent", "untag:fork-child"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the stale tag deleted and no child created %v", m.calls, want)
	}
}

// Forking a memory harness with no parent snapshot must fail loudly. Cold-booting the child instead
// would hand back a child whose RAM is empty while its copied journal prefix says otherwise — a
// silently divergent branch, since such a harness never rebuilds state from History (I4).
func TestForkMemoryHarnessWithoutSnapshotIsRefused(t *testing.T) {
	m := newClonerWithSuspendedParent()
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrNoSnapshotToClone) {
		t.Fatalf("err=%v want ErrNoSnapshotToClone", err)
	}
	if len(m.calls) != 0 {
		t.Fatalf("a refused fork must not touch the control plane, got %v", m.calls)
	}
}

// A control client without the Tag APIs cannot clone. It must refuse a stateful fork rather than
// silently degrading to a replay-fork that loses the in-RAM state.
func TestForkMemoryHarnessWithoutClonerIsRefused(t *testing.T) {
	m := &mockControl{info: running("worker-c")}
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrNoSnapshotToClone) {
		t.Fatalf("err=%v want ErrNoSnapshotToClone", err)
	}
}

// Cloning is a full per-actor restore, not a shared copy-on-write image, so the backend must not
// advertise CoWFork. Capability claims gate placement decisions; an inflated one degrades dishonestly.
func TestCapabilitiesDoNotClaimCoWFork(t *testing.T) {
	if newBackend(&mockControl{}).Capabilities().CoWFork {
		t.Fatal("substrate clones via a full snapshot restore; CoWFork must stay false")
	}
}

// TestTwoBackendNeutrality is the spike §8 success criterion: substrate reports MemorySnapshot=true
// and can host a REQUIRES_MEMORY_SNAPSHOT harness, while a plain pod (MemorySnapshot=false) must
// refuse it — honest degradation across two Runtime backends behind one SPI. A STATELESS_REPLAY
// harness runs on both.
func TestTwoBackendNeutrality(t *testing.T) {
	sub := newBackend(&mockControl{}).Capabilities()
	pod := api.RuntimeCapabilities{MemorySnapshot: false}

	if !sub.MemorySnapshot {
		t.Fatal("substrate must report MemorySnapshot=true")
	}
	memHarness := api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}
	if !controller.CanPlace(memHarness, sub) {
		t.Fatal("substrate must host a REQUIRES_MEMORY_SNAPSHOT harness")
	}
	if controller.CanPlace(memHarness, pod) {
		t.Fatal("a plain pod must REFUSE a REQUIRES_MEMORY_SNAPSHOT harness (honest degradation)")
	}

	stateless := api.Capabilities{Resumability: api.ResumabilityStatelessReplay}
	if !controller.CanPlace(stateless, sub) || !controller.CanPlace(stateless, pod) {
		t.Fatal("a STATELESS_REPLAY harness must run on both backends")
	}
}

// A tag owns a full copy of the parent's snapshot, so a fork that fails after tagging must delete
// it. Otherwise every failed fork attempt strands one more snapshot copy in object storage.
func TestFailedCloneReleasesItsTag(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.cloneErr = errors.New("template mismatch")
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err == nil {
		t.Fatal("expected the clone to fail")
	}
	want := []string{"get:parent", "tag:parent->fork-child", "get:parent", "untag:fork-child"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the tag to be released %v", m.calls, want)
	}
}

// A clone that was created and resumed but is not reported RUNNING may still hold a worker. Fork
// returns no handle in that case, so the child UID is discarded upstream and this is the last moment
// the actor can be named: it must be torn down here, not left behind forever.
func TestForkTearsDownACloneThatIsNotRunning(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.info = substrate.ActorInfo{Status: substrate.StatusUnknown} // resumed, but not reported running
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err == nil {
		t.Fatal("a clone that is not running must not be returned as a usable incarnation")
	}
	want := []string{
		"get:parent", "tag:parent->fork-child", "get:parent", "clone:child:from=fork-child", "resume:child",
		"suspend:child", "delete:child", "untag:fork-child",
	}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the stranded clone torn down %v", m.calls, want)
	}
}

// The likeliest reason a fork fails partway is that the caller's deadline expired or it went away.
// Cleanup bound to that same context would do nothing in exactly that case, so it must be detached.
func TestForkCleanupSurvivesACancelledCaller(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.cloneErr = errors.New("deadline exceeded")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newMemoryBackend(m).Fork(ctx,
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"}); err == nil {
		t.Fatal("expected the fork to fail")
	}
	// The mock refuses any call carrying a cancelled context, so recording this proves the release
	// did NOT inherit the caller's context.
	if !slices.Contains(m.calls, "untag:fork-child") {
		t.Fatalf("cleanup must run on a detached context, calls=%v", m.calls)
	}
}
