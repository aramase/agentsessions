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
func (m *mockControl) ResumeActor(ctx context.Context, a substrate.ActorRef, boot bool) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, fmt.Sprintf("resume:%s:boot=%v", a.Name, boot))
	m.exists(a.Name, substrate.StatusRunning)
	return m.info, nil
}
func (m *mockControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.calls = append(m.calls, "suspend:"+a.Name)
	m.exists(a.Name, substrate.StatusSuspended)
	return "gcs://snap/" + a.Name, nil
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
	if st, ok := m.actors[a.Name]; ok {
		return substrate.ActorInfo{Status: st, PodIP: m.info.PodIP, MeshDNS: m.info.MeshDNS}, nil
	}
	if m.status != substrate.StatusUnknown { // canned state for the suspend/restore cases
		return substrate.ActorInfo{Status: m.status, PodIP: m.info.PodIP, MeshDNS: m.info.MeshDNS}, nil
	}
	return substrate.ActorInfo{}, substrate.ErrActorNotFound
}

func newBackend(m *mockControl, opts ...substrate.Option) *substrate.Backend {
	return substrate.New(m, "space", substrate.ObjectRef{Namespace: "tmpl", Name: "echo"},
		api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, opts...)
}

// mockCloner is a control client that ALSO supports substrate's ActorSnapshot clone APIs. It is a
// separate type from mockControl so tests can cover a deployment that predates those APIs.
type mockCloner struct {
	*mockControl
	cloneErr error
}

func (m *mockCloner) TagSnapshot(ctx context.Context, snapshot, tag substrate.SnapshotID) error {
	m.calls = append(m.calls, "tag:"+snapshot.Name+"->"+tag.Name)
	return nil
}

func (m *mockCloner) CreateActorFromSnapshot(ctx context.Context, a substrate.ActorRef, _ substrate.ObjectRef, tag substrate.SnapshotID) error {
	if m.cloneErr != nil {
		return m.cloneErr
	}
	m.calls = append(m.calls, "clone:"+a.Name+":from="+tag.Name)
	return nil
}

func (m *mockCloner) DeleteSnapshotTag(ctx context.Context, tag substrate.SnapshotID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.calls = append(m.calls, "untag:"+tag.Name)
	return nil
}

// newMemoryBackend builds a backend for a REQUIRES_MEMORY_SNAPSHOT harness (the counter tier), whose
// live state exists only in RAM and therefore cannot be replay-forked.
func newMemoryBackend(ctl substrate.ControlClient) *substrate.Backend {
	return substrate.New(ctl, "space", substrate.ObjectRef{Namespace: "tmpl", Name: "counter"},
		api.Descriptor{ID: "counter", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}})
}

func TestCreateBootsActor(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.5", MeshDNS: "sess-x.space.actors"}}
	in, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:sess-x", "create:sess-x", "resume:sess-x:boot=true"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("create calls=%v want %v", m.calls, want)
	}
	// Address is the direct h2c dial target (PodIP:HarnessPort), not the mesh DNS — the router cannot
	// proxy gRPC, so an in-cluster driver dials the actor's pod IP directly.
	if in.Worker != "10.0.0.5" || in.Address != "10.0.0.5:80" || in.Runtime != "substrate" {
		t.Fatalf("unexpected incarnation %+v", in)
	}
}

// A session is multi-turn by definition, and the Placer calls Create on every Exec. Substrate
// rejects a repeat CreateActor with AlreadyExists, so the second turn must ATTACH to the running
// actor rather than try to create it again.
func TestCreateAttachesToARunningActor(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.5", MeshDNS: "sess-x.space.actors"}}
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
	if in.Address != "10.0.0.5:80" {
		t.Fatalf("attach returned %+v, want the running actor's address", in)
	}
}

// A forked child's actor is created from the parent's snapshot and resumed before its UID is ever
// handed out, so it is already RUNNING when the first Exec lands. Cold-booting it there would throw
// away the cloned RAM the fork exists to carry.
func TestCreateOnAForkedChildDoesNotBootOverTheClone(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.9", MeshDNS: "child.space.actors"}}
	m.exists("child", substrate.StatusRunning)
	in, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(m.calls, "resume:child:boot=true") {
		t.Fatalf("calls=%v cold-booted a cloned child, discarding its restored RAM", m.calls)
	}
	if want := []string{"get:child"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want a bare attach %v", m.calls, want)
	}
	if in.Address != "10.0.0.9:80" {
		t.Fatalf("attach returned %+v, want the clone's address", in)
	}
}

// A session whose worker was freed by Suspend must come back through its snapshot, not a cold boot.
func TestCreateRestoresASuspendedActor(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.7", MeshDNS: "sess-x.space.actors"}}
	m.exists("sess-x", substrate.StatusSuspended)
	if _, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:sess-x", "resume:sess-x:boot=false"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want a restore %v (boot=true would discard the memory snapshot)", m.calls, want)
	}
}

func TestCreateRejectsMissingPodIP(t *testing.T) {
	// A resumed actor with no pod IP means it was not scheduled onto a worker; the backend must fail
	// loudly rather than hand back a bogus ":80" dial target.
	m := &mockControl{info: substrate.ActorInfo{MeshDNS: "sess-x.space.actors"}}
	if _, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err == nil {
		t.Fatal("expected an error when the actor has no pod IP, got nil")
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
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.7"}}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := newBackend(m, substrate.WithLogger(logger))
	ref, err := b.Snapshot(context.Background(), api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Memory || ref.Local != "sess-x" || ref.ExternalURI != "gcs://snap/sess-x" {
		t.Fatalf("unexpected snapshot ref %+v", ref)
	}
	if !strings.Contains(output.String(), `"operation":"suspend_actor"`) ||
		!strings.Contains(output.String(), `"reason":"snapshot"`) {
		t.Fatalf("snapshot did not log its SuspendActor call: %s", output.String())
	}
	if _, err := b.Restore(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if want := []string{"suspend:sess-x", "resume:sess-x:boot=false"}; !reflect.DeepEqual(m.calls, want) {
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
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.9"}}
	if _, err := newBackend(m).Fork(context.Background(), api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"get:child", "create:child", "resume:child:boot=true"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("fork calls=%v want a fresh cold child actor %v", m.calls, want)
	}
}

// A REQUIRES_MEMORY_SNAPSHOT harness's state lives only in RAM, so its fork must CLONE the parent's
// durable snapshot: tag it, create the child from that tag, and resume with boot=false. A boot=true
// resume here would discard exactly the state the fork exists to carry.
func TestForkClonesSnapshotForMemoryHarness(t *testing.T) {
	m := &mockCloner{mockControl: &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.11"}}}
	in, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tag:snap-parent-1->fork-child", "clone:child:from=fork-child", "resume:child:boot=false"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("fork calls=%v want a snapshot clone %v", m.calls, want)
	}
	if in.Address != "10.0.0.11:80" {
		t.Fatalf("cloned child address=%q want the child's own pod IP", in.Address)
	}
}

// Forking a memory harness with no parent snapshot must fail loudly. Cold-booting the child instead
// would hand back a child whose RAM is empty while its copied journal prefix says otherwise — a
// silently divergent branch, since such a harness never rebuilds state from History (I4).
func TestForkMemoryHarnessWithoutSnapshotIsRefused(t *testing.T) {
	m := &mockCloner{mockControl: &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.11"}}}
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrNoSnapshotToClone) {
		t.Fatalf("err=%v want ErrNoSnapshotToClone", err)
	}
	if len(m.calls) != 0 {
		t.Fatalf("a refused fork must not touch the control plane, got %v", m.calls)
	}
}

// A deployment predating substrate's ActorSnapshot APIs cannot clone. It must refuse a stateful fork
// rather than silently degrading to a replay-fork that loses the in-RAM state.
func TestForkMemoryHarnessWithoutClonerIsRefused(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.11"}}
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

// A tag is a retention pin, so a fork that fails after tagging must release it. Otherwise every
// failed fork attempt permanently pins one more snapshot, and upstream snapshot GC is still deferred.
func TestFailedCloneReleasesItsTag(t *testing.T) {
	m := &mockCloner{
		mockControl: &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.11"}},
		cloneErr:    errors.New("template mismatch"),
	}
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err == nil {
		t.Fatal("expected the clone to fail")
	}
	want := []string{"tag:snap-parent-1->fork-child", "untag:fork-child"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the tag to be released %v", m.calls, want)
	}
}

// A clone that is created and placed on a worker but reports no dialable address still holds that
// worker. Fork returns no handle in that case, so the child UID is discarded upstream and this is the
// last moment the actor can be named: it must be torn down here, not left running forever.
func TestForkTearsDownAClonePlacedWithoutAnAddress(t *testing.T) {
	m := &mockCloner{mockControl: &mockControl{info: substrate.ActorInfo{}}} // resumed, but no pod IP
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if err == nil {
		t.Fatal("a clone with no address must not be returned as a usable incarnation")
	}
	want := []string{
		"tag:snap-parent-1->fork-child", "clone:child:from=fork-child", "resume:child:boot=false",
		"suspend:child", "delete:child", "untag:fork-child",
	}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the stranded clone torn down %v", m.calls, want)
	}
}

// The likeliest reason a fork fails partway is that the caller's deadline expired or it went away.
// Cleanup bound to that same context would do nothing in exactly that case, so it must be detached.
func TestForkCleanupSurvivesACancelledCaller(t *testing.T) {
	m := &mockCloner{
		mockControl: &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.11"}},
		cloneErr:    errors.New("deadline exceeded"),
	}
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
