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

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/runtime/substrate"
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
	// resumeErr, when set, fails ResumeActor; getErr fails GetActor.
	resumeErr error
	getErr    error
	// uids is each actor's server-assigned UID, minted the first time the actor exists and dropped
	// when it is deleted, so an actor created again under a name reports a new one.
	uids    map[string]string
	created int
	// suspendErr fails SuspendActor; getNoUID and suspendNoUID make GetActor and SuspendActor report
	// no UID, as a ControlClient that breaks its contract would.
	suspendErr   error
	getNoUID     bool
	suspendNoUID bool
}

// exists records an actor at a status, so GetActor reports it as already placed.
func (m *mockControl) exists(name string, st substrate.ActorStatus) {
	if m.actors == nil {
		m.actors = map[string]substrate.ActorStatus{}
	}
	m.actors[name] = st
	m.uid(name)
}

// uid returns the actor's UID, minting one for an actor that has none yet.
func (m *mockControl) uid(name string) string {
	if m.uids == nil {
		m.uids = map[string]string{}
	}
	if _, ok := m.uids[name]; !ok {
		m.created++
		m.uids[name] = fmt.Sprintf("uid-%s-%d", name, m.created)
	}
	return m.uids[name]
}

func (m *mockControl) CreateActor(ctx context.Context, a substrate.ActorRef, _ substrate.ObjectRef) error {
	m.calls = append(m.calls, "create:"+a.Name)
	if _, ok := m.actors[a.Name]; ok {
		return fmt.Errorf("%w: %q", substrate.ErrActorExists, a.Name)
	}
	m.exists(a.Name, substrate.StatusRunning)
	return nil
}
func (m *mockControl) ResumeActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, "resume:"+a.Name)
	if m.resumeErr != nil {
		return substrate.ActorInfo{}, m.resumeErr
	}
	m.exists(a.Name, substrate.StatusRunning)
	info := m.info
	if info.UID == "" {
		info.UID = m.uid(a.Name)
	}
	return info, nil
}
func (m *mockControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	if err := ctx.Err(); err != nil {
		return substrate.ActorInfo{}, err
	}
	m.calls = append(m.calls, "suspend:"+a.Name)
	if m.suspendErr != nil {
		return substrate.ActorInfo{}, m.suspendErr
	}
	m.exists(a.Name, substrate.StatusSuspended)
	snap := "gs://snap/" + a.Name
	m.holds(a.Name, snap)
	uid := m.uid(a.Name)
	if m.suspendNoUID {
		uid = ""
	}
	return substrate.ActorInfo{Status: substrate.StatusSuspended, Worker: m.info.Worker, Snapshot: snap, UID: uid}, nil
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
	delete(m.uids, a.Name)
	return nil
}
func (m *mockControl) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, "get:"+a.Name)
	if m.afterGet != nil {
		defer m.afterGet(a.Name)
	}
	if m.getErr != nil {
		return substrate.ActorInfo{}, m.getErr
	}
	if st, ok := m.actors[a.Name]; ok {
		uid := m.uid(a.Name)
		if m.getNoUID {
			uid = ""
		}
		return substrate.ActorInfo{Status: st, Worker: m.info.Worker, Snapshot: m.snapshots[a.Name], UID: uid}, nil
	}
	if m.status != substrate.StatusUnknown { // canned state for the suspend/restore cases
		return substrate.ActorInfo{Status: m.status, Worker: m.info.Worker, Snapshot: m.snapshots[a.Name], UID: m.uid(a.Name)}, nil
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
	tagErr   error
}

func (m *mockCloner) TagActor(ctx context.Context, source substrate.ActorRef, tag substrate.SnapshotID) error {
	m.calls = append(m.calls, "tag:"+source.Name+"->"+tag.Name)
	return m.tagErr
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
	if !errors.Is(err, substrate.ErrSnapshotSuperseded) || !errors.Is(err, api.ErrSnapshotSuperseded) {
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
	if !errors.Is(err, substrate.ErrSnapshotSuperseded) || !errors.Is(err, api.ErrSnapshotSuperseded) {
		t.Fatalf("err=%v want ErrSnapshotSuperseded", err)
	}
	want := []string{"get:parent", "tag:parent->fork-child", "get:parent", "untag:fork-child"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the stale tag deleted and no child created %v", m.calls, want)
	}
}

// A parent deleted before or after the fork tags it took its snapshot with it: no child can be
// cloned and a retry cannot help, so the fork is api.ErrMemoryStateLost (FAILED_PRECONDITION at the
// Sessions API), not a plain error, and a tag taken before the parent vanished is released.
func TestForkOfADeletedParentIsMemoryStateLost(t *testing.T) {
	for _, tc := range []struct {
		name      string
		afterTag  bool // the parent is deleted between the first check and the tag, else before the fork
		wantCalls []string
	}{
		{name: "before the tag", wantCalls: []string{"get:parent"}},
		{name: "after the tag", afterTag: true, wantCalls: []string{"get:parent", "tag:parent->fork-child", "get:parent", "untag:fork-child"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newClonerWithSuspendedParent()
			if tc.afterTag {
				m.afterGet = func(name string) {
					delete(m.actors, name)
					m.afterGet = nil
				}
			} else {
				delete(m.actors, "parent")
			}
			_, err := newMemoryBackend(m).Fork(context.Background(),
				api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
				api.ForkOpts{ChildSessionUID: "child"})
			if !errors.Is(err, api.ErrMemoryStateLost) || !errors.Is(err, substrate.ErrActorNotFound) {
				t.Fatalf("fork of a deleted parent: err=%v, want ErrMemoryStateLost wrapping ErrActorNotFound", err)
			}
			if !reflect.DeepEqual(m.calls, tc.wantCalls) {
				t.Fatalf("calls=%v want %v", m.calls, tc.wantCalls)
			}
		})
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

// Substrate reserves a tag before copying the snapshot into it, so a tag whose copy failed is left
// pending, holding partial data, and blocks deleting the atespace. The fork must release it. It then
// reads the parent again to classify the failure; a parent that still holds the snapshot leaves the
// tag's own error, untyped.
func TestFailedTagReleasesTheReservedTag(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.tagErr = errors.New("while copying the external snapshot: object store unavailable")
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, m.tagErr) || errors.Is(err, api.ErrMemoryStateLost) || errors.Is(err, api.ErrSnapshotSuperseded) {
		t.Fatalf("err=%v, want the tag error alone", err)
	}
	want := []string{"get:parent", "tag:parent->fork-child", "untag:fork-child", "get:parent"}
	if !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want the reserved tag released %v", m.calls, want)
	}
}

// A tag that already existed was not reserved by this attempt, so the fork must leave it alone.
func TestTagThatAlreadyExistsIsNotDeleted(t *testing.T) {
	m := newClonerWithSuspendedParent()
	m.tagErr = fmt.Errorf("%w: space/fork-child", substrate.ErrTagExists)
	_, err := newMemoryBackend(m).Fork(context.Background(),
		api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-1", Memory: true},
		api.ForkOpts{ChildSessionUID: "child"})
	if !errors.Is(err, substrate.ErrTagExists) {
		t.Fatalf("err=%v want ErrTagExists", err)
	}
	if slices.Contains(m.calls, "untag:fork-child") {
		t.Fatalf("calls=%v deleted a tag this fork did not create", m.calls)
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

// A REQUIRES_MEMORY_SNAPSHOT session keeps state only in its actor's RAM. Once it has a started
// turn, an actor that no longer exists cannot be replaced by a fresh one from the template: that would
// continue the journal with a harness that remembers none of it. Create must refuse with
// ErrMemoryStateLost and create nothing. It counts the turns only after GetActor reports the actor
// missing ("count" follows "get"), so a turn journaled while the call was being placed is seen, and
// it fails closed when the count fails. Every other combination keeps the usual placement, without
// reading the journal.
func TestCreateRefusesToColdCreateAMemorySessionWithTurns(t *testing.T) {
	memory := func(m *mockControl) *substrate.Backend { return newMemoryBackend(m) }
	stateless := func(m *mockControl) *substrate.Backend { return newBackend(m) }
	errCount := errors.New("journal unreadable")
	for _, tc := range []struct {
		name      string
		backend   func(*mockControl) *substrate.Backend
		turns     int
		countErr  error
		existing  substrate.ActorStatus // StatusUnknown: the actor does not exist
		wantCalls []string
		wantErr   error
	}{
		{name: "memory tier with turns and no actor is refused", backend: memory, turns: 2,
			wantCalls: []string{"get:sess-x", "count"}, wantErr: substrate.ErrMemoryStateLost},
		{name: "memory tier whose count fails is refused", backend: memory, countErr: errCount,
			wantCalls: []string{"get:sess-x", "count"}, wantErr: errCount},
		{name: "memory tier with zero turns creates", backend: memory,
			wantCalls: []string{"get:sess-x", "count", "create:sess-x", "resume:sess-x"}},
		{name: "stateless tier with turns creates", backend: stateless, turns: 2,
			wantCalls: []string{"get:sess-x", "create:sess-x", "resume:sess-x"}},
		{name: "memory tier with turns attaches to a running actor", backend: memory, turns: 2, existing: substrate.StatusRunning,
			wantCalls: []string{"get:sess-x"}},
		{name: "memory tier with turns resumes a suspended actor", backend: memory, turns: 2, existing: substrate.StatusSuspended,
			wantCalls: []string{"get:sess-x", "resume:sess-x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockControl{info: running("worker-a")}
			if tc.existing != substrate.StatusUnknown {
				m.exists("sess-x", tc.existing)
			}
			count := func() (int, error) {
				m.calls = append(m.calls, "count")
				return tc.turns, tc.countErr
			}
			inc, err := tc.backend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x", StartedTurns: count})
			if !reflect.DeepEqual(m.calls, tc.wantCalls) {
				t.Errorf("calls=%v want %v", m.calls, tc.wantCalls)
			}
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				if inc.CallMetadata[substrate.TargetActorHeader] != "space/sess-x" {
					t.Fatalf("incarnation=%+v want the session's actor", inc)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Create error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == substrate.ErrMemoryStateLost {
				if !errors.Is(err, api.ErrMemoryStateLost) {
					t.Fatalf("Create error = %v, want it to wrap api.ErrMemoryStateLost", err)
				}
				for _, want := range []string{`"sess-x"`, "2 started turn(s)", "cannot be reconstructed"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err, want)
					}
				}
			}
			if _, ok := m.actors["sess-x"]; ok {
				t.Error("refused Create left an actor behind")
			}
		})
	}
}

// A caller that leaves StartedTurns nil declares a session with no started turns, so a memory-tier
// session's actor is created as on a new session.
func TestCreateWithNoTurnCountCreates(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	if _, err := newMemoryBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if want := []string{"get:sess-x", "create:sess-x", "resume:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Errorf("calls=%v want %v", m.calls, want)
	}
}

// Restore resumes the actor that holds the session's snapshot. When a REQUIRES_MEMORY_SNAPSHOT
// session's actor no longer exists, the snapshot went with it, so Restore reports ErrMemoryStateLost
// (FAILED_PRECONDITION at the Sessions API) instead of the resume error, and creates nothing. Every
// other failure keeps the resume error.
func TestRestoreOfAGoneMemoryActorIsMemoryStateLost(t *testing.T) {
	memory := func(m *mockControl) *substrate.Backend { return newMemoryBackend(m) }
	stateless := func(m *mockControl) *substrate.Backend { return newBackend(m) }
	resumeErr := errors.New("rpc error: code = NotFound desc = actor not found")
	notFound := fmt.Errorf("%w: space/sess-x: %w", substrate.ErrActorNotFound, resumeErr)
	for _, tc := range []struct {
		name      string
		backend   func(*mockControl) *substrate.Backend
		existing  substrate.ActorStatus // StatusUnknown: the actor does not exist
		getErr    error
		holds     string // the snapshot the existing actor holds
		snapshot  string // the ExternalURI restored: "" is gs://snap/sess-x, "-" is none
		resumeErr error  // nil: resumeErr below, which GetActor has to classify
		wantCalls []string
		wantLost  bool
		wantMsg   string // "" is "cannot be restored" alone
	}{
		{name: "memory tier without an actor", backend: memory,
			wantCalls: []string{"resume:sess-x", "get:sess-x"}, wantLost: true},
		{name: "memory tier whose actor exists keeps the resume error", backend: memory, existing: substrate.StatusSuspended,
			wantCalls: []string{"resume:sess-x", "get:sess-x"}},
		{name: "memory tier whose GetActor fails keeps the resume error", backend: memory, getErr: errors.New("unavailable"),
			wantCalls: []string{"resume:sess-x", "get:sess-x"}},
		{name: "stateless tier keeps the resume error", backend: stateless,
			wantCalls: []string{"resume:sess-x"}},
		// After a resume that reports the actor missing, GetActor decides: no actor, or one that
		// does not hold the snapshot (a replacement created after the resume failed), is lost.
		{name: "memory tier whose resume reports the actor missing and which is gone", backend: memory,
			resumeErr: notFound, wantCalls: []string{"resume:sess-x", "get:sess-x"}, wantLost: true},
		{name: "memory tier whose resume reports the actor missing and which is replaced", backend: memory, existing: substrate.StatusSuspended,
			resumeErr: notFound, wantCalls: []string{"resume:sess-x", "get:sess-x"}, wantLost: true, wantMsg: "no longer holds the snapshot"},
		{name: "memory tier whose resume reports the actor missing and no snapshot to compare", backend: memory, existing: substrate.StatusSuspended,
			holds: "gs://snap/sess-x", snapshot: "-", resumeErr: notFound, wantCalls: []string{"resume:sess-x", "get:sess-x"}, wantLost: true},
		// Substrate also answers NOT_FOUND for an actor that exists (a template volume it cannot
		// find); one that still holds the snapshot keeps the resume error.
		{name: "memory tier whose resume reports the actor missing but which holds the snapshot keeps the resume error", backend: memory,
			existing: substrate.StatusSuspended, holds: "gs://snap/sess-x", resumeErr: notFound, wantCalls: []string{"resume:sess-x", "get:sess-x"}},
		{name: "memory tier whose resume reports the actor missing and whose GetActor fails keeps the resume error", backend: memory,
			getErr: errors.New("unavailable"), resumeErr: notFound, wantCalls: []string{"resume:sess-x", "get:sess-x"}},
		{name: "stateless tier whose resume reports the actor missing keeps the resume error", backend: stateless,
			resumeErr: notFound, wantCalls: []string{"resume:sess-x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resumeErr := resumeErr
			if tc.resumeErr != nil {
				resumeErr = tc.resumeErr
			}
			m := &mockControl{info: running("worker-a"), resumeErr: resumeErr, getErr: tc.getErr}
			if tc.existing != substrate.StatusUnknown {
				m.exists("sess-x", tc.existing)
			}
			if tc.holds != "" {
				m.holds("sess-x", tc.holds)
			}
			uri := "gs://snap/sess-x"
			switch tc.snapshot {
			case "-":
				uri = ""
			case "":
			default:
				uri = tc.snapshot
			}
			_, err := tc.backend(m).Restore(context.Background(), api.SnapshotRef{Local: "sess-x", ExternalURI: uri, Memory: true})
			if !reflect.DeepEqual(m.calls, tc.wantCalls) {
				t.Errorf("calls=%v want %v", m.calls, tc.wantCalls)
			}
			if !tc.wantLost {
				if !errors.Is(err, resumeErr) || errors.Is(err, api.ErrMemoryStateLost) {
					t.Fatalf("Restore error = %v, want the resume error and not ErrMemoryStateLost", err)
				}
				return
			}
			if !errors.Is(err, substrate.ErrMemoryStateLost) || !errors.Is(err, api.ErrMemoryStateLost) || !errors.Is(err, resumeErr) {
				t.Fatalf("Restore error = %v, want ErrMemoryStateLost wrapping api.ErrMemoryStateLost and the resume error", err)
			}
			for _, want := range []string{`"sess-x"`, "space/sess-x", "cannot be restored", tc.wantMsg} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
			if _, ok := m.actors["sess-x"]; ok && tc.existing == substrate.StatusUnknown {
				t.Error("Restore created an actor")
			}
		})
	}
}

// For a REQUIRES_MEMORY_SNAPSHOT harness the incarnation carries the actor's UID as ComputeInstance on
// every path that hands one out (create, attach, resume of a suspended actor, Restore), so the host can
// tell the actor apart from one created later under the same name. A stateless harness's state is in
// the journal, so its incarnation carries none.
func TestIncarnationCarriesTheActorUIDForAMemoryHarness(t *testing.T) {
	ctx := context.Background()
	m := &mockControl{info: running("worker-a")}
	mem := newMemoryBackend(m)
	created, err := mem.Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	uid := m.uids["sess-x"]
	if uid == "" || created.ComputeInstance != uid {
		t.Fatalf("created incarnation ComputeInstance=%q, want the actor's UID %q", created.ComputeInstance, uid)
	}
	attached, err := mem.Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil || attached.ComputeInstance != uid {
		t.Fatalf("attach: ComputeInstance=%q err=%v, want %q", attached.ComputeInstance, err, uid)
	}
	m.exists("sess-x", substrate.StatusSuspended)
	resumed, err := mem.Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil || resumed.ComputeInstance != uid {
		t.Fatalf("resume of a suspended actor: ComputeInstance=%q err=%v, want %q", resumed.ComputeInstance, err, uid)
	}
	restored, err := mem.Restore(ctx, api.SnapshotRef{Local: "sess-x", Memory: true})
	if err != nil || restored.ComputeInstance != uid {
		t.Fatalf("restore: ComputeInstance=%q err=%v, want %q", restored.ComputeInstance, err, uid)
	}

	stateless, err := newBackend(&mockControl{info: running("worker-a")}).Create(ctx, &api.SessionSpec{SessionUID: "sess-y"})
	if err != nil || stateless.ComputeInstance != "" {
		t.Fatalf("stateless incarnation ComputeInstance=%q err=%v, want none", stateless.ComputeInstance, err)
	}
}

// uidless reports every actor without a UID, as a control client that does not read it would.
type uidless struct{ *mockControl }

func (u uidless) ResumeActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	info, err := u.mockControl.ResumeActor(ctx, a)
	info.UID = ""
	return info, err
}

func (u uidless) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	info, err := u.mockControl.GetActor(ctx, a)
	info.UID = ""
	return info, err
}

// A memory-tier session is never placed on an actor that reports no UID: the host could not tell it
// from a replacement, so the guard against continuing on lost state would be off without anyone
// noticing. A stateless harness does not need the UID and is placed as before.
func TestMemoryHarnessRefusesAnActorWithoutAUID(t *testing.T) {
	ctx := context.Background()
	_, err := newMemoryBackend(uidless{&mockControl{info: running("worker-a")}}).Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	if err == nil || !strings.Contains(err.Error(), "reported no UID") {
		t.Fatalf("memory harness on a UID-less actor: err=%v, want a refusal naming the missing UID", err)
	}
	stateless := substrate.New(uidless{&mockControl{info: running("worker-a")}}, "space", substrate.ObjectRef{Atespace: "tmpl", Name: "echo"},
		api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}})
	if _, err := stateless.Create(ctx, &api.SessionSpec{SessionUID: "sess-x"}); err != nil {
		t.Fatalf("stateless harness on a UID-less actor: %v", err)
	}
}

// Two hosts place a new session's first turn at once. Both find no actor; the other host's
// CreateActor wins, so this one's reports ErrActorExists. Create attaches to the winner's actor
// instead of failing the turn, and reports that actor's UID.
func TestCreateAttachesWhenAnotherHostCreatedTheActorFirst(t *testing.T) {
	m := &mockControl{info: running("worker-a")}
	var once bool
	m.afterGet = func(name string) {
		if !once {
			once = true
			m.exists(name, substrate.StatusRunning) // the other host's CreateActor and ResumeActor
		}
	}
	inc, err := newMemoryBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatalf("Create after losing the create race: %v", err)
	}
	if want := []string{"get:sess-x", "create:sess-x", "get:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want %v", m.calls, want)
	}
	if inc.ComputeInstance != m.uids["sess-x"] {
		t.Fatalf("ComputeInstance=%q, want the winner's actor UID %q", inc.ComputeInstance, m.uids["sess-x"])
	}

	// The winner's actor can be gone again by the time this host looks; that is reported, not retried.
	m = &mockControl{info: running("worker-a")}
	once = false
	m.afterGet = func(name string) {
		if !once {
			once = true
			m.exists(name, substrate.StatusRunning)
		}
	}
	_, err = newMemoryBackend(goneOnReread{m}).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if !errors.Is(err, substrate.ErrActorExists) {
		t.Fatalf("taken then gone: err=%v, want ErrActorExists", err)
	}
}

// goneOnReread deletes the actor just before a GetActor that follows a CreateActor answers, so
// Create's re-read after ErrActorExists finds nothing.
type goneOnReread struct{ *mockControl }

func (g goneOnReread) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	if slices.Contains(g.calls, "create:"+a.Name) {
		delete(g.actors, a.Name)
	}
	return g.mockControl.GetActor(ctx, a)
}

// A stateful fork passes the compute instance the journal last recorded, and Snapshot refuses an
// actor that replaced it (api.ErrMemoryStateLost): before suspending when it can see the
// replacement, and after, on the UID the suspend reports, when the actor was replaced between the
// look and the suspend. With no instance required, Snapshot suspends whatever actor the name holds,
// as before.
func TestSnapshotRefusesAReplacedActor(t *testing.T) {
	ctx := context.Background()
	newRunning := func() *mockControl {
		m := &mockControl{info: running("worker-a")}
		m.exists("sess-x", substrate.StatusRunning)
		return m
	}

	m := newRunning()
	uid := m.uids["sess-x"]
	ref, err := newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: uid}, api.SnapshotExternal)
	if err != nil || ref.ExternalURI == "" {
		t.Fatalf("snapshot of the recorded actor: ref=%+v err=%v, want a snapshot", ref, err)
	}

	m = newRunning()
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: "uid-recorded"}, api.SnapshotExternal)
	if !errors.Is(err, api.ErrMemoryStateLost) || !strings.Contains(err.Error(), "uid-recorded") {
		t.Fatalf("snapshot of a replacement: err=%v, want ErrMemoryStateLost naming the recorded instance", err)
	}
	if want := []string{"get:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want %v: a replacement seen before the suspend is not suspended", m.calls, want)
	}

	m = newRunning()
	uid = m.uids["sess-x"]
	m.afterGet = func(name string) {
		// The actor is deleted and created again under its name between the look and the suspend.
		delete(m.actors, name)
		delete(m.uids, name)
		m.exists(name, substrate.StatusRunning)
		m.afterGet = nil
	}
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: uid}, api.SnapshotExternal)
	if !errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("actor replaced between the look and the suspend: err=%v, want ErrMemoryStateLost", err)
	}

	// An actor that is gone took the session's state with it: that is ErrMemoryStateLost, as Exec and
	// Resume report it, whether it is missing at the look or by the time of the suspend.
	m = &mockControl{info: running("worker-a")}
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: "uid-recorded"}, api.SnapshotExternal)
	if !errors.Is(err, api.ErrMemoryStateLost) || !errors.Is(err, substrate.ErrActorNotFound) || !strings.Contains(err.Error(), "uid-recorded") {
		t.Fatalf("snapshot of a missing actor: err=%v, want ErrMemoryStateLost wrapping ErrActorNotFound and naming the instance", err)
	}
	if want := []string{"get:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want %v: a missing actor is not suspended", m.calls, want)
	}
	m = newRunning()
	uid = m.uids["sess-x"]
	m.suspendErr = fmt.Errorf("deleted after the look: %w", substrate.ErrActorNotFound)
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: uid}, api.SnapshotExternal)
	if !errors.Is(err, api.ErrMemoryStateLost) || !errors.Is(err, substrate.ErrActorNotFound) {
		t.Fatalf("actor deleted between the look and the suspend: err=%v, want ErrMemoryStateLost wrapping ErrActorNotFound", err)
	}
	// Any other control error stays what it was.
	m = newRunning()
	uid = m.uids["sess-x"]
	m.suspendErr = errors.New("control plane down")
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: uid}, api.SnapshotExternal)
	if err == nil || errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("suspend failure: err=%v, want a plain error", err)
	}
	// A memory-tier actor with no recorded instance (a fork child before its first turn) that is
	// gone took the cloned state with it too.
	m = &mockControl{info: running("worker-a"), suspendErr: substrate.ErrActorNotFound}
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if !errors.Is(err, api.ErrMemoryStateLost) || !errors.Is(err, substrate.ErrActorNotFound) {
		t.Fatalf("snapshot of a missing memory-tier actor with no recorded instance: err=%v, want ErrMemoryStateLost wrapping ErrActorNotFound", err)
	}
	// A stateless snapshot of a missing actor is not memory-state loss: the journal holds the state.
	m = &mockControl{info: running("worker-a"), suspendErr: substrate.ErrActorNotFound}
	_, err = newBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if !errors.Is(err, substrate.ErrActorNotFound) || errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("stateless snapshot of a missing actor: err=%v, want ErrActorNotFound only", err)
	}

	// An actor that reports no UID cannot be compared. That is a ControlClient fault, not a
	// replacement, so it is a plain error that does not claim the actor was replaced.
	for _, tc := range []struct {
		name      string
		set       func(*mockControl)
		wantCalls []string
	}{
		{"look", func(m *mockControl) { m.getNoUID = true }, []string{"get:sess-x"}},
		{"suspend", func(m *mockControl) { m.suspendNoUID = true }, []string{"get:sess-x", "suspend:sess-x"}},
	} {
		m = newRunning()
		uid = m.uids["sess-x"]
		tc.set(m)
		_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x", ComputeInstance: uid}, api.SnapshotExternal)
		if err == nil || errors.Is(err, api.ErrMemoryStateLost) || !strings.Contains(err.Error(), "reported no UID") ||
			strings.Contains(err.Error(), "replaced") {
			t.Fatalf("no UID at the %s: err=%v, want a plain error saying the actor reported no UID", tc.name, err)
		}
		if !reflect.DeepEqual(m.calls, tc.wantCalls) {
			t.Fatalf("no UID at the %s: calls=%v want %v", tc.name, m.calls, tc.wantCalls)
		}
	}

	m = newRunning()
	if _, err := newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x"}, api.SnapshotExternal); err != nil {
		t.Fatalf("snapshot with no instance required: %v", err)
	}
	if want := []string{"suspend:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want %v", m.calls, want)
	}

	// A memory-tier actor with no recorded instance (a fork child before its first turn) must still
	// report a UID from the suspend: the ControlClient contract requires one. A stateless backend
	// does not depend on it.
	m = newRunning()
	m.suspendNoUID = true
	_, err = newMemoryBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if err == nil || errors.Is(err, api.ErrMemoryStateLost) || !strings.Contains(err.Error(), "reported no UID") ||
		strings.Contains(err.Error(), "instance \"\"") {
		t.Fatalf("memory-tier suspend with no UID and no recorded instance: err=%v, want a plain error saying the actor reported no UID", err)
	}
	m = newRunning()
	m.suspendNoUID = true
	if _, err := newBackend(m).Snapshot(ctx, api.Incarnation{ID: "sess-x"}, api.SnapshotExternal); err != nil {
		t.Fatalf("stateless suspend with no UID and no recorded instance: %v", err)
	}
}

// The lost-state message reads the same whether or not an instance was recorded.
func TestSnapshotOfAMissingActorMessage(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		in   api.Incarnation
		want string
	}{
		{"recorded instance", api.Incarnation{ID: "sess-x", ComputeInstance: "uid-1"},
			`actor space/sess-x, instance "uid-1", which held the session's in-RAM state, no longer exists, `},
		{"no recorded instance", api.Incarnation{ID: "sess-x"},
			`actor space/sess-x, which held the session's in-RAM state, no longer exists, `},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockControl{info: running("worker-a"), suspendErr: substrate.ErrActorNotFound}
			_, err := newMemoryBackend(m).Snapshot(ctx, tc.in, api.SnapshotExternal)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// An actor in the middle of another operation (usually another host resuming the actor it just
// created) settles on its own, so Create reports it as ErrActorBusy, which wraps api.ErrComputeBusy
// and is retryable, whether Create finds it on its first look, after losing the CreateActor race,
// or when substrate refuses the resume of a suspended actor because another operation holds it.
func TestCreateOnABusyActorIsRetryable(t *testing.T) {
	ctx := context.Background()
	check := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, substrate.ErrActorBusy) || !errors.Is(err, api.ErrComputeBusy) {
			t.Fatalf("%s: err=%v, want ErrActorBusy wrapping api.ErrComputeBusy", name, err)
		}
	}

	m := &mockControl{info: running("worker-a")}
	m.exists("sess-x", substrate.StatusTransitioning)
	_, err := newMemoryBackend(m).Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	check("actor mid-transition", err)

	m = &mockControl{info: running("worker-a")}
	once := false
	m.afterGet = func(name string) {
		if !once {
			once = true
			m.exists(name, substrate.StatusTransitioning) // the other host created it and is resuming it
		}
	}
	_, err = newMemoryBackend(m).Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	check("lost the create race to an actor mid-resume", err)
	if want := []string{"get:sess-x", "create:sess-x", "get:sess-x"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("calls=%v want %v", m.calls, want)
	}

	m = &mockControl{info: running("worker-a"), resumeErr: fmt.Errorf("%w: another operation is in progress", substrate.ErrActorBusy)}
	m.exists("sess-x", substrate.StatusSuspended)
	_, err = newMemoryBackend(m).Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	check("resume refused while another operation holds the actor", err)

	m = &mockControl{info: running("worker-a")}
	m.exists("sess-x", substrate.StatusUnknown)
	_, err = newMemoryBackend(m).Create(ctx, &api.SessionSpec{SessionUID: "sess-x"})
	if err == nil || errors.Is(err, api.ErrComputeBusy) {
		t.Fatalf("actor in an unknown state: err=%v, want a failure that is not retryable", err)
	}
}

// Create looks at the actor, then resumes it if it is SUSPENDED. When the resume reports the actor
// missing, Create reads it again rather than fail with the bare resume error: gone, it is a missing
// actor (a memory-tier session with turns is ErrMemoryStateLost, anything else gets a new actor);
// replaced under the name, or deleted again after CreateActor found the name taken, it is
// ErrActorBusy, so a retry looks afresh; still there (substrate also answers NOT_FOUND for a template
// volume it cannot find), the resume error stands.
func TestCreateWhoseActorChangesDuringTheResume(t *testing.T) {
	ctx := context.Background()
	notFound := fmt.Errorf("%w: space/sess-x: rpc error: code = NotFound", substrate.ErrActorNotFound)
	// gone deletes the actor out of band, without recording a call.
	gone := func(m *mockControl) {
		delete(m.actors, "sess-x")
		delete(m.uids, "sess-x")
	}
	for _, tc := range []struct {
		name string
		// memory selects the REQUIRES_MEMORY_SNAPSHOT tier; turns is what StartedTurns reports.
		memory bool
		turns  int
		// absent starts with no actor; otherwise the actor is SUSPENDED.
		absent bool
		// onGet runs after the n-th GetActor (1-based) answers.
		onGet     func(m *mockControl, n int)
		wantCalls []string
		wantErr   error // nil: success
		notErr    []error
	}{
		{
			name: "memory tier with turns whose actor is deleted is lost", memory: true, turns: 1,
			onGet: func(m *mockControl, n int) {
				if n == 1 {
					gone(m)
				}
			},
			wantCalls: []string{"get:sess-x", "resume:sess-x", "get:sess-x", "count"},
			wantErr:   substrate.ErrMemoryStateLost,
		},
		{
			name: "stateless tier whose actor is deleted gets a new actor",
			onGet: func(m *mockControl, n int) {
				switch n {
				case 1:
					gone(m)
				case 2:
					m.resumeErr = nil // the new actor resumes
				}
			},
			wantCalls: []string{"get:sess-x", "resume:sess-x", "get:sess-x", "create:sess-x", "resume:sess-x"},
		},
		{
			name: "memory tier whose actor is replaced is retryable", memory: true, turns: 1,
			onGet: func(m *mockControl, n int) {
				if n == 1 {
					gone(m)
					m.exists("sess-x", substrate.StatusSuspended)
				}
			},
			wantCalls: []string{"get:sess-x", "resume:sess-x", "get:sess-x"},
			wantErr:   substrate.ErrActorBusy,
			notErr:    []error{api.ErrMemoryStateLost},
		},
		{
			name: "memory tier whose actor is still there keeps the resume error", memory: true, turns: 1,
			wantCalls: []string{"get:sess-x", "resume:sess-x", "get:sess-x"},
			wantErr:   notFound,
			notErr:    []error{api.ErrMemoryStateLost, api.ErrComputeBusy},
		},
		{
			name: "an actor created by another host and deleted before its resume is retryable", memory: true, absent: true,
			onGet: func(m *mockControl, n int) {
				switch n {
				case 1:
					m.exists("sess-x", substrate.StatusSuspended) // the other host creates it
				case 2:
					gone(m)
				}
			},
			wantCalls: []string{"get:sess-x", "count", "create:sess-x", "get:sess-x", "resume:sess-x", "get:sess-x"},
			wantErr:   substrate.ErrActorBusy,
			notErr:    []error{api.ErrMemoryStateLost},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockControl{info: running("worker-a"), resumeErr: notFound}
			if !tc.absent {
				m.exists("sess-x", substrate.StatusSuspended)
			}
			gets := 0
			m.afterGet = func(string) {
				gets++
				if tc.onGet != nil {
					tc.onGet(m, gets)
				}
			}
			b := newBackend(m)
			if tc.memory {
				b = newMemoryBackend(m)
			}
			count := func() (int, error) {
				m.calls = append(m.calls, "count")
				return tc.turns, nil
			}
			_, err := b.Create(ctx, &api.SessionSpec{SessionUID: "sess-x", StartedTurns: count})
			if !reflect.DeepEqual(m.calls, tc.wantCalls) {
				t.Errorf("calls=%v want %v", m.calls, tc.wantCalls)
			}
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Create error = %v, want %v", err, tc.wantErr)
			}
			for _, not := range tc.notErr {
				if errors.Is(err, not) {
					t.Errorf("Create error = %v, want it not to wrap %v", err, not)
				}
			}
		})
	}
}
