package ateadapter

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/runtime/substrate"
	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// fakeControl is an ate-api Control double: it records calls and returns canned actors so the
// translation is tested without a real ate-api-server. Only the mapped methods are overridden; the
// rest are promoted from the embedded (nil) interface and never called.
type fakeControl struct {
	atepb.ControlClient
	calls      []string
	lastCreate *atepb.Actor
	lastTag    *atepb.Tag
	actor      *atepb.Actor
	getErr     error
	tagErr     error
	createErr  error
	resumeErr  error
	suspendErr error
	// onGet, when set, runs before GetActor answers its n-th call (1-based).
	onGet func(n int)
	gets  int
}

func (f *fakeControl) CreateActor(_ context.Context, in *atepb.CreateActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	a := in.GetActor()
	f.calls = append(f.calls, "create:"+a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName()+
		":tmpl="+a.GetActorTemplate().GetAtespace()+"/"+a.GetActorTemplate().GetName())
	f.lastCreate = a
	if f.createErr != nil {
		return nil, f.createErr
	}
	return a, nil
}
func (f *fakeControl) ResumeActor(_ context.Context, in *atepb.ResumeActorRequest, _ ...grpc.CallOption) (*atepb.ResumeActorResponse, error) {
	f.calls = append(f.calls, "resume:"+in.GetActor().GetAtespace()+"/"+in.GetActor().GetName())
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	return &atepb.ResumeActorResponse{Actor: f.actor, Resumed: true}, nil
}
func (f *fakeControl) SuspendActor(_ context.Context, in *atepb.SuspendActorRequest, _ ...grpc.CallOption) (*atepb.SuspendActorResponse, error) {
	f.calls = append(f.calls, "suspend:"+in.GetActor().GetName())
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
	return &atepb.SuspendActorResponse{Actor: f.actor}, nil
}
func (f *fakeControl) DeleteActor(_ context.Context, in *atepb.DeleteActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	f.calls = append(f.calls, "delete:"+in.GetActor().GetName())
	return &atepb.Actor{}, nil
}
func (f *fakeControl) GetActor(_ context.Context, in *atepb.GetActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	f.calls = append(f.calls, "get:"+in.GetActor().GetName())
	f.gets++
	if f.onGet != nil {
		f.onGet(f.gets)
	}
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.actor, nil
}
func (f *fakeControl) CreateTag(_ context.Context, in *atepb.CreateTagRequest, _ ...grpc.CallOption) (*atepb.Tag, error) {
	tg := in.GetTag()
	f.calls = append(f.calls, "tag:"+tg.GetSourceActor().GetAtespace()+"/"+tg.GetSourceActor().GetName()+
		"->"+tg.GetMetadata().GetAtespace()+"/"+tg.GetMetadata().GetName())
	f.lastTag = tg
	if f.tagErr != nil {
		return nil, f.tagErr
	}
	return tg, nil
}
func (f *fakeControl) DeleteTag(_ context.Context, in *atepb.DeleteTagRequest, _ ...grpc.CallOption) (*atepb.Tag, error) {
	f.calls = append(f.calls, "untag:"+in.GetTag().GetAtespace()+"/"+in.GetTag().GetName())
	return &atepb.Tag{}, nil
}

func runningActor() *atepb.Actor {
	return &atepb.Actor{
		Metadata: &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x", Uid: "0b5c3c56-6d5e-4f0e-9b1a-2f4d8c1e7a90"},
		Status: &atepb.ActorStatus{
			State: atepb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &atepb.WorkerAssignment{
				WorkerPod:    "echo-harness-7d9f-abcde",
				WorkerPodIps: []string{"10.0.0.5"},
			},
			Snapshots:              []*atepb.Snapshot{durableSnapshot(1, "gs://b/atespaces/space/actors/u1/snapshots/s1", atepb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)},
			LastAssignedGeneration: 1,
		},
	}
}

func durableSnapshot(gen int32, uri string, st atepb.SnapshotStorageStatus) *atepb.Snapshot {
	return &atepb.Snapshot{
		Uuid:       fmt.Sprintf("s%d", gen),
		Generation: gen,
		Owner:      atepb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:   atepb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		DurableSnapshot: &atepb.SnapshotStorage{
			Status: st,
			Object: &atepb.ObjectSnapshot{SnapshotUri: uri},
		},
	}
}

// The template is an atespaced substrate resource, referenced by (atespace, name) on the actor.
func TestCreateMapsTemplate(t *testing.T) {
	f := &fakeControl{}
	err := FromClient(f).CreateActor(context.Background(),
		substrate.ActorRef{Atespace: "space", Name: "sess-x"},
		substrate.ObjectRef{Atespace: "tmpl", Name: "echo"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"create:space/sess-x:tmpl=tmpl/echo"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
	if f.lastCreate.GetSourceTag() != nil {
		t.Fatalf("a plain create must not name a source tag, got %v", f.lastCreate.GetSourceTag())
	}
}

// A name another caller already took is reported as substrate.ErrActorExists, keeping the gRPC
// error, so the backend can attach to that actor; any other create error is returned unchanged.
func TestCreateReportsATakenName(t *testing.T) {
	ref := substrate.ActorRef{Atespace: "space", Name: "sess-x"}
	tmpl := substrate.ObjectRef{Atespace: "space", Name: "echo"}
	taken := status.Error(codes.AlreadyExists, "actor space/sess-x already exists")
	err := FromClient(&fakeControl{createErr: taken}).CreateActor(context.Background(), ref, tmpl)
	if !errors.Is(err, substrate.ErrActorExists) || !errors.Is(err, taken) {
		t.Fatalf("taken name: err=%v, want ErrActorExists wrapping the AlreadyExists error", err)
	}
	other := status.Error(codes.FailedPrecondition, "template has no golden snapshot")
	err = FromClient(&fakeControl{createErr: other}).CreateActor(context.Background(), ref, tmpl)
	if errors.Is(err, substrate.ErrActorExists) || !errors.Is(err, other) {
		t.Fatalf("other create error: err=%v, want it unchanged", err)
	}
}

// GetActor reports the actor's server-assigned UID, which an actor created again under the same name
// does not share.
func TestGetReportsTheActorUID(t *testing.T) {
	first := runningActor()
	again := runningActor()
	again.Metadata.Uid = "7f1e2d3c-4b5a-4968-8776-655443322110"
	ref := substrate.ActorRef{Atespace: "space", Name: "sess-x"}
	a, err := FromClient(&fakeControl{actor: first}).GetActor(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := FromClient(&fakeControl{actor: again}).GetActor(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if a.UID != first.GetMetadata().GetUid() || b.UID != again.GetMetadata().GetUid() || a.UID == b.UID {
		t.Fatalf("UIDs = %q and %q, want the two actors' distinct metadata UIDs", a.UID, b.UID)
	}
}

// ResumeActor reports the actor's state, its worker (diagnostics only) and the snapshot it holds.
func TestResumeMapsActorState(t *testing.T) {
	f := &fakeControl{actor: runningActor()}
	info, err := FromClient(f).ResumeActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	want := substrate.ActorInfo{
		Status:   substrate.StatusRunning,
		Worker:   "echo-harness-7d9f-abcde",
		Snapshot: "gs://b/atespaces/space/actors/u1/snapshots/s1",
		UID:      "0b5c3c56-6d5e-4f0e-9b1a-2f4d8c1e7a90",
	}
	if info != want {
		t.Fatalf("info=%+v want %+v", info, want)
	}
	if want := []string{"resume:space/sess-x"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
}

// The bytes here are encoded by hand with the field numbers of ateapi.proto at substrate 96dffb28,
// not by the generated package, so a regenerated client whose numbering no longer matches that
// server fails here instead of silently on a cluster:
//
//	Actor.status = 7; ActorStatus.state = 1, worker_assignment = 2, snapshots = 11;
//	WorkerAssignment.worker_pod = 3; Snapshot.generation = 2, durable_snapshot = 7;
//	SnapshotStorage.status = 1, object = 2; ObjectSnapshot.snapshot_uri = 1;
//	ACTOR_STATE_RUNNING = 2; SNAPSHOT_STORAGE_STATUS_COMPLETED = 2.
func TestGetActorDecodesCurrentWireFormat(t *testing.T) {
	var st []byte
	st = protowire.AppendVarint(protowire.AppendTag(st, 1, protowire.VarintType), 2)
	st = append(st, wireMessage(2, wireString(3, "echo-harness-7d9f-abcde"))...)
	var storage []byte
	storage = protowire.AppendVarint(protowire.AppendTag(storage, 1, protowire.VarintType), 2)
	storage = append(storage, wireMessage(2, wireString(1, "gs://b/atespaces/space/actors/u1/snapshots/s1"))...)
	var snap []byte
	snap = protowire.AppendVarint(protowire.AppendTag(snap, 2, protowire.VarintType), 1)
	snap = append(snap, wireMessage(7, storage)...)
	st = append(st, wireMessage(11, snap)...)

	info := decodeActorInfo(t, wireMessage(7, st))
	if info.Status != substrate.StatusRunning || info.Worker != "echo-harness-7d9f-abcde" ||
		info.Snapshot != "gs://b/atespaces/space/actors/u1/snapshots/s1" {
		t.Fatalf("decoded info=%+v, want a running actor on its worker holding its snapshot", info)
	}
}

// A server older than substrate f03327bd sends the held snapshot as ActorStatus.external_snapshot
// (field 4), which this client does not read. It must report no snapshot rather than fail to decode,
// so a stateful fork against such a server refuses (unplaceable through the Placer,
// ErrNoSnapshotToClone from a direct Backend.Fork) instead of tagging.
func TestGetActorIgnoresPreSnapshotsListField(t *testing.T) {
	var st []byte
	st = protowire.AppendVarint(protowire.AppendTag(st, 1, protowire.VarintType), 4) // SUSPENDED
	st = append(st, wireMessage(4, wireString(1, "gs://b/atespaces/space/actors/u1/snapshots/s1"))...)

	info := decodeActorInfo(t, wireMessage(7, st))
	if info.Status != substrate.StatusSuspended || info.Snapshot != "" {
		t.Fatalf("decoded info=%+v, want a suspended actor reporting no snapshot", info)
	}
}

func wireMessage(num protowire.Number, body []byte) []byte {
	b := protowire.AppendTag(nil, num, protowire.BytesType)
	return protowire.AppendBytes(b, body)
}

func wireString(num protowire.Number, v string) []byte {
	b := protowire.AppendTag(nil, num, protowire.BytesType)
	return protowire.AppendString(b, v)
}

func decodeActorInfo(t *testing.T, wire []byte) substrate.ActorInfo {
	t.Helper()
	var decoded atepb.Actor
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	info, err := FromClient(&fakeControl{actor: &decoded}).GetActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// The held snapshot is the one substrate restores and tags: the highest generation whose durable
// copy is COMPLETED. A newer PENDING copy (suspend in flight), a node-local pause checkpoint and an
// older completed generation are all skipped, whatever their order in the list. The newest
// completed generation is the held one even without a URI: the result is then empty (the caller
// refuses), never an older generation, since that is not the snapshot substrate would tag.
func TestHeldSnapshotIsLatestCompletedDurable(t *testing.T) {
	const (
		completed = atepb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		pending   = atepb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	)
	local := &atepb.Snapshot{Uuid: "s5", Generation: 5, Owner: atepb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, Locality: "node-a"}
	for _, tc := range []struct {
		name  string
		snaps []*atepb.Snapshot
		want  string
	}{
		{name: "none", want: ""},
		{name: "only pending", snaps: []*atepb.Snapshot{durableSnapshot(1, "uri-1", pending)}, want: ""},
		{name: "only local", snaps: []*atepb.Snapshot{local}, want: ""},
		{name: "single completed", snaps: []*atepb.Snapshot{durableSnapshot(1, "uri-1", completed)}, want: "uri-1"},
		{
			name:  "newer pending is not held",
			snaps: []*atepb.Snapshot{durableSnapshot(2, "uri-2", completed), durableSnapshot(3, "uri-3", pending)},
			want:  "uri-2",
		},
		{
			name:  "highest completed wins regardless of order",
			snaps: []*atepb.Snapshot{durableSnapshot(4, "uri-4", completed), local, durableSnapshot(2, "uri-2", completed)},
			want:  "uri-4",
		},
		{
			name:  "newest completed without a uri is held, not an older one",
			snaps: []*atepb.Snapshot{durableSnapshot(2, "uri-2", completed), durableSnapshot(3, "", completed)},
			want:  "",
		},
		{
			name:  "equal generations: the later entry wins, as in substrate",
			snaps: []*atepb.Snapshot{durableSnapshot(3, "uri-3a", completed), durableSnapshot(3, "uri-3b", completed)},
			want:  "uri-3b",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := heldSnapshot(&atepb.ActorStatus{Snapshots: tc.snaps}); got != tc.want {
				t.Fatalf("heldSnapshot=%q want %q", got, tc.want)
			}
		})
	}
	if got := heldSnapshot(nil); got != "" {
		t.Fatalf("heldSnapshot(nil)=%q want empty", got)
	}
}

// A tag name that is already taken was not reserved by this call. The backend must be able to tell,
// so it does not delete someone else's tag while cleaning up its own failed attempt.
func TestTagActorReportsAlreadyExistsAsSentinel(t *testing.T) {
	f := &fakeControl{tagErr: status.Error(codes.AlreadyExists, "Tag space/fork-c already exists")}
	err := FromClient(f).TagActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "p"}, substrate.SnapshotID{Atespace: "space", Name: "fork-c"})
	if !errors.Is(err, substrate.ErrTagExists) {
		t.Fatalf("err=%v want ErrTagExists", err)
	}
	f.tagErr = status.Error(codes.Unavailable, "object store unavailable")
	err = FromClient(f).TagActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "p"}, substrate.SnapshotID{Atespace: "space", Name: "fork-c"})
	if err == nil || errors.Is(err, substrate.ErrTagExists) {
		t.Fatalf("err=%v want a failure that is not ErrTagExists", err)
	}
}

func TestGetActorReportsNotFoundAsSentinel(t *testing.T) {
	f := &fakeControl{getErr: status.Error(codes.NotFound, "actor not found")}
	_, err := FromClient(f).GetActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "gone"})
	if !errors.Is(err, substrate.ErrActorNotFound) {
		t.Fatalf("err=%v want ErrActorNotFound", err)
	}
}

// Substrate refuses SuspendActor of a missing actor with NOT_FOUND (RPCService.SuspendActor in
// cmd/ateapi/internal/controlapi/actor.go, from the store lookup in
// cmd/ateapi/internal/controlapi/workflow_suspend.go). The ControlClient contract requires
// ErrActorNotFound for it, as from GetActor, so a memory-tier Snapshot can report the session's
// in-RAM state as lost; it is not ErrActorBusy, and substrate's reason is kept.
func TestSuspendActorReportsNotFoundAsSentinel(t *testing.T) {
	f := &fakeControl{suspendErr: status.Error(codes.NotFound, "Actor gone not found")}
	_, err := FromClient(f).SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "gone"})
	if !errors.Is(err, substrate.ErrActorNotFound) || errors.Is(err, substrate.ErrActorBusy) || !strings.Contains(err.Error(), "Actor gone not found") {
		t.Fatalf("err=%v want ErrActorNotFound carrying substrate's reason", err)
	}
}

// Substrate refuses ResumeActor of a missing actor with NOT_FOUND (RPCService.ResumeActor in
// cmd/ateapi/internal/controlapi/actor.go). The adapter reports it as ErrActorNotFound, so a
// memory-tier Restore reports the session's in-RAM state as lost from the resume itself; substrate's
// reason is kept.
func TestResumeActorReportsNotFoundAsSentinel(t *testing.T) {
	f := &fakeControl{resumeErr: status.Error(codes.NotFound, "Actor space/gone not found")}
	_, err := FromClient(f).ResumeActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "gone"})
	if !errors.Is(err, substrate.ErrActorNotFound) || errors.Is(err, substrate.ErrActorBusy) || !strings.Contains(err.Error(), "Actor space/gone not found") {
		t.Fatalf("err=%v want ErrActorNotFound carrying substrate's reason", err)
	}
}

// A suspend yields the URI of the external snapshot the actor now holds, the handle the backend
// records and later compares against GetActor to detect a superseded snapshot, and the UID of the
// actor it suspended, which a stateful fork compares with the one the journal recorded.
func TestSuspendReturnsTheHeldSnapshot(t *testing.T) {
	a := runningActor()
	a.Status.State = atepb.ActorState_ACTOR_STATE_SUSPENDED
	a.Status.WorkerAssignment = nil
	f := &fakeControl{actor: a}
	got, err := FromClient(f).SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "gs://b/atespaces/space/actors/u1/snapshots/s1"; got.Snapshot != want {
		t.Fatalf("snapshot=%q want %q", got.Snapshot, want)
	}
	if want := a.GetMetadata().GetUid(); got.UID != want {
		t.Fatalf("uid=%q want %q", got.UID, want)
	}
}

// A snapshot still being written is not one the actor holds, so a suspend that has not completed
// its durable copy reports no handle.
func TestSuspendWithoutCompletedSnapshotReturnsEmpty(t *testing.T) {
	f := &fakeControl{actor: &atepb.Actor{
		Metadata: &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status: &atepb.ActorStatus{
			State:                  atepb.ActorState_ACTOR_STATE_SUSPENDING,
			Snapshots:              []*atepb.Snapshot{durableSnapshot(1, "gs://b/atespaces/space/actors/u1/snapshots/s1", atepb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)},
			LastAssignedGeneration: 1,
		},
	}}
	got, err := FromClient(f).SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Snapshot != "" {
		t.Fatalf("snapshot=%q want empty: an in-progress snapshot is not one the actor holds", got.Snapshot)
	}
}

// The clone path substrate requires: tag the suspended parent ACTOR, then create the child FROM THE
// TAG under the same template, then delete the tag on teardown.
func TestCloneTagsParentThenCreatesFromTag(t *testing.T) {
	f := &fakeControl{}
	ad := FromClient(f)
	parent := substrate.ActorRef{Atespace: "space", Name: "parent"}
	tag := substrate.SnapshotID{Atespace: "space", Name: "fork-child"}

	if err := ad.TagActor(context.Background(), parent, tag); err != nil {
		t.Fatal(err)
	}
	if err := ad.CreateActorFromTag(context.Background(),
		substrate.ActorRef{Atespace: "space", Name: "child"},
		substrate.ObjectRef{Atespace: "tmpl", Name: "counter"}, tag); err != nil {
		t.Fatal(err)
	}
	if err := ad.DeleteTag(context.Background(), tag); err != nil {
		t.Fatal(err)
	}

	want := []string{"tag:space/parent->space/fork-child", "create:space/child:tmpl=tmpl/counter", "untag:space/fork-child"}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
	if src := f.lastCreate.GetSourceTag(); src.GetAtespace() != "space" || src.GetName() != "fork-child" {
		t.Fatalf("child must be seeded from the fork tag, got %v", src)
	}
	// A fork's child is created in its parent's atespace, so the unpublished scope suffices.
	if f.lastTag.GetScope() != atepb.TagScope_TAG_SCOPE_ATESPACE {
		t.Fatalf("tag scope=%v want ATESPACE (no cross-atespace publication)", f.lastTag.GetScope())
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		s    atepb.ActorState
		want substrate.ActorStatus
	}{
		{atepb.ActorState_ACTOR_STATE_RUNNING, substrate.StatusRunning},
		{atepb.ActorState_ACTOR_STATE_SUSPENDED, substrate.StatusSuspended},
		{atepb.ActorState_ACTOR_STATE_PAUSED, substrate.StatusSuspended},
		{atepb.ActorState_ACTOR_STATE_CRASHED, substrate.StatusTerminated},
		{atepb.ActorState_ACTOR_STATE_RESUMING, substrate.StatusTransitioning},
		{atepb.ActorState_ACTOR_STATE_SUSPENDING, substrate.StatusTransitioning},
		{atepb.ActorState_ACTOR_STATE_PAUSING, substrate.StatusTransitioning},
		{atepb.ActorState_ACTOR_STATE_DELETING, substrate.StatusUnknown},
		{atepb.ActorState_ACTOR_STATE_REVERTING, substrate.StatusUnknown},
		{atepb.ActorState_ACTOR_STATE_UNSPECIFIED, substrate.StatusUnknown},
	} {
		f := &fakeControl{actor: &atepb.Actor{
			Metadata: &atepb.ResourceMetadata{Name: "a", Atespace: "s"},
			Status:   &atepb.ActorStatus{State: tc.s},
		}}
		info, err := FromClient(f).GetActor(context.Background(), substrate.ActorRef{Atespace: "s", Name: "a"})
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != tc.want {
			t.Fatalf("state %v -> %v, want %v", tc.s, info.Status, tc.want)
		}
	}
}

// Substrate refuses a resume, suspend or tag while another operation holds the actor's lease with
// ABORTED (another host resuming the actor it just created, say). The adapter reports that as
// substrate.ErrActorBusy, which the session service maps to a retryable ABORTED; any other failure
// keeps its own error.
func TestAbortedResumeAndSuspendAreBusy(t *testing.T) {
	ref := substrate.ActorRef{Atespace: "space", Name: "sess-x"}
	aborted := status.Error(codes.Aborted, "another operation is in progress for this actor")
	ops := map[string]func(*fakeControl) error{
		"resume": func(f *fakeControl) error {
			_, err := FromClient(f).ResumeActor(context.Background(), ref)
			return err
		},
		"suspend": func(f *fakeControl) error {
			_, err := FromClient(f).SuspendActor(context.Background(), ref)
			return err
		},
		"tag": func(f *fakeControl) error {
			return FromClient(f).TagActor(context.Background(), ref, substrate.SnapshotID{Atespace: "space", Name: "fork-c"})
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			f := &fakeControl{actor: runningActor(), resumeErr: aborted, suspendErr: aborted, tagErr: aborted}
			if err := op(f); !errors.Is(err, substrate.ErrActorBusy) || !errors.Is(err, api.ErrComputeBusy) || !strings.Contains(err.Error(), "in progress") {
				t.Fatalf("aborted %s: err=%v, want ErrActorBusy carrying substrate's reason", name, err)
			}
			unavailable := status.Error(codes.Unavailable, "ate-api unavailable")
			f = &fakeControl{actor: runningActor(), resumeErr: unavailable, suspendErr: unavailable, tagErr: unavailable}
			if err := op(f); err == nil || errors.Is(err, substrate.ErrActorBusy) {
				t.Fatalf("unavailable %s: err=%v, want a failure that is not ErrActorBusy", name, err)
			}
		})
	}
}

// Substrate answers ResumeActor with NOT_FOUND for a missing actor and also for one that exists but
// whose template lacks a volume (createActorVolumes in cmd/ateapi/internal/controlapi/volumes.go).
// Over the adapter, a memory-tier Restore and Create report the session's state lost only when
// GetActor confirms the actor is gone (or, for Restore, no longer holds the snapshot); an actor that
// still holds it keeps substrate's resume error.
func TestResumeNotFoundOnAnExistingActorIsNotLostState(t *testing.T) {
	const uri = "gs://b/atespaces/space/actors/u1/snapshots/s1"
	volume := status.Error(codes.NotFound, `volume "scratch" not found in template`)
	suspended := func() *atepb.Actor {
		a := runningActor()
		a.Status.State = atepb.ActorState_ACTOR_STATE_SUSPENDED
		a.Status.WorkerAssignment = nil
		return a
	}
	desc := api.Descriptor{ID: "counter", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}
	backend := func(f *fakeControl) *substrate.Backend {
		return substrate.New(FromClient(f), "space", substrate.ObjectRef{Atespace: "space", Name: "counter"}, desc)
	}
	turns := func() (int, error) { return 1, nil }
	ops := map[string]func(*substrate.Backend) error{
		"restore": func(b *substrate.Backend) error {
			_, err := b.Restore(context.Background(), api.SnapshotRef{Local: "sess-x", ExternalURI: uri, Memory: true})
			return err
		},
		"create": func(b *substrate.Backend) error {
			_, err := b.Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x", StartedTurns: turns})
			return err
		},
	}
	for name, op := range ops {
		t.Run(name+" of an actor that still exists keeps the resume error", func(t *testing.T) {
			f := &fakeControl{actor: suspended(), resumeErr: volume}
			err := op(backend(f))
			if err == nil || errors.Is(err, api.ErrMemoryStateLost) || !strings.Contains(err.Error(), "not found in template") {
				t.Fatalf("err=%v, want substrate's resume error, not lost state", err)
			}
		})
		t.Run(name+" of an actor that is gone is lost state", func(t *testing.T) {
			f := &fakeControl{actor: suspended(), resumeErr: status.Error(codes.NotFound, "Actor space/sess-x not found")}
			if name == "restore" {
				f.getErr = status.Error(codes.NotFound, "Actor space/sess-x not found")
			} else {
				// Create's first GetActor finds it SUSPENDED; it is gone by the second.
				f.onGet = func(n int) {
					if n == 2 {
						f.getErr = status.Error(codes.NotFound, "Actor space/sess-x not found")
					}
				}
			}
			if err := op(backend(f)); !errors.Is(err, api.ErrMemoryStateLost) {
				t.Fatalf("err=%v, want api.ErrMemoryStateLost", err)
			}
		})
	}
}
