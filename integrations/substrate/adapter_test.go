package ateadapter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/runtime/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
}

func (f *fakeControl) CreateActor(_ context.Context, in *atepb.CreateActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	a := in.GetActor()
	f.calls = append(f.calls, "create:"+a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName()+
		":tmpl="+a.GetActorTemplate().GetAtespace()+"/"+a.GetActorTemplate().GetName())
	f.lastCreate = a
	return a, nil
}
func (f *fakeControl) ResumeActor(_ context.Context, in *atepb.ResumeActorRequest, _ ...grpc.CallOption) (*atepb.ResumeActorResponse, error) {
	f.calls = append(f.calls, "resume:"+in.GetActor().GetAtespace()+"/"+in.GetActor().GetName())
	return &atepb.ResumeActorResponse{Actor: f.actor, Resumed: true}, nil
}
func (f *fakeControl) SuspendActor(_ context.Context, in *atepb.SuspendActorRequest, _ ...grpc.CallOption) (*atepb.SuspendActorResponse, error) {
	f.calls = append(f.calls, "suspend:"+in.GetActor().GetName())
	return &atepb.SuspendActorResponse{Actor: f.actor}, nil
}
func (f *fakeControl) DeleteActor(_ context.Context, in *atepb.DeleteActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	f.calls = append(f.calls, "delete:"+in.GetActor().GetName())
	return &atepb.Actor{}, nil
}
func (f *fakeControl) GetActor(_ context.Context, in *atepb.GetActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	f.calls = append(f.calls, "get:"+in.GetActor().GetName())
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
	return tg, nil
}
func (f *fakeControl) DeleteTag(_ context.Context, in *atepb.DeleteTagRequest, _ ...grpc.CallOption) (*atepb.Tag, error) {
	f.calls = append(f.calls, "untag:"+in.GetTag().GetAtespace()+"/"+in.GetTag().GetName())
	return &atepb.Tag{}, nil
}

func runningActor() *atepb.Actor {
	return &atepb.Actor{
		Metadata: &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status: &atepb.ActorStatus{
			State: atepb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &atepb.WorkerAssignment{
				WorkerPod:    "echo-harness-7d9f-abcde",
				WorkerPodIps: []string{"10.0.0.5"},
			},
			ExternalSnapshot: &atepb.ExternalSnapshot{SnapshotUri: "gs://b/atespaces/space/actors/u1/snapshots/s1"},
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
	}
	if info != want {
		t.Fatalf("info=%+v want %+v", info, want)
	}
	if want := []string{"resume:space/sess-x"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
}

// The pinned pre-WorkerAssignment client decoded a current Actor without error but read the status
// message's bytes as a pod IP and the state as UNSPECIFIED. Decode real wire bytes, not a struct
// literal, so a field renumbering upstream fails here instead of silently on a cluster.
func TestGetActorDecodesCurrentWireFormat(t *testing.T) {
	wire, err := proto.Marshal(runningActor())
	if err != nil {
		t.Fatal(err)
	}
	var decoded atepb.Actor
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	f := &fakeControl{actor: &decoded}
	info, err := FromClient(f).GetActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != substrate.StatusRunning || info.Worker != "echo-harness-7d9f-abcde" {
		t.Fatalf("decoded info=%+v, want a running actor on its worker", info)
	}
}

func TestGetActorReportsNotFoundAsSentinel(t *testing.T) {
	f := &fakeControl{getErr: status.Error(codes.NotFound, "actor not found")}
	_, err := FromClient(f).GetActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "gone"})
	if !errors.Is(err, substrate.ErrActorNotFound) {
		t.Fatalf("err=%v want ErrActorNotFound", err)
	}
}

// A suspend yields the URI of the external snapshot the actor now holds, the handle the backend
// records and later compares against GetActor to detect a superseded snapshot.
func TestSuspendReturnsTheHeldSnapshot(t *testing.T) {
	a := runningActor()
	a.Status.State = atepb.ActorState_ACTOR_STATE_SUSPENDED
	a.Status.WorkerAssignment = nil
	f := &fakeControl{actor: a}
	got, err := FromClient(f).SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "gs://b/atespaces/space/actors/u1/snapshots/s1"; got != want {
		t.Fatalf("snapshot=%q want %q", got, want)
	}
}

// The in-progress URI names a snapshot still being written, so it is never reported as the held one.
func TestSuspendWithoutSnapshotReturnsEmpty(t *testing.T) {
	f := &fakeControl{actor: &atepb.Actor{
		Metadata: &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status: &atepb.ActorStatus{
			State:                 atepb.ActorState_ACTOR_STATE_SUSPENDING,
			InProgressSnapshotUri: "gs://b/atespaces/space/actors/u1/snapshots/s2",
		},
	}}
	got, err := FromClient(f).SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("snapshot=%q want empty: an in-progress snapshot is not one the actor holds", got)
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
		{atepb.ActorState_ACTOR_STATE_RESUMING, substrate.StatusUnknown},
		{atepb.ActorState_ACTOR_STATE_SUSPENDING, substrate.StatusUnknown},
		{atepb.ActorState_ACTOR_STATE_REVERTING, substrate.StatusUnknown},
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
