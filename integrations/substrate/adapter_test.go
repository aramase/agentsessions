package ateadapter

import (
	"context"
	"reflect"
	"testing"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/runtime/substrate"
	"google.golang.org/grpc"
)

// fakeControl is an ate-api Control double: it records calls and returns canned actors so the
// translation is tested without a real ate-api-server. Only the 5 mapped methods are overridden; the
// rest are promoted from the embedded (nil) interface and never called.
type fakeControl struct {
	atepb.ControlClient
	calls        []string
	lastBoot     bool
	lastSource   *atepb.ActorSnapshotRef
	lastTagScope atepb.ActorSnapshotTagScope
	actor        *atepb.Actor
}

func (f *fakeControl) CreateActor(_ context.Context, in *atepb.CreateActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	a := in.GetActor()
	f.calls = append(f.calls, "create:"+a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName()+":tmpl="+a.GetActorTemplateNamespace()+"/"+a.GetActorTemplateName())
	f.lastSource = in.GetSourceSnapshot()
	return a, nil
}
func (f *fakeControl) ResumeActor(_ context.Context, in *atepb.ResumeActorRequest, _ ...grpc.CallOption) (*atepb.ResumeActorResponse, error) {
	f.calls = append(f.calls, "resume:"+in.GetActor().GetName())
	f.lastBoot = in.GetBoot()
	return &atepb.ResumeActorResponse{Actor: f.actor}, nil
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
	return f.actor, nil
}
func (f *fakeControl) TagActorSnapshot(_ context.Context, in *atepb.TagActorSnapshotRequest, _ ...grpc.CallOption) (*atepb.ActorSnapshotTag, error) {
	f.calls = append(f.calls, "tag:"+in.GetSnapshot().GetSnapshot().GetName()+"->"+in.GetTag().GetMetadata().GetAtespace()+"/"+in.GetTag().GetMetadata().GetName())
	f.lastTagScope = in.GetTag().GetScope()
	return in.GetTag(), nil
}

func runningActor() *atepb.Actor {
	return &atepb.Actor{
		Metadata:   &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status:     atepb.Actor_STATUS_RUNNING,
		AteomPodIp: "10.0.0.5",
	}
}

func TestCreateMapsTemplate(t *testing.T) {
	f := &fakeControl{}
	err := FromClient(f, "").CreateActor(context.Background(),
		substrate.ActorRef{Atespace: "space", Name: "sess-x"},
		substrate.ObjectRef{Namespace: "tmpl", Name: "echo"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"create:space/sess-x:tmpl=tmpl/echo"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
}

func TestResumeBootAndMeshDNS(t *testing.T) {
	f := &fakeControl{actor: runningActor()}
	info, err := FromClient(f, "").ResumeActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !f.lastBoot {
		t.Fatal("boot=true must pass through to ResumeActor{boot}")
	}
	if info.Status != substrate.StatusRunning || info.PodIP != "10.0.0.5" {
		t.Fatalf("unexpected info %+v", info)
	}
	if want := "sess-x.space." + DefaultDNSSuffix; info.MeshDNS != want {
		t.Fatalf("MeshDNS=%q want %q", info.MeshDNS, want)
	}
}

// A suspend yields the durable ActorSnapshot's NAME. Substrate keeps the physical storage location
// private, so latest_snapshot (an atespace-scoped ObjectRef) is the only handle a client can clone from.
func TestSuspendExtractsSnapshotName(t *testing.T) {
	f := &fakeControl{actor: &atepb.Actor{
		Metadata:       &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status:         atepb.Actor_STATUS_SUSPENDED,
		LatestSnapshot: &atepb.ObjectRef{Atespace: "space", Name: "snap-sess-x-1"},
	}}
	got, err := FromClient(f, "").SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "snap-sess-x-1" {
		t.Fatalf("snapshot=%q want %q", got, "snap-sess-x-1")
	}
}

// There is deliberately no fallback to in_progress_snapshot: substrate sets that to a STORAGE URI,
// not a resource name, and only leaves it set when the suspend registered no snapshot. Returning ""
// makes the backend refuse a fork loudly instead of committing an unusable handle to the chain.
func TestSuspendWithoutSnapshotReturnsEmpty(t *testing.T) {
	f := &fakeControl{actor: &atepb.Actor{
		Metadata:           &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status:             atepb.Actor_STATUS_SUSPENDING,
		InProgressSnapshot: "gs://ate-snapshots/space/snapshots/2026-08-05T01-02-03Z-abc",
	}}
	got, err := FromClient(f, "").SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("snapshot=%q want %q (a storage URI is not a cloneable snapshot name)", got, "")
	}
}

// The clone path substrate requires: tag the snapshot, then create the child FROM THE TAG. Substrate
// rejects a canonical snapshot reference here with FailedPrecondition, so asserting the oneof arm is
// the point of this test, not an implementation detail.
func TestCloneTagsSnapshotThenCreatesFromTag(t *testing.T) {
	f := &fakeControl{}
	ad := FromClient(f, "")
	snapshot := substrate.SnapshotID{Atespace: "space", Name: "snap-parent-1"}
	tag := substrate.SnapshotID{Atespace: "space", Name: "fork-child"}

	if err := ad.TagSnapshot(context.Background(), snapshot, tag); err != nil {
		t.Fatal(err)
	}
	if err := ad.CreateActorFromSnapshot(context.Background(),
		substrate.ActorRef{Atespace: "space", Name: "child"},
		substrate.ObjectRef{Namespace: "tmpl", Name: "counter"}, tag); err != nil {
		t.Fatal(err)
	}

	want := []string{"tag:snap-parent-1->space/fork-child", "create:space/child:tmpl=tmpl/counter"}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls=%v want %v", f.calls, want)
	}
	gotTag, ok := f.lastSource.GetReference().(*atepb.ActorSnapshotRef_Tag)
	if !ok {
		t.Fatalf("source snapshot must be referenced BY TAG, got %T", f.lastSource.GetReference())
	}
	if gotTag.Tag.GetAtespace() != "space" || gotTag.Tag.GetName() != "fork-child" {
		t.Fatalf("unexpected source tag %+v", gotTag.Tag)
	}
	// A fork's child is created in its parent's atespace, so the default (unpublished) scope suffices.
	if f.lastTagScope != atepb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE {
		t.Fatalf("tag scope=%v want ATESPACE (no cross-atespace publication)", f.lastTagScope)
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		s    atepb.Actor_Status
		want substrate.ActorStatus
	}{
		{atepb.Actor_STATUS_RUNNING, substrate.StatusRunning},
		{atepb.Actor_STATUS_SUSPENDED, substrate.StatusSuspended},
		{atepb.Actor_STATUS_PAUSED, substrate.StatusSuspended},
		{atepb.Actor_STATUS_CRASHED, substrate.StatusTerminated},
		{atepb.Actor_STATUS_RESUMING, substrate.StatusUnknown},
	} {
		f := &fakeControl{actor: &atepb.Actor{Metadata: &atepb.ResourceMetadata{Name: "a", Atespace: "s"}, Status: tc.s}}
		info, err := FromClient(f, "").GetActor(context.Background(), substrate.ActorRef{Atespace: "s", Name: "a"})
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != tc.want {
			t.Fatalf("status %v -> %v, want %v", tc.s, info.Status, tc.want)
		}
	}
}
