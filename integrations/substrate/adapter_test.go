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
	calls    []string
	lastBoot bool
	actor    *atepb.Actor
}

func (f *fakeControl) CreateActor(_ context.Context, in *atepb.CreateActorRequest, _ ...grpc.CallOption) (*atepb.Actor, error) {
	a := in.GetActor()
	f.calls = append(f.calls, "create:"+a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName()+":tmpl="+a.GetActorTemplateNamespace()+"/"+a.GetActorTemplateName())
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

func TestSuspendExtractsSnapshotURI(t *testing.T) {
	f := &fakeControl{actor: &atepb.Actor{
		Metadata: &atepb.ResourceMetadata{Atespace: "space", Name: "sess-x"},
		Status:   atepb.Actor_STATUS_SUSPENDED,
		LatestSnapshotInfo: &atepb.SnapshotInfo{
			Data: &atepb.SnapshotInfo_External{External: &atepb.ExternalSnapshotInfo{SnapshotUriPrefix: "s3://snap/sess-x"}},
		},
	}}
	uri, err := FromClient(f, "").SuspendActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if uri != "s3://snap/sess-x" {
		t.Fatalf("snapshot uri=%q", uri)
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
