package substrate_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/runtime/substrate"
)

// mockControl records the ordered control-plane calls and returns canned actor state, so tests can
// assert the Runtime→substrate mapping without a real ate-api-server.
type mockControl struct {
	calls  []string
	status substrate.ActorStatus
	info   substrate.ActorInfo
}

func (m *mockControl) CreateActor(ctx context.Context, a substrate.ActorRef, _ substrate.ObjectRef) error {
	m.calls = append(m.calls, "create:"+a.Name)
	return nil
}
func (m *mockControl) ResumeActor(ctx context.Context, a substrate.ActorRef, boot bool) (substrate.ActorInfo, error) {
	m.calls = append(m.calls, fmt.Sprintf("resume:%s:boot=%v", a.Name, boot))
	return m.info, nil
}
func (m *mockControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (string, error) {
	m.calls = append(m.calls, "suspend:"+a.Name)
	return "gcs://snap/" + a.Name, nil
}
func (m *mockControl) DeleteActor(ctx context.Context, a substrate.ActorRef) error {
	m.calls = append(m.calls, "delete:"+a.Name)
	return nil
}
func (m *mockControl) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{Status: m.status, PodIP: m.info.PodIP, MeshDNS: m.info.MeshDNS}, nil
}

func newBackend(m *mockControl) *substrate.Backend {
	return substrate.New(m, "space", substrate.ObjectRef{Namespace: "tmpl", Name: "echo"},
		api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}})
}

func TestCreateBootsActor(t *testing.T) {
	m := &mockControl{info: substrate.ActorInfo{PodIP: "10.0.0.5", MeshDNS: "sess-x.space.actors"}}
	in, err := newBackend(m).Create(context.Background(), &api.SessionSpec{SessionUID: "sess-x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"create:sess-x", "resume:sess-x:boot=true"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("create calls=%v want %v", m.calls, want)
	}
	if in.Worker != "10.0.0.5" || in.Address != "sess-x.space.actors" || in.Runtime != "substrate" {
		t.Fatalf("unexpected incarnation %+v", in)
	}
}

func TestSuspendRestoreRoundtrip(t *testing.T) {
	m := &mockControl{}
	b := newBackend(m)
	ref, err := b.Snapshot(context.Background(), api.Incarnation{ID: "sess-x"}, api.SnapshotExternal)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Memory || ref.Local != "sess-x" || ref.ExternalURI != "gcs://snap/sess-x" {
		t.Fatalf("unexpected snapshot ref %+v", ref)
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

func TestForkIsReplayFork(t *testing.T) {
	m := &mockControl{}
	if _, err := newBackend(m).Fork(context.Background(), api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"}); err != nil {
		t.Fatal(err)
	}
	// A replay-fork creates a FRESH cold actor for the child (no clone-from-snapshot call).
	if want := []string{"create:child", "resume:child:boot=true"}; !reflect.DeepEqual(m.calls, want) {
		t.Fatalf("fork calls=%v want a fresh cold child actor %v", m.calls, want)
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
