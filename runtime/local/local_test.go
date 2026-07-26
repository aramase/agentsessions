package local_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/runtime/local"
)

// stubHarness is a placeholder api.Harness the backend serves.
type stubHarness struct{}

func (stubHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "echo"}, nil
}
func (stubHarness) Run(context.Context, *api.Start, api.EventSink) error { return nil }

// newBackend builds a backend and closes its harness server when the test ends.
func newBackend(t *testing.T) *local.Backend {
	t.Helper()
	b := local.New(stubHarness{})
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestCreateInProcess(t *testing.T) {
	b := newBackend(t)
	in, err := b.Create(context.Background(), &api.SessionSpec{SessionUID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if in.ID != "s1" || in.Runtime != "local" || !strings.HasPrefix(in.Address, "unix://") || in.Worker == "" {
		t.Fatalf("unexpected incarnation %+v", in)
	}
	if d, err := b.Describe(context.Background()); err != nil || d.ID == "" {
		t.Fatalf("backend must expose the harness descriptor for the placement gate: %v %+v", err, d)
	}
}

func TestCreateRequiresUID(t *testing.T) {
	if _, err := newBackend(t).Create(context.Background(), &api.SessionSpec{}); err == nil {
		t.Fatal("create must require a session uid")
	}
}

func TestSnapshotFilesystemOnly(t *testing.T) {
	ref, err := newBackend(t).Snapshot(context.Background(), api.Incarnation{ID: "s1"}, api.SnapshotExternal)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Memory {
		t.Fatal("local snapshot must be filesystem-only (Memory=false)")
	}
	if ref.Local != "s1" {
		t.Fatalf("snapshot ref must carry the session handle for Restore, got %+v", ref)
	}
}

func TestSnapshotLocalUnsupported(t *testing.T) {
	if _, err := newBackend(t).Snapshot(context.Background(), api.Incarnation{ID: "s"}, api.SnapshotLocal); err == nil {
		t.Fatal("warm (LOCAL) snapshot must be unsupported on the filesystem-only local backend")
	}
}

func TestRestoreReprovisions(t *testing.T) {
	in, err := newBackend(t).Restore(context.Background(), api.SnapshotRef{Local: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if in.ID != "s1" || in.Runtime != "local" {
		t.Fatalf("restore must re-provision the incarnation, got %+v", in)
	}
}

func TestRestoreRequiresHandle(t *testing.T) {
	if _, err := newBackend(t).Restore(context.Background(), api.SnapshotRef{}); err == nil {
		t.Fatal("restore must require a session handle in the ref")
	}
}

func TestForkIsReplayFork(t *testing.T) {
	in, err := newBackend(t).Fork(context.Background(), api.SnapshotRef{Local: "parent"}, api.ForkOpts{ChildSessionUID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if in.ID != "child" || in.Runtime != "local" {
		t.Fatalf("fork must provision a fresh child incarnation, got %+v", in)
	}
}

func TestStopAndStatus(t *testing.T) {
	b := newBackend(t)
	if err := b.Stop(context.Background(), api.Incarnation{ID: "s1"}); err != nil {
		t.Fatal(err)
	}
	st, err := b.Status(context.Background(), api.Incarnation{ID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if st != api.ComputeLive {
		t.Fatalf("status=%v want ComputeLive", st)
	}
}

func TestCapabilitiesFilesystemOnly(t *testing.T) {
	caps := newBackend(t).Capabilities()
	if caps.MemorySnapshot || caps.CoWFork || caps.Attest || caps.GPUState {
		t.Fatalf("local must be filesystem-only, got %+v", caps)
	}
}

// TestCanPlaceRefusal is the honest-degradation counterpart at the unit level: the filesystem-only
// local backend must REFUSE a REQUIRES_MEMORY_SNAPSHOT harness (which substrate accepts) and accept a
// STATELESS_REPLAY one. This is the pod side of the two-backend neutrality proof.
func TestCanPlaceRefusal(t *testing.T) {
	caps := newBackend(t).Capabilities()
	mem := api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}
	if controller.CanPlace(mem, caps) {
		t.Fatal("local (MemorySnapshot=false) must refuse a REQUIRES_MEMORY_SNAPSHOT harness")
	}
	stateless := api.Capabilities{Resumability: api.ResumabilityStatelessReplay}
	if !controller.CanPlace(stateless, caps) {
		t.Fatal("local must accept a STATELESS_REPLAY harness")
	}
}
