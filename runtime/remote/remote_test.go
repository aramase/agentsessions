package remote_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/counteragent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/sqlitelog"
)

// serveHarness runs a harness on its own listener, the way an operator would run one out of band,
// and returns the address to register it by.
func serveHarness(t *testing.T, h api.Harness) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(h))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// The backend has to satisfy the interface the Placer drives, which is api.Runtime plus Describe.
var _ placement.Backend = (*remote.Backend)(nil)

// The point of the package: a harness this binary never imported into a registry still runs,
// because it was registered by address.
func TestTurnRunsOnAHarnessRegisteredByAddress(t *testing.T) {
	addr := serveHarness(t, echoagent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	placer := placement.New(backend, echoagent.Model)
	log := store.Session("sess-remote")

	if _, err := placer.Exec(ctx, log, "sess-remote", []api.Message{*api.TextMessage("user", "hello")}, 0); err != nil {
		t.Fatalf("exec on remote harness: %v", err)
	}

	records, err := log.Read(1)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	var output string
	for _, r := range records {
		if r.Event.Kind == api.EventOutput {
			output = r.Event.Message.Text()
		}
	}
	if want := "echo:hello"; output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

// Describe must come from the harness rather than from configuration, because that declaration is
// what CanPlace gates on.
func TestDescribeComesFromTheHarness(t *testing.T) {
	addr := serveHarness(t, echoagent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })

	desc, err := backend.Describe(context.Background())
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if desc.ID != "echo" {
		t.Fatalf("descriptor id = %q, want the harness's own id %q", desc.ID, "echo")
	}
	if got, want := desc.Capabilities.Resumability, api.ResumabilityStatelessReplay; got != want {
		t.Fatalf("resumability = %v, want %v", got, want)
	}
}

// A backend that does not own the sandbox cannot capture its memory, so a memory-snapshot harness
// must be refused before any compute is touched rather than silently resumed from a replay.
func TestMemorySnapshotHarnessIsRefused(t *testing.T) {
	addr := serveHarness(t, &counteragent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })

	desc, err := backend.Describe(context.Background())
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got, want := desc.Capabilities.Resumability, api.ResumabilityRequiresMemorySnapshot; got != want {
		t.Fatalf("counter harness resumability = %v, want %v", got, want)
	}
	if controller.CanPlace(desc.Capabilities, backend.Capabilities()) {
		t.Fatal("CanPlace allowed a memory-snapshot harness on a backend that owns no sandbox")
	}
}

func TestCreateRejectsAMissingSessionUID(t *testing.T) {
	backend := remote.New("127.0.0.1:1")
	t.Cleanup(func() { _ = backend.Close() })

	if _, err := backend.Create(context.Background(), &api.SessionSpec{}); err == nil {
		t.Fatal("Create accepted a spec with no session uid")
	}
	if _, err := backend.Create(context.Background(), nil); err == nil {
		t.Fatal("Create accepted a nil spec")
	}
}

// Stop detaches one session; the harness belongs to whoever started it and other sessions are
// still using it.
func TestStopLeavesTheHarnessRunning(t *testing.T) {
	addr := serveHarness(t, echoagent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })

	ctx := context.Background()
	inc, err := backend.Create(ctx, &api.SessionSpec{SessionUID: "sess-a"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := backend.Stop(ctx, inc); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := backend.Describe(ctx); err != nil {
		t.Fatalf("harness unreachable after stopping a session: %v", err)
	}
}

// The four below mirror runtime/local's direct unit tests, so the claim that this backend has the
// same filesystem-only semantics is asserted rather than implied by the end-to-end test.

func TestSnapshotIsExternalOnly(t *testing.T) {
	backend := remote.New("127.0.0.1:1")
	t.Cleanup(func() { _ = backend.Close() })
	ctx := context.Background()
	inc := api.Incarnation{ID: "sess-a"}

	ref, err := backend.Snapshot(ctx, inc, api.SnapshotExternal)
	if err != nil {
		t.Fatalf("external snapshot: %v", err)
	}
	if ref.Local != "sess-a" {
		t.Fatalf("ref.Local = %q, want the session handle %q", ref.Local, "sess-a")
	}
	if ref.Memory {
		t.Fatal("ref.Memory is true; this backend owns no sandbox and cannot capture memory")
	}
	if _, err := backend.Snapshot(ctx, inc, api.SnapshotLocal); err == nil {
		t.Fatal("warm (LOCAL) snapshot was accepted; the journal is the durable state")
	}
}

func TestRestoreReattachesToTheSameAddress(t *testing.T) {
	backend := remote.New("127.0.0.1:9999")
	t.Cleanup(func() { _ = backend.Close() })
	ctx := context.Background()

	inc, err := backend.Restore(ctx, api.SnapshotRef{Local: "sess-a"})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if inc.ID != "sess-a" || inc.Address != "127.0.0.1:9999" || inc.Runtime != "remote" {
		t.Fatalf("restored incarnation = %+v, want the session at the configured address", inc)
	}
	if _, err := backend.Restore(ctx, api.SnapshotRef{}); err == nil {
		t.Fatal("restore accepted a ref with no session handle")
	}
}

func TestForkGivesTheChildItsOwnIncarnation(t *testing.T) {
	backend := remote.New("127.0.0.1:9999")
	t.Cleanup(func() { _ = backend.Close() })
	ctx := context.Background()

	inc, err := backend.Fork(ctx, api.SnapshotRef{Local: "sess-parent"}, api.ForkOpts{ChildSessionUID: "sess-child"})
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if inc.ID != "sess-child" {
		t.Fatalf("forked incarnation id = %q, want the child uid", inc.ID)
	}
	if inc.Address != "127.0.0.1:9999" {
		t.Fatalf("child address = %q, want the same harness", inc.Address)
	}
	if _, err := backend.Fork(ctx, api.SnapshotRef{Local: "sess-parent"}, api.ForkOpts{}); err == nil {
		t.Fatal("fork accepted opts with no child session uid")
	}
}

func TestStatusReportsLive(t *testing.T) {
	backend := remote.New("127.0.0.1:1")
	t.Cleanup(func() { _ = backend.Close() })

	state, err := backend.Status(context.Background(), api.Incarnation{ID: "sess-a"})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if state != api.ComputeLive {
		t.Fatalf("state = %v, want %v", state, api.ComputeLive)
	}
}
