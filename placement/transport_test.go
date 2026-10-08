package placement_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// routedBackend is a placement.Backend whose incarnations sit behind a shared ingress, the shape
// substrate's atenet-router gives them: one address for every session, and call metadata that names
// the session's sandbox.
type routedBackend struct {
	address string
	desc    api.Descriptor

	mu        sync.Mutex
	snapshots int
	// onSnapshot runs inside Snapshot, standing in for the runtime's checkpoint.
	onSnapshot func()
}

func (b *routedBackend) Describe(context.Context) (api.Descriptor, error) { return b.desc, nil }

func (b *routedBackend) Create(_ context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	return api.Incarnation{
		ID:           s.SessionUID,
		Address:      b.address,
		CallMetadata: map[string]string{"ate-target-actor": "space/" + s.SessionUID},
		Runtime:      "routed",
	}, nil
}

func (b *routedBackend) Snapshot(_ context.Context, in api.Incarnation, _ api.SnapshotKind) (api.SnapshotRef, error) {
	b.mu.Lock()
	b.snapshots++
	b.mu.Unlock()
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	return api.SnapshotRef{Local: in.ID, ExternalURI: "snap-" + in.ID, Memory: true}, nil
}

func (b *routedBackend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	return b.Create(ctx, &api.SessionSpec{SessionUID: ref.Local})
}

func (b *routedBackend) Fork(ctx context.Context, _ api.SnapshotRef, opts api.ForkOpts) (api.Incarnation, error) {
	return b.Create(ctx, &api.SessionSpec{SessionUID: opts.ChildSessionUID})
}

func (b *routedBackend) Stop(context.Context, api.Incarnation) error { return nil }

func (b *routedBackend) Status(context.Context, api.Incarnation) (api.ComputeState, error) {
	return api.ComputeLive, nil
}

func (b *routedBackend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: true}
}

// harnessServer serves a harness over harnesswire on loopback TCP and records, per Connect stream, the
// metadata the stream arrived with and when its handler returned.
type harnessServer struct {
	addr string

	mu       sync.Mutex
	targets  []string
	started  chan struct{}
	finished chan struct{}
}

func startHarnessServer(t *testing.T, h api.Harness) *harnessServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &harnessServer{addr: lis.Addr().String(), started: make(chan struct{}, 8), finished: make(chan struct{}, 8)}
	srv := grpc.NewServer(grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		hs.mu.Lock()
		hs.targets = append(hs.targets, md.Get("ate-target-actor")...)
		hs.mu.Unlock()
		hs.started <- struct{}{}
		defer func() { hs.finished <- struct{}{} }()
		return handler(srv, ss)
	}))
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(h))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return hs
}

func (hs *harnessServer) seenTargets() []string {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return append([]string(nil), hs.targets...)
}

var statelessEcho = api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}

// A routed incarnation shares its address with every other session, so the metadata is the only
// thing that picks the sandbox. The default dialer must put it on the harness stream itself, not
// just on the connection's first call.
func TestDefaultDialAttachesCallMetadataToTheHarnessStream(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	p := placement.New(&routedBackend{address: hs.addr, desc: statelessEcho}, echoagent.Model)
	if _, err := p.Exec(context.Background(), store.Session("s1"), "s1", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatalf("exec through the routed address: %v", err)
	}
	if got := hs.seenTargets(); len(got) != 1 || got[0] != "space/s1" {
		t.Fatalf("Connect stream carried ate-target-actor=%v, want [space/s1]", got)
	}
}

// The incarnation, not the caller, decides which sandbox a call reaches: a value the caller already
// put on the context for the same key must not survive.
func TestDefaultDialCallMetadataOverridesTheCaller(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	har, closeHarness, err := placement.DefaultDial(api.Incarnation{
		Address:      hs.addr,
		CallMetadata: map[string]string{"ate-target-actor": "space/mine"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeHarness() }()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "ate-target-actor", "space/someone-else")
	start := &api.Start{ExecutionID: "e1", Inputs: []api.Message{*api.TextMessage("user", "hi")}}
	if err := har.Run(ctx, start, echoSink{}); err != nil {
		t.Fatal(err)
	}
	if got := hs.seenTargets(); len(got) != 1 || got[0] != "space/mine" {
		t.Fatalf("Connect stream carried ate-target-actor=%v, want only the incarnation's [space/mine]", got)
	}
}

// echoSink answers model calls with the echo model and accepts everything else.
type echoSink struct{}

func (echoSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	return echoagent.Model(ctx, req)
}
func (echoSink) Output(context.Context, string) error { return nil }
func (echoSink) ToolCall(context.Context, api.ToolCall) (api.ToolResult, error) {
	return api.ToolResult{}, errors.New("no tools")
}
func (echoSink) Report(context.Context, api.ToolResult) error { return nil }
func (echoSink) Usage(context.Context, api.Usage) error       { return nil }

// A turn parked on a slow model call leaves its Connect stream open and idle. Substrate drains an
// actor's in-flight requests before it checkpoints, so suspending under that stream stalls until the
// drain times out. Suspend must end the stream first: by the time the runtime checkpoints, the
// harness side of the stream must already have returned.
func TestSuspendEndsAnOpenHarnessStreamBeforeTheCheckpoint(t *testing.T) {
	testCheckpointEndsOpenStream(t, func(p *placement.Placer, log *sqlitelog.Log, uid string) error {
		_, err := p.Suspend(context.Background(), log, uid)
		return err
	})
}

// A stateful fork checkpoints its parent the same way, so it must end the parent's stream too.
func TestForkEndsTheParentsOpenHarnessStreamBeforeTheCheckpoint(t *testing.T) {
	testCheckpointEndsOpenStream(t, func(p *placement.Placer, log *sqlitelog.Log, uid string) error {
		head, err := log.Head()
		if err != nil {
			return err
		}
		store, err := sqlitelog.Open(":memory:")
		if err != nil {
			return err
		}
		defer store.Close()
		return p.Fork(context.Background(), log, uid, []placement.ForkChild{{UID: uid + "-child", Log: store.Session(uid + "-child")}}, head)
	})
}

func testCheckpointEndsOpenStream(t *testing.T, checkpoint func(*placement.Placer, *sqlitelog.Log, string) error) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	// The model call never answers on its own: the stream stays open and idle until something ends it.
	modelCalled := make(chan struct{})
	model := func(ctx context.Context, _ api.ModelRequest) (api.ModelResponse, error) {
		close(modelCalled)
		<-ctx.Done()
		return api.ModelResponse{}, context.Cause(ctx)
	}
	streamEnded := make(chan bool, 1)
	backend := &routedBackend{
		address: hs.addr,
		desc:    api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}},
		onSnapshot: func() {
			// Give the server a bounded moment to observe the reset, as substrate's drain would.
			select {
			case <-hs.finished:
				streamEnded <- true
			case <-time.After(5 * time.Second):
				streamEnded <- false
			}
		},
	}
	p := placement.New(backend, model)

	execErr := make(chan error, 1)
	go func() {
		_, err := p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
		execErr <- err
	}()
	select {
	case <-modelCalled:
	case err := <-execErr:
		t.Fatalf("turn returned before parking on the model call: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("turn never reached the model call")
	}
	<-hs.started

	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !<-streamEnded {
		t.Fatal("the harness's Connect stream was still open when the runtime checkpointed")
	}
	select {
	case err := <-execErr:
		if err == nil {
			t.Fatal("a turn whose harness was ended for a checkpoint must not report success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the interrupted turn never returned")
	}
}

// With no turn in flight, a checkpoint has nothing to end and must not fail for it.
func TestSuspendWithNoOpenStreamStillCheckpoints(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	backend := &routedBackend{address: "127.0.0.1:1", desc: statelessEcho}
	if _, err := placement.New(backend, echoagent.Model).Suspend(context.Background(), store.Session("idle"), "idle"); err != nil {
		t.Fatal(err)
	}
	if backend.snapshots != 1 {
		t.Fatalf("snapshots=%d want 1", backend.snapshots)
	}
}
