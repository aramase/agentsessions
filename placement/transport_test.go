package placement_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/session"
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
	// snapshotErr, when set, fails the next Snapshot (after onSnapshot) and is then cleared.
	snapshotErr error
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
	failWith := b.snapshotErr
	b.snapshotErr = nil
	b.mu.Unlock()
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	if failWith != nil {
		return api.SnapshotRef{}, failWith
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

	mu      sync.Mutex
	targets []string
	// unaryTargets is the ate-target-actor metadata of each unary call (Describe), in order.
	unaryTargets [][]string
	started      chan struct{}
	finished     chan struct{}
}

func startHarnessServer(t *testing.T, h api.Harness) *harnessServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &harnessServer{addr: lis.Addr().String(), started: make(chan struct{}, 8), finished: make(chan struct{}, 8)}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		hs.mu.Lock()
		hs.unaryTargets = append(hs.unaryTargets, md.Get("ate-target-actor"))
		hs.mu.Unlock()
		return handler(ctx, req)
	}), grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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

func (hs *harnessServer) seenUnaryTargets() [][]string {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return slices.Clone(hs.unaryTargets)
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

// The incarnation, not the caller, decides which sandbox a call reaches. Whatever the caller already
// put on the context for the same key, in any letter case and with any number of values, every call
// must carry exactly one ate-target-actor: the incarnation's. Two values would let a router that
// reads the last one route to an actor an upstream check that read the first one never approved.
func TestDefaultDialCallMetadataOverridesTheCaller(t *testing.T) {
	const mine, other = "space/mine", "space/someone-else"
	cases := []struct {
		name   string
		caller func(context.Context) context.Context
	}{
		{"appended value", func(ctx context.Context) context.Context {
			return metadata.AppendToOutgoingContext(ctx, "ate-target-actor", other)
		}},
		{"both values, other last", func(ctx context.Context) context.Context {
			return metadata.AppendToOutgoingContext(ctx, "ate-target-actor", mine, "ate-target-actor", other)
		}},
		{"mixed-case key", func(ctx context.Context) context.Context {
			return metadata.NewOutgoingContext(ctx, metadata.MD{"Ate-Target-Actor": {other}})
		}},
		{"mixed-case key and appended value", func(ctx context.Context) context.Context {
			ctx = metadata.NewOutgoingContext(ctx, metadata.MD{"ATE-TARGET-ACTOR": {other}})
			return metadata.AppendToOutgoingContext(ctx, "Ate-Target-Actor", other)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hs := startHarnessServer(t, echoagent.Harness{})
			har, closeHarness, err := placement.DefaultDial(api.Incarnation{
				Address:      hs.addr,
				CallMetadata: map[string]string{"ate-target-actor": mine},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeHarness() }()
			ctx := tc.caller(context.Background())

			if _, err := har.Describe(ctx); err != nil {
				t.Fatal(err)
			}
			start := &api.Start{ExecutionID: "e1", Inputs: []api.Message{*api.TextMessage("user", "hi")}}
			if err := har.Run(ctx, start, echoSink{}); err != nil {
				t.Fatal(err)
			}
			if got := hs.seenUnaryTargets(); len(got) != 1 || !slices.Equal(got[0], []string{mine}) {
				t.Errorf("Describe carried ate-target-actor=%v, want only the incarnation's [[%s]]", got, mine)
			}
			if got := hs.seenTargets(); !slices.Equal(got, []string{mine}) {
				t.Errorf("Connect stream carried ate-target-actor=%v, want only the incarnation's [%s]", got, mine)
			}
		})
	}
}

// A caller of the Sessions API cannot pick the actor either: an ate-target-actor header on its Exec
// call must not reach the harness call, which carries only the session's own actor.
func TestExecCallerHeaderDoesNotReachTheHarnessCall(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(&routedBackend{address: hs.addr, desc: statelessEcho}, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(store, registry))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := client.Dial(lis.Addr().String(), client.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx := metadata.AppendToOutgoingContext(t.Context(), "ate-target-actor", "space/victim")
	turn, err := c.Exec(ctx, client.ExecOptions{Inputs: []string{"hi"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"space/" + turn.Session.GetMetadata().GetUid()}
	if got := hs.seenTargets(); !slices.Equal(got, want) {
		t.Fatalf("Connect stream carried ate-target-actor=%v, want only the session's %v", got, want)
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
	testCheckpointEndsOpenStream(t, suspendCheckpoint)
}

// A stateful fork checkpoints its parent the same way, so it must end the parent's stream too.
func TestForkEndsTheParentsOpenHarnessStreamBeforeTheCheckpoint(t *testing.T) {
	testCheckpointEndsOpenStream(t, forkCheckpoint)
}

// session.Service routes a turn by ExecRequest.harness but a Suspend or Fork by the session's
// recorded harness, so the turn and the checkpoint can run on different Placers of one Registry.
// The checkpoint must still end the turn's stream.
func TestSuspendEndsAStreamOpenedByAnotherPlacerOfTheRegistry(t *testing.T) {
	testCheckpointEndsOpenStream(t, suspendCheckpoint, registryPair)
}

func TestForkEndsAParentStreamOpenedByAnotherPlacerOfTheRegistry(t *testing.T) {
	testCheckpointEndsOpenStream(t, forkCheckpoint, registryPair)
}

// placerPair returns the Placer a checkpoint runs on and the one a turn runs on.
type placerPair func(t *testing.T, b placement.Backend, model func(context.Context, api.ModelRequest) (api.ModelResponse, error)) (checkpoint, turn *placement.Placer)

// samePlacer runs the turn and the checkpoint on one Placer.
func samePlacer(_ *testing.T, b placement.Backend, model func(context.Context, api.ModelRequest) (api.ModelResponse, error)) (*placement.Placer, *placement.Placer) {
	p := placement.New(b, model)
	return p, p
}

// registryPair runs them on two Placers of one Registry: the session's default harness and an
// override, as ExecRequest.harness selects.
func registryPair(t *testing.T, b placement.Backend, model func(context.Context, api.ModelRequest) (api.ModelResponse, error)) (*placement.Placer, *placement.Placer) {
	t.Helper()
	def, other := placement.New(b, model), placement.New(b, model)
	if _, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": def, "other": other}); err != nil {
		t.Fatal(err)
	}
	return def, other
}

func testCheckpointEndsOpenStream(t *testing.T, checkpoint func(*placement.Placer, *sqlitelog.Log, string) error, pair ...placerPair) {
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
		return api.ModelResponse{}, ctx.Err() // what a real model client returns: not the cause
	}
	streamEnded := make(chan bool, 1)
	backend := &routedBackend{
		address: hs.addr,
		desc:    memDescriptor,
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
	mk := placerPair(samePlacer)
	if len(pair) > 0 {
		mk = pair[0]
	}
	p, turn := mk(t, backend, model)

	execErr := make(chan error, 1)
	go func() {
		_, err := turn.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
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
		// The checkpoint fenced the turn before ending its stream, so the turn reports that, not
		// the incidental cancellation, and the service maps it to a retryable Aborted.
		if !errors.Is(err, placement.ErrCheckpointing) || !errors.Is(err, eventlog.ErrFenced) {
			t.Fatalf("interrupted turn returned %v, want ErrCheckpointing and eventlog.ErrFenced", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the interrupted turn never returned")
	}
	// The host interrupted the turn; the fence keeps that out of the journal as a harness ERROR.
	kinds := journalKinds(t, log)
	if slices.Contains(kinds, api.EventError) {
		t.Fatalf("journal %v records the checkpoint's interruption as a harness ERROR", kinds)
	}
	if kinds[len(kinds)-1] != api.EventLifecycle {
		t.Fatalf("journal %v does not end in the checkpoint's SUSPEND", kinds)
	}
}

func journalKinds(t *testing.T, log eventlog.Store) []api.EventKind {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]api.EventKind, 0, len(recs))
	for _, r := range recs {
		kinds = append(kinds, r.Event.Kind)
	}
	return kinds
}

// memDescriptor is a REQUIRES_MEMORY_SNAPSHOT harness, the tier whose fork checkpoints the parent.
var memDescriptor = api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}

func suspendCheckpoint(p *placement.Placer, log *sqlitelog.Log, uid string) error {
	_, err := p.Suspend(context.Background(), log, uid)
	return err
}

func forkCheckpoint(p *placement.Placer, log *sqlitelog.Log, uid string) error {
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
}

// A turn that starts while the session is being checkpointed must not open a harness stream: on
// substrate that stream would stall the very checkpoint that is running.
func TestSuspendRefusesATurnStartedDuringTheCheckpoint(t *testing.T) {
	testCheckpointRefusesANewTurn(t, suspendCheckpoint)
}

func TestForkRefusesATurnOnTheParentDuringTheCheckpoint(t *testing.T) {
	testCheckpointRefusesANewTurn(t, forkCheckpoint)
}

// The refusal is per session, not per Placer: a turn routed to another harness of the same
// Registry must not open a stream under the checkpoint either.
func TestSuspendRefusesATurnOnAnotherPlacerOfTheRegistry(t *testing.T) {
	testCheckpointRefusesANewTurn(t, suspendCheckpoint, registryPair)
}

func TestForkRefusesAParentTurnOnAnotherPlacerOfTheRegistry(t *testing.T) {
	testCheckpointRefusesANewTurn(t, forkCheckpoint, registryPair)
}

func testCheckpointRefusesANewTurn(t *testing.T, checkpoint func(*placement.Placer, *sqlitelog.Log, string) error, pair ...placerPair) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	var turn *placement.Placer
	var execErr error
	backend := &routedBackend{address: hs.addr, desc: memDescriptor}
	backend.onSnapshot = func() {
		_, execErr = turn.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
	}
	mk := placerPair(samePlacer)
	if len(pair) > 0 {
		mk = pair[0]
	}
	var p *placement.Placer
	p, turn = mk(t, backend, echoagent.Model)
	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !errors.Is(execErr, placement.ErrCheckpointing) {
		t.Fatalf("turn started during the checkpoint returned %v, want ErrCheckpointing", execErr)
	}
	if n := len(hs.started); n != 0 {
		t.Fatalf("%d harness stream(s) opened while the session was being checkpointed", n)
	}
}

// NewRegistry replaces each Placer's session set. Replacing it under a running turn would orphan
// the turn's connection, so a checkpoint through the registry would neither end nor refuse it.
// NewRegistry must reject a Placer with a turn in progress and accept it again once the turn ends.
func TestNewRegistryRejectsAPlacerWithATurnInProgress(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	modelCalled := make(chan struct{})
	model := func(ctx context.Context, _ api.ModelRequest) (api.ModelResponse, error) {
		close(modelCalled)
		<-ctx.Done()
		return api.ModelResponse{}, ctx.Err()
	}
	p := placement.New(&routedBackend{address: hs.addr, desc: memDescriptor}, model)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	execErr := make(chan error, 1)
	go func() {
		_, err := p.Exec(ctx, log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
		execErr <- err
	}()
	select {
	case <-modelCalled:
	case err := <-execErr:
		t.Fatalf("turn returned before parking on the model call: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("turn never reached the model call")
	}

	if _, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": p}); err == nil {
		t.Fatal("NewRegistry accepted a placer with a turn in progress")
	}
	cancel()
	select {
	case <-execErr:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled turn never returned")
	}
	if _, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": p}); err != nil {
		t.Fatalf("NewRegistry rejected a placer whose only turn has ended: %v", err)
	}
}

// A checkpoint in progress holds a mark in the set that refuses new turns; NewRegistry must not
// drop it either.
func TestNewRegistryRejectsAPlacerWithACheckpointInProgress(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	backend := &routedBackend{desc: memDescriptor}
	p := placement.New(backend, echoagent.Model)
	var regErr error
	backend.onSnapshot = func() {
		_, regErr = placement.NewRegistry("echo", map[string]*placement.Placer{"echo": p})
	}
	if _, err := p.Suspend(context.Background(), log, "s1"); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if regErr == nil {
		t.Fatal("NewRegistry accepted a placer with a checkpoint in progress")
	}
}

// The other interleaving: the turn passed every early check and is about to register its connection
// when the checkpoint begins. Registration must still be refused.
func TestSuspendRefusesATurnRegisteringAsTheCheckpointBegins(t *testing.T) {
	testCheckpointRefusesARegisteringTurn(t, suspendCheckpoint)
}

func TestForkRefusesAParentTurnRegisteringAsTheCheckpointBegins(t *testing.T) {
	testCheckpointRefusesARegisteringTurn(t, forkCheckpoint)
}

func testCheckpointRefusesARegisteringTurn(t *testing.T, checkpoint func(*placement.Placer, *sqlitelog.Log, string) error) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	dialing, release := make(chan struct{}), make(chan struct{})
	dial := func(inc api.Incarnation) (api.Harness, func() error, error) {
		close(dialing)
		<-release
		return placement.DefaultDial(inc)
	}
	execErr := make(chan error, 1)
	backend := &routedBackend{address: hs.addr, desc: memDescriptor}
	p := placement.New(backend, echoagent.Model, placement.WithDialer(dial))
	var gotErr error
	backend.onSnapshot = func() {
		// The checkpoint has begun; now let the held turn try to register, and wait for it.
		close(release)
		select {
		case gotErr = <-execErr:
		case <-time.After(10 * time.Second):
			gotErr = errors.New("held turn did not return while the checkpoint ran")
		}
	}
	go func() {
		_, err := p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
		execErr <- err
	}()
	<-dialing
	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !errors.Is(gotErr, placement.ErrCheckpointing) {
		t.Fatalf("turn registering as the checkpoint began returned %v, want ErrCheckpointing", gotErr)
	}
	if n := len(hs.started); n != 0 {
		t.Fatalf("%d harness stream(s) opened while the session was being checkpointed", n)
	}
}

// Ending the streams cannot wait for the checkpoint to succeed: the checkpoint is what an open
// stream stalls. So an attempted Suspend supersedes the in-flight turn even when the checkpoint then
// fails. The contract that makes that safe: nothing is recorded for the failed attempt, the session
// stays live, and the caller can run the next turn or retry the Suspend.
func TestFailedSuspendSupersedesTheTurnAndARetryRecovers(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	// The first model call parks until its turn is ended; later calls (the re-drive) answer.
	modelCalled := make(chan struct{})
	var calls int
	var callsMu sync.Mutex
	model := func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		callsMu.Lock()
		calls++
		first := calls == 1
		callsMu.Unlock()
		if !first {
			return echoagent.Model(ctx, req)
		}
		close(modelCalled)
		<-ctx.Done()
		return api.ModelResponse{}, ctx.Err() // what a real model client returns: not the cause
	}
	backend := &routedBackend{address: hs.addr, desc: memDescriptor, snapshotErr: errors.New("checkpoint failed")}
	p := placement.New(backend, model)

	execErr := make(chan error, 1)
	go func() {
		_, err := p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
		execErr <- err
	}()
	<-modelCalled

	if _, err := p.Suspend(context.Background(), log, "s1"); err == nil {
		t.Fatal("Suspend reported success for a failed checkpoint")
	}
	select {
	case err := <-execErr:
		if !errors.Is(err, placement.ErrCheckpointing) || !errors.Is(err, eventlog.ErrFenced) {
			t.Fatalf("turn returned %v, want it superseded (ErrCheckpointing, ErrFenced)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight turn survived a failed Suspend")
	}
	kinds := journalKinds(t, log)
	if slices.Contains(kinds, api.EventError) || slices.Contains(kinds, api.EventLifecycle) || slices.Contains(kinds, api.EventEnd) {
		t.Fatalf("failed Suspend left journal %v, want only the incomplete execution", kinds)
	}

	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "again")}, head); err != nil {
		t.Fatalf("next turn after a failed Suspend: %v", err)
	}
	if kinds := journalKinds(t, log); kinds[len(kinds)-1] != api.EventEnd {
		t.Fatalf("next turn after a failed Suspend did not complete: %v", kinds)
	}
	if _, err := p.Suspend(context.Background(), log, "s1"); err != nil {
		t.Fatalf("retried Suspend: %v", err)
	}
	if kinds := journalKinds(t, log); kinds[len(kinds)-1] != api.EventLifecycle {
		t.Fatalf("retried Suspend did not record SUSPEND: %v", kinds)
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

// gatedStore holds the turn's fence mint until the checkpoint has minted its own, then holds the
// checkpoint until the turn has either reached the model or returned. That forces the one ordering
// the checkpoint's own fence does not cover: a turn already registered when the checkpoint began
// mints a NEWER fence just before the checkpoint ends its stream.
type gatedStore struct {
	eventlog.Store

	mu              sync.Mutex
	fences          int
	turnAtFence     chan struct{} // closed when the turn asks for its fence
	checkpointFence chan struct{} // closed once the checkpoint's fence is minted
	turnMoved       <-chan struct{}
	turnDone        <-chan struct{}
}

func (g *gatedStore) NewFence() (int64, error) {
	g.mu.Lock()
	g.fences++
	n := g.fences
	g.mu.Unlock()
	switch n {
	case 1: // the turn's
		close(g.turnAtFence)
		<-g.checkpointFence
		return g.Store.NewFence()
	case 2: // the checkpoint's, minted before it ends the turn's stream
		f, err := g.Store.NewFence()
		close(g.checkpointFence)
		select {
		case <-g.turnMoved:
		case <-g.turnDone:
		case <-time.After(10 * time.Second):
		}
		return f, err
	default:
		return g.Store.NewFence()
	}
}

// A turn that registered before the checkpoint began but mints its fence after the checkpoint's is
// not fenced by it. The checkpoint still ends that turn's stream, so the turn must not be allowed to
// write under its newer fence: it would journal the host's interruption as a harness ERROR.
func TestSuspendSupersedesATurnThatMintsANewerFence(t *testing.T) {
	testCheckpointSupersedesNewerFence(t, func(p *placement.Placer, log eventlog.Store, uid string) error {
		_, err := p.Suspend(context.Background(), log, uid)
		return err
	})
}

// A stateful fork must do the same, and must not abort because the turn advanced the parent's head
// under the newer fence.
func TestForkSupersedesAParentTurnThatMintsANewerFence(t *testing.T) {
	testCheckpointSupersedesNewerFence(t, func(p *placement.Placer, log eventlog.Store, uid string) error {
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

func testCheckpointSupersedesNewerFence(t *testing.T, checkpoint func(*placement.Placer, eventlog.Store, string) error) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	modelCalled, turnDone := make(chan struct{}), make(chan struct{})
	model := func(ctx context.Context, _ api.ModelRequest) (api.ModelResponse, error) {
		close(modelCalled)
		<-ctx.Done()
		return api.ModelResponse{}, ctx.Err()
	}
	log := &gatedStore{
		Store:           store.Session("s1"),
		turnAtFence:     make(chan struct{}),
		checkpointFence: make(chan struct{}),
		turnMoved:       modelCalled,
		turnDone:        turnDone,
	}
	p := placement.New(&routedBackend{address: hs.addr, desc: memDescriptor}, model)

	var execErr error
	go func() {
		defer close(turnDone)
		_, execErr = p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
	}()
	select {
	case <-log.turnAtFence: // registered, so the checkpoint will see and end its stream
	case <-turnDone:
		t.Fatalf("turn returned before minting its fence: %v", execErr)
	}
	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	select {
	case <-turnDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the superseded turn never returned")
	}
	if !errors.Is(execErr, placement.ErrCheckpointing) || !errors.Is(execErr, eventlog.ErrFenced) {
		t.Fatalf("superseded turn returned %v, want ErrCheckpointing and eventlog.ErrFenced", execErr)
	}
	kinds := journalKinds(t, log)
	if slices.Contains(kinds, api.EventError) {
		t.Fatalf("journal %v records the checkpoint's interruption as a harness ERROR", kinds)
	}
	if kinds[len(kinds)-1] != api.EventLifecycle {
		t.Fatalf("journal %v does not end in the checkpoint's SUSPEND", kinds)
	}
}

// lateFenceStore holds the turn's fence mint (call 1) past the checkpoint's first fence (call 2).
// The turn is let go either when the checkpoint asks for its next fence, the one its record is
// written under, or after lateFenceDelay, whichever is first. When the next fence comes first, it is
// minted BEFORE the turn's, which is the ordering that stales the checkpoint's record. A checkpoint
// that waits for its ended turns to mint never asks for that fence while the turn is held, so only
// the delay can let the turn go and the record fence is minted last.
type lateFenceStore struct {
	eventlog.Store

	mu          sync.Mutex
	fences      int
	turnAtFence chan struct{} // closed when the turn asks for its fence
	release     chan struct{} // closed to let the turn mint
	releaseOnce sync.Once
	turnMinted  chan struct{} // closed once the turn's fence is minted
}

const lateFenceDelay = 300 * time.Millisecond

func (g *lateFenceStore) letTurnMint() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *lateFenceStore) NewFence() (int64, error) {
	g.mu.Lock()
	g.fences++
	n := g.fences
	g.mu.Unlock()
	switch n {
	case 1: // the turn's
		close(g.turnAtFence)
		<-g.release
		defer close(g.turnMinted)
		return g.Store.NewFence()
	case 2: // the checkpoint's first, minted before it ends the turn's stream
		time.AfterFunc(lateFenceDelay, g.letTurnMint)
		return g.Store.NewFence()
	default: // the fence the checkpoint's record is written under
		f, err := g.Store.NewFence()
		g.letTurnMint()
		select {
		case <-g.turnMinted:
		case <-time.After(10 * time.Second):
		}
		return f, err
	}
}

// Ending a turn's stream only cancels its context. A turn that registered but had not yet minted its
// fence still mints one, and if that lands after the fence the checkpoint records under, the record
// fails after the compute is already checkpointed. The checkpoint must wait for the turn to mint.
func TestSuspendWaitsForAnEndedTurnToMintBeforeRecording(t *testing.T) {
	testCheckpointWaitsForLateFence(t, func(p *placement.Placer, log eventlog.Store, uid string) error {
		_, err := p.Suspend(context.Background(), log, uid)
		return err
	})
}

func TestForkWaitsForAnEndedParentTurnToMintBeforeRecording(t *testing.T) {
	testCheckpointWaitsForLateFence(t, func(p *placement.Placer, log eventlog.Store, uid string) error {
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

func testCheckpointWaitsForLateFence(t *testing.T, checkpoint func(*placement.Placer, eventlog.Store, string) error) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	log := &lateFenceStore{
		Store:       store.Session("s1"),
		turnAtFence: make(chan struct{}),
		release:     make(chan struct{}),
		turnMinted:  make(chan struct{}),
	}
	backend := &routedBackend{address: hs.addr, desc: memDescriptor}
	p := placement.New(backend, echoagent.Model)

	turnDone := make(chan struct{})
	var execErr error
	go func() {
		defer close(turnDone)
		_, execErr = p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
	}()
	select {
	case <-log.turnAtFence:
	case <-turnDone:
		t.Fatalf("turn returned before minting its fence: %v", execErr)
	}
	var turnMintedBeforeSnapshot bool
	backend.onSnapshot = func() {
		select {
		case <-log.turnMinted:
			turnMintedBeforeSnapshot = true
		default:
		}
	}
	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	<-turnDone
	if !turnMintedBeforeSnapshot {
		t.Fatal("the checkpoint snapshotted while the superseded turn could still mint a fence")
	}
	if !errors.Is(execErr, placement.ErrCheckpointing) || !errors.Is(execErr, eventlog.ErrFenced) {
		t.Fatalf("superseded turn returned %v, want ErrCheckpointing and eventlog.ErrFenced", execErr)
	}
	kinds := journalKinds(t, log)
	if len(kinds) != 1 || kinds[0] != api.EventLifecycle {
		t.Fatalf("journal %v, want only the checkpoint's SUSPEND", kinds)
	}
}

// The wait is bounded by the caller's context. When it ends first the checkpoint aborts before it
// snapshots, and nothing is recorded, so the session stays live.
func TestSuspendAbortsBeforeSnapshotWhenAnEndedTurnDoesNotMintInTime(t *testing.T) {
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := &lateFenceStore{
		Store:       store.Session("s1"),
		turnAtFence: make(chan struct{}),
		release:     make(chan struct{}),
		turnMinted:  make(chan struct{}),
	}
	backend := &routedBackend{address: hs.addr, desc: memDescriptor}
	p := placement.New(backend, echoagent.Model)

	turnDone := make(chan struct{})
	go func() {
		defer close(turnDone)
		_, _ = p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
	}()
	<-log.turnAtFence
	ctx, cancel := context.WithTimeout(context.Background(), lateFenceDelay/6)
	defer cancel()
	if _, err := p.Suspend(ctx, log, "s1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Suspend returned %v, want context.DeadlineExceeded", err)
	}
	<-turnDone
	backend.mu.Lock()
	snapshots := backend.snapshots
	backend.mu.Unlock()
	if snapshots != 0 {
		t.Fatalf("Suspend took %d snapshot(s) after giving up on the turn", snapshots)
	}
	if kinds := journalKinds(t, log); len(kinds) != 0 {
		t.Fatalf("journal %v, want nothing recorded for the aborted Suspend", kinds)
	}
}
