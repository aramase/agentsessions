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
	"google.golang.org/grpc/metadata"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
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
// actor's in-flight requests before it checkpoints, so checkpointing under that stream stalls until
// the drain times out. Suspend does not interrupt the turn: it is refused with ErrSessionBusy and
// leaves the turn, its stream, compute and the journal alone. Once the turn has returned its stream
// is closed, so the retried Suspend never checkpoints under it.
func TestSuspendRefusesASessionWithAnOpenHarnessStream(t *testing.T) {
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
		return api.ModelResponse{}, context.Cause(ctx)
	}
	streamEnded := make(chan bool, 1)
	backend := &routedBackend{
		address: hs.addr,
		desc:    memDescriptor,
		onSnapshot: func() {
			select {
			case <-hs.finished:
				streamEnded <- true
			case <-time.After(5 * time.Second):
				streamEnded <- false
			}
		},
	}
	p := placement.New(backend, model)

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
	<-hs.started
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := p.Suspend(context.Background(), log, "s1"); !errors.Is(err, placement.ErrSessionBusy) {
		t.Fatalf("Suspend with a running turn returned %v, want ErrSessionBusy", err)
	}
	backend.mu.Lock()
	snapshots := backend.snapshots
	backend.mu.Unlock()
	if snapshots != 0 {
		t.Fatalf("refused Suspend took %d snapshot(s)", snapshots)
	}
	if after, err := log.Head(); err != nil || after != head {
		t.Fatalf("refused Suspend moved the journal head from %d to %d (err %v)", head, after, err)
	}
	select {
	case err := <-execErr:
		t.Fatalf("refused Suspend ended the running turn: %v", err)
	default:
	}
	if n := len(hs.finished); n != 0 {
		t.Fatalf("refused Suspend closed %d harness stream(s)", n)
	}

	cancel()
	select {
	case err := <-execErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled turn returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled turn never returned")
	}
	if _, err := p.Suspend(context.Background(), log, "s1"); err != nil {
		t.Fatalf("Suspend after the turn returned: %v", err)
	}
	if !<-streamEnded {
		t.Fatal("the harness's Connect stream was still open when the runtime checkpointed")
	}
}

// A stateful fork checkpoints its parent the same way, so it must end the parent's stream too.
func TestForkEndsTheParentsOpenHarnessStreamBeforeTheCheckpoint(t *testing.T) {
	testCheckpointEndsOpenStream(t, forkCheckpoint)
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
// substrate that stream would stall the very checkpoint that is running. Suspend refuses it through
// the session guard it holds; a stateful fork, which the guard does not cover, through its
// checkpoint mark.
func TestSuspendRefusesATurnStartedDuringTheCheckpoint(t *testing.T) {
	testCheckpointRefusesANewTurn(t, suspendCheckpoint, placement.ErrSessionBusy)
}

func TestForkRefusesATurnOnTheParentDuringTheCheckpoint(t *testing.T) {
	testCheckpointRefusesANewTurn(t, forkCheckpoint, placement.ErrCheckpointing)
}

func testCheckpointRefusesANewTurn(t *testing.T, checkpoint func(*placement.Placer, *sqlitelog.Log, string) error, want error) {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s1")

	var p *placement.Placer
	var execErr error
	backend := &routedBackend{address: hs.addr, desc: memDescriptor}
	backend.onSnapshot = func() {
		_, execErr = p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "hi")}, 0)
	}
	p = placement.New(backend, echoagent.Model)
	if err := checkpoint(p, log, "s1"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !errors.Is(execErr, want) {
		t.Fatalf("turn started during the checkpoint returned %v, want %v", execErr, want)
	}
	if n := len(hs.started); n != 0 {
		t.Fatalf("%d harness stream(s) opened while the session was being checkpointed", n)
	}
}

// The other interleaving: the turn passed every early check and is about to register its connection
// when the fork's checkpoint begins. Registration must still be refused. (Suspend cannot begin while
// a turn is past its early checks: the turn holds the session guard.)
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
// stream stalls. So a stateful fork that reaches its checkpoint supersedes the parent's in-flight
// turn even when the checkpoint then fails. The contract that makes that safe: nothing is recorded
// for the failed attempt, the parent stays live, and the caller can run the next turn or retry the
// fork at the new head.
func TestFailedForkCheckpointSupersedesTheParentTurnAndARetryRecovers(t *testing.T) {
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

	if err := forkCheckpoint(p, log, "s1"); err == nil {
		t.Fatal("Fork reported success for a failed checkpoint")
	}
	select {
	case err := <-execErr:
		if !errors.Is(err, placement.ErrCheckpointing) || !errors.Is(err, eventlog.ErrFenced) {
			t.Fatalf("turn returned %v, want it superseded (ErrCheckpointing, ErrFenced)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight turn survived a failed fork checkpoint")
	}
	kinds := journalKinds(t, log)
	if slices.Contains(kinds, api.EventError) || slices.Contains(kinds, api.EventLifecycle) || slices.Contains(kinds, api.EventEnd) {
		t.Fatalf("failed fork left journal %v, want only the incomplete execution", kinds)
	}

	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), log, "s1", []api.Message{*api.TextMessage("user", "again")}, head); err != nil {
		t.Fatalf("next turn after a failed fork: %v", err)
	}
	if kinds := journalKinds(t, log); kinds[len(kinds)-1] != api.EventEnd {
		t.Fatalf("next turn after a failed fork did not complete: %v", kinds)
	}
	if err := forkCheckpoint(p, log, "s1"); err != nil {
		t.Fatalf("retried fork: %v", err)
	}
	if kinds := journalKinds(t, log); kinds[len(kinds)-1] != api.EventLifecycle {
		t.Fatalf("retried fork did not record the parent checkpoint's SUSPEND: %v", kinds)
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

// A turn that registered before a stateful fork's checkpoint began but mints its fence after the
// checkpoint's is not fenced by it. The checkpoint still ends that turn's stream, so the turn must not
// be allowed to write under its newer fence: it would journal the host's interruption as a harness
// ERROR. The fork must not abort because the turn advanced the parent's head under that fence either.
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
// fence still mints one, and if that lands after the fence a stateful fork records the parent's
// checkpoint under, the record fails after the parent is already cold. The fork must wait for the
// turn to mint.
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

// The wait is bounded by the caller's context. When it ends first the fork aborts before it
// snapshots the parent, and nothing is recorded, so the parent stays live.
func TestForkAbortsBeforeSnapshotWhenAnEndedParentTurnDoesNotMintInTime(t *testing.T) {
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
	child, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	err = p.Fork(ctx, log, "s1", []placement.ForkChild{{UID: "s1-child", Log: child.Session("s1-child")}}, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fork returned %v, want context.DeadlineExceeded", err)
	}
	<-turnDone
	backend.mu.Lock()
	snapshots := backend.snapshots
	backend.mu.Unlock()
	if snapshots != 0 {
		t.Fatalf("Fork took %d snapshot(s) after giving up on the parent's turn", snapshots)
	}
	if kinds := journalKinds(t, log); len(kinds) != 0 {
		t.Fatalf("journal %v, want nothing recorded for the aborted fork", kinds)
	}
}
