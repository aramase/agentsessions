package remote_test

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
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
// must be refused by the Placer before any compute is touched rather than silently resumed from a
// replay. Driven through placer.Exec so it fails if the Placer ever stops asking the backend.
func TestMemorySnapshotHarnessIsRefused(t *testing.T) {
	addr := serveHarness(t, &counteragent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-counter")

	placer := placement.New(backend, echoagent.Model)
	_, err := placer.Exec(context.Background(), log, "sess-counter", []api.Message{*api.TextMessage("user", "hello")}, 0)
	if !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("exec on a REQUIRES_MEMORY_SNAPSHOT harness: got %v, want ErrUnplaceable", err)
	}
	assertEmptyJournal(t, log)

	err = placer.Fork(context.Background(), log, "sess-counter", []placement.ForkChild{{UID: "child", Log: newJournal(t).Session("child")}}, 0)
	if !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("fork of a REQUIRES_MEMORY_SNAPSHOT harness: got %v, want ErrUnplaceable", err)
	}
	assertEmptyJournal(t, log)
}

// The descriptor is live: whatever harness answers at the address now is the one gated. A
// REQUIRES_MEMORY_SNAPSHOT harness that starts answering after a turn was interrupted must not have
// that turn replayed into it on Resume.
func TestResumeRefusesAMemorySnapshotHarnessSwappedIn(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	first := grpc.NewServer()
	v1.RegisterHarnessServer(first, harnesswire.NewServer(failOnceHarness{failed: &atomic.Bool{}}))
	go func() { _ = first.Serve(lis) }()

	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-swap")
	placer := placement.New(backend, echoagent.Model)
	ctx := context.Background()

	if _, err := placer.Exec(ctx, log, "sess-swap", []api.Message{*api.TextMessage("user", "hello")}, 0); err == nil {
		t.Fatal("the first turn should have been interrupted")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	// Replace the harness at the same address with one this backend cannot host.
	first.Stop()
	lis, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen on %s: %v", addr, err)
	}
	serveOn(t, lis, &counteragent.Harness{})
	waitForHarness(t, backend, "counter")

	if err := placer.Resume(ctx, log, "sess-swap"); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("resume into a swapped-in REQUIRES_MEMORY_SNAPSHOT harness: got %v, want ErrUnplaceable", err)
	}
	if got, _ := log.Head(); got != head {
		t.Fatalf("a refused resume wrote to the log: head %d -> %d", head, got)
	}
}

// An unreachable harness is an outage, not a host fault: the Placer reports ErrHarnessUnavailable,
// the error names the address, and nothing is journaled.
func TestUnreachableHarnessIsUnavailable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // nothing listens here now

	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-dead")

	_, err = placement.New(backend, echoagent.Model).Exec(context.Background(), log, "sess-dead", []api.Message{*api.TextMessage("user", "hello")}, 0)
	if !errors.Is(err, placement.ErrHarnessUnavailable) {
		t.Fatalf("exec against an unreachable harness: got %v, want ErrHarnessUnavailable", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("error %q does not name the harness address %s", err, addr)
	}
	assertEmptyJournal(t, log)
}

// silentHarness accepts connections and never says anything, like a peer that is not a gRPC server
// or has wedged. It returns the address.
func silentHarness(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() }) // hold it open, say nothing
		}
	}()
	return lis.Addr().String()
}

// When the caller's own deadline runs out while a silent harness is being described, the caller's
// deadline is the error, not an outage: resending the same call would run out the same way. Nothing
// is journaled.
func TestSilentHarnessRunsOutTheCallersDeadline(t *testing.T) {
	addr := silentHarness(t)
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-silent")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := placement.New(backend, echoagent.Model).Exec(ctx, log, "sess-silent", []api.Message{*api.TextMessage("user", "hello")}, 0)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, placement.ErrHarnessUnavailable) {
		t.Fatalf("exec against a harness that never answers within the caller's deadline: got %v, want context.DeadlineExceeded and not ErrHarnessUnavailable", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("error %q does not name the harness address %s", err, addr)
	}
	assertEmptyJournal(t, log)
}

// While the caller's deadline is still live, a harness that accepts the connection but never
// completes the gRPC handshake is an outage: the Placer stops waiting for Describe after its own
// 10s bound, before gRPC's 20s connect timeout, and reports ErrHarnessUnavailable, which is
// retryable.
func TestSilentHarnessIsUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("takes about 10s: waits out the Placer's describe bound")
	}
	t.Parallel()
	addr := silentHarness(t)
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-silent")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := placement.New(backend, echoagent.Model).Exec(ctx, log, "sess-silent", []api.Message{*api.TextMessage("user", "hello")}, 0)
	if !errors.Is(err, placement.ErrHarnessUnavailable) {
		t.Fatalf("exec against a harness that never answers: got %v, want ErrHarnessUnavailable", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("the caller's deadline ran out first (%v); the describe bound should have ended the call", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("error %q does not name the harness address %s", err, addr)
	}
	assertEmptyJournal(t, log)
}

// A harness that comes back after an outage must be usable again within seconds. Describe gates
// every Exec, Resume and Fork, so a connection left in gRPC's default reconnect backoff (which grows
// to 120s) would keep refusing turns as UNAVAILABLE long after the harness is healthy.
func TestDescribeRecoversQuicklyAfterAnOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("takes about 12s: holds the harness down long enough for default backoff to grow")
	}
	t.Parallel() // overlaps the 10s silent-harness test instead of adding to it
	g := newGate(t)
	serveOn(t, g, echoagent.Harness{})
	g.up.Store(true)

	backend := remote.New(g.Addr().String())
	t.Cleanup(func() { _ = backend.Close() })
	ctx := context.Background()
	if _, err := backend.Describe(ctx); err != nil {
		t.Fatalf("describe: %v", err)
	}

	// Take the harness down and keep asking, the way a client retrying on UNAVAILABLE would. Each
	// reconnect attempt fails, growing the backoff.
	g.up.Store(false)
	g.drop()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		if _, err := backend.Describe(ctx); err == nil {
			t.Fatal("describe succeeded while the harness was down")
		}
	}

	// Bring the harness back right after a reconnect attempt has failed, so the client's next
	// attempt is a whole backoff interval away: about a second with the cap, several seconds without.
	g.drainAttempts()
	select {
	case <-g.attempts:
	case <-time.After(30 * time.Second):
		t.Fatal("client stopped trying to reconnect")
	}
	g.up.Store(true)
	start := time.Now()
	for {
		_, err := backend.Describe(ctx)
		if err == nil {
			break
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("harness back but describe still failing after %v: %v", time.Since(start), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// gate is a harness address that is up, handing connections to whatever serves on it, or down,
// accepting each connection and closing it at once. Down, every client reconnect attempt is
// reported on attempts, so a test knows exactly when the client last tried.
type gate struct {
	lis      net.Listener
	up       atomic.Bool
	conns    chan net.Conn
	attempts chan struct{}
	done     chan struct{}
	closed   sync.Once
	mu       sync.Mutex
	live     []net.Conn
}

func newGate(t *testing.T) *gate {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &gate{lis: lis, conns: make(chan net.Conn), attempts: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			if !g.up.Load() {
				_ = c.Close()
				select {
				case g.attempts <- struct{}{}:
				default:
				}
				continue
			}
			g.mu.Lock()
			g.live = append(g.live, c)
			g.mu.Unlock()
			select {
			case g.conns <- c:
			case <-g.done:
				_ = c.Close()
				return
			}
		}
	}()
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// drop closes every connection handed out while up, as a crashed harness would.
func (g *gate) drop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.live {
		_ = c.Close()
	}
	g.live = nil
}

func (g *gate) drainAttempts() {
	for {
		select {
		case <-g.attempts:
		default:
			return
		}
	}
}

func (g *gate) Accept() (net.Conn, error) {
	select {
	case c := <-g.conns:
		return c, nil
	case <-g.done:
		return nil, net.ErrClosed
	}
}

func (g *gate) Close() error {
	g.closed.Do(func() { close(g.done); _ = g.lis.Close(); g.drop() })
	return nil
}

func (g *gate) Addr() net.Addr { return g.lis.Addr() }

// Close is final: a later Describe must fail instead of opening a connection nobody will close.
func TestDescribeAfterCloseFails(t *testing.T) {
	addr := serveHarness(t, echoagent.Harness{})
	backend := remote.New(addr)
	if _, err := backend.Describe(context.Background()); err != nil {
		t.Fatalf("describe: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := backend.Describe(context.Background()); err == nil {
		t.Fatal("describe after close succeeded, so it re-dialed")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// unix://relative must mean the same thing to Describe as it does to the Placer's dialer, which
// strips the prefix and dials the rest as a path. gRPC's own resolver rejects it as an authority.
func TestRelativeUnixSocketAddress(t *testing.T) {
	dir, err := os.MkdirTemp("", "remote")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Chdir(dir) // keeps the socket path short and makes the relative form meaningful

	lis, err := net.Listen("unix", "h.sock")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveOn(t, lis, echoagent.Harness{})

	backend := remote.New("unix://h.sock")
	t.Cleanup(func() { _ = backend.Close() })
	log := newJournal(t).Session("sess-unix")
	if _, err := placement.New(backend, echoagent.Model).Exec(context.Background(), log, "sess-unix", []api.Message{*api.TextMessage("user", "hello")}, 0); err != nil {
		t.Fatalf("exec on unix://h.sock: %v", err)
	}
	if got, want := outputOf(t, log), "echo:hello"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

// Suspend, Resume and Fork through the Placer: the session goes cold with a filesystem-only ref,
// comes back by replay against the same harness, keeps executing, and forks a child that runs on
// the same address.
func TestLifecycleThroughThePlacer(t *testing.T) {
	addr := serveHarness(t, echoagent.Harness{})
	backend := remote.New(addr)
	t.Cleanup(func() { _ = backend.Close() })
	store := newJournal(t)
	log := store.Session("sess-life")
	placer := placement.New(backend, echoagent.Model)
	ctx := context.Background()

	if _, err := placer.Exec(ctx, log, "sess-life", []api.Message{*api.TextMessage("user", "one")}, 0); err != nil {
		t.Fatalf("exec: %v", err)
	}
	ref, err := placer.Suspend(ctx, log, "sess-life")
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if ref.Memory || ref.Local != "sess-life" {
		t.Fatalf("suspend ref = %+v, want a filesystem-only ref naming the session", ref)
	}
	if err := placer.Resume(ctx, log, "sess-life"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placer.Exec(ctx, log, "sess-life", []api.Message{*api.TextMessage("user", "two")}, head); err != nil {
		t.Fatalf("exec after resume: %v", err)
	}
	if got, want := outputOf(t, log), "echo:two"; got != want {
		t.Fatalf("output after resume = %q, want %q", got, want)
	}

	head, err = log.Head()
	if err != nil {
		t.Fatal(err)
	}
	child := store.Session("sess-child")
	if err := placer.Fork(ctx, log, "sess-life", []placement.ForkChild{{UID: "sess-child", Log: child}}, head); err != nil {
		t.Fatalf("fork: %v", err)
	}
	childHead, err := child.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placer.Exec(ctx, child, "sess-child", []api.Message{*api.TextMessage("user", "three")}, childHead); err != nil {
		t.Fatalf("exec on fork child: %v", err)
	}
	if got, want := outputOf(t, child), "echo:three"; got != want {
		t.Fatalf("child output = %q, want %q", got, want)
	}
	for _, l := range []eventlog.Store{log, child} {
		if err := l.Verify(); err != nil {
			t.Fatalf("verify: %v", err)
		}
	}
}

// failOnceHarness is a STATELESS_REPLAY echo whose first Run fails, leaving an interrupted turn
// (no END) on the log for Resume to re-drive.
type failOnceHarness struct {
	echoagent.Harness
	failed *atomic.Bool
}

func (h failOnceHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if h.failed.CompareAndSwap(false, true) {
		return errors.New("harness went away mid-turn")
	}
	return h.Harness.Run(ctx, s, sink)
}

func serveOn(t *testing.T, lis net.Listener, h api.Harness) {
	t.Helper()
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(h))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
}

// waitForHarness polls until the harness with the given id answers. gRPC reconnects with backoff
// after the previous server went away, so the first few Describes can still fail.
func waitForHarness(t *testing.T, b *remote.Backend, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		desc, err := b.Describe(context.Background())
		if err == nil && desc.ID == id {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("harness %q never answered: last descriptor %q, err %v", id, desc.ID, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newJournal(t *testing.T) *sqlitelog.Store {
	t.Helper()
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func assertEmptyJournal(t *testing.T, log eventlog.Store) {
	t.Helper()
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != 0 {
		t.Fatalf("a refused placement wrote to the log: head=%d", head)
	}
}

// outputOf returns the last OUTPUT text on the log.
func outputOf(t *testing.T, log eventlog.Store) string {
	t.Helper()
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
	return output
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

// The Placer must re-check the harness on the connection that runs the turn.
var _ placement.LiveDescriber = (*remote.Backend)(nil)

// connQueue is a net.Listener fed connections by hand, so a test can decide which server gets each
// connection accepted on a shared address.
type connQueue struct {
	conns  chan net.Conn
	done   chan struct{}
	once   sync.Once
	parent net.Addr
}

func newConnQueue(parent net.Addr) *connQueue {
	return &connQueue{conns: make(chan net.Conn), done: make(chan struct{}), parent: parent}
}

func (q *connQueue) Accept() (net.Conn, error) {
	select {
	case c := <-q.conns:
		return c, nil
	case <-q.done:
		return nil, net.ErrClosed
	}
}

func (q *connQueue) Close() error {
	q.once.Do(func() { close(q.done) })
	return nil
}

func (q *connQueue) Addr() net.Addr { return q.parent }

// replicas serves two harnesses behind one address, handing out accepted connections to them in
// turn, the way a load balancer that picks a backend per connection (a Kubernetes Service) spreads
// clients over replicas. It returns the shared address.
func replicas(t *testing.T, first, second api.Harness) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	queues := []*connQueue{newConnQueue(lis.Addr()), newConnQueue(lis.Addr())}
	serveOn(t, queues[0], first)
	serveOn(t, queues[1], second)
	go func() {
		for i := 0; ; i++ {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			select {
			case queues[i%2].conns <- c:
			case <-queues[i%2].done:
				_ = c.Close()
				return
			}
		}
	}()
	return lis.Addr().String()
}

// Two replicas behind one registered address can declare different tiers, for instance during a
// rolling change. The backend's Describe connection reaching a STATELESS_REPLAY replica must not let
// a turn run on a REQUIRES_MEMORY_SNAPSHOT replica: the Placer asks again on the turn's own
// connection and refuses before anything is journaled.
func TestMixedReplicasBehindOneAddressAreGatedPerConnection(t *testing.T) {
	t.Run("same tier", func(t *testing.T) {
		backend := remote.New(replicas(t, echoagent.Harness{}, echoagent.Harness{}))
		t.Cleanup(func() { _ = backend.Close() })
		log := newJournal(t).Session("sess-same")
		if _, err := placement.New(backend, echoagent.Model).Exec(context.Background(), log, "sess-same", []api.Message{*api.TextMessage("user", "hello")}, 0); err != nil {
			t.Fatalf("exec across two stateless replicas: %v", err)
		}
		if got, want := outputOf(t, log), "echo:hello"; got != want {
			t.Fatalf("output = %q, want %q", got, want)
		}
	})
	t.Run("memory replica runs the turn", func(t *testing.T) {
		backend := remote.New(replicas(t, echoagent.Harness{}, &counteragent.Harness{}))
		t.Cleanup(func() { _ = backend.Close() })
		desc, err := backend.Describe(context.Background())
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		if desc.Capabilities.Resumability != api.ResumabilityStatelessReplay {
			t.Fatalf("the backend's own connection should reach the stateless replica, got %v", desc.Capabilities.Resumability)
		}
		log := newJournal(t).Session("sess-mixed")
		_, err = placement.New(backend, echoagent.Model).Exec(context.Background(), log, "sess-mixed", []api.Message{*api.TextMessage("user", "hello")}, 0)
		if !errors.Is(err, placement.ErrUnplaceable) {
			t.Fatalf("exec whose turn connection reaches a REQUIRES_MEMORY_SNAPSHOT replica: got %v, want ErrUnplaceable", err)
		}
		assertEmptyJournal(t, log)
	})
}
