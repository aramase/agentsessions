// Package remote is a Runtime backend for a harness somebody else is already running.
//
// Every other backend provisions the compute it hands back: runtime/local starts the harness in
// process, runtime/substrate creates an actor. That makes a harness a build-time decision, because
// the binary has to import it. This one provisions nothing. It is given an address where a harness
// is already listening and returns an incarnation pointing at it, so a harness can be registered by
// configuration instead of by recompiling the server.
//
// The harness self-describes: Describe dials it and asks, so placement gates on the tier the harness
// actually declares rather than on anything configured here. The answer is live, not pinned: if a
// different harness starts answering at the address, the next Exec, Resume or Fork sees it. Because
// an address can front more than one process (replicas behind a load balancer, or a harness restarted
// between two calls), the backend implements placement.LiveDescriber: the Placer asks again on the
// connection that runs the turn and refuses it there, before journaling, if that harness cannot be
// placed. That check is not bound to the turn's Connect stream, so a harness that takes over the
// address between the two calls is not detected (see placement's recheck). Describe itself is bounded by the Placer, so a harness that accepts the connection and
// never answers is reported as unavailable rather than holding the call.
//
// Capabilities report MemorySnapshot=false, and that is not a limitation to be lifted later. This
// backend does not own the process it points at, so it cannot capture its memory. A harness that
// declares REQUIRES_MEMORY_SNAPSHOT is therefore refused by the Placer's CanPlace gate on Exec,
// Resume and Fork before any compute is touched or anything is journaled, which is the correct
// answer rather than a degraded one: run that harness on a backend that owns the sandbox.
//
// Addresses are host:port (dialed over cleartext h2c) or a unix socket. unix:///abs/path and
// unix://rel/path are dialed the same way placement's default dialer does, so the address that
// passes Describe is the address the Placer runs turns on; any other form, such as unix:path or a
// dns:/// target, is handed to gRPC's resolver unchanged.
package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/observability"
)

var (
	errCreateRequiresSessionSpec = errors.New("remote: Create requires a SessionSpec")
	errCreateRequiresSessionUID  = errors.New("remote: Create requires a session uid")
	errNoAddress                 = errors.New("remote: no harness address configured")
	errClosed                    = errors.New("remote: backend is closed")
)

// Backend attaches sessions to a harness reachable at a fixed address.
type Backend struct {
	addr   string
	logger *slog.Logger

	mu      sync.Mutex
	conn    *grpc.ClientConn
	harness api.Harness
	closed  bool
}

// Option configures a Backend.
type Option func(*Backend)

// WithLogger sets the logger. Without one the backend stays silent.
func WithLogger(l *slog.Logger) Option {
	return func(b *Backend) {
		if l != nil {
			b.logger = l
		}
	}
}

// New returns a Backend that points every session at the harness listening on addr.
func New(addr string, opts ...Option) *Backend {
	b := &Backend{addr: addr, logger: slog.New(slog.DiscardHandler)}
	for _, o := range opts {
		o(b)
	}
	return b
}

// reconnectBackoff bounds how long the Describe connection waits between reconnect attempts. gRPC's
// default grows to 120s, and every Exec, Resume and Fork goes through Describe, so a harness that
// restarted would keep being refused as UNAVAILABLE for up to two minutes after it was healthy. One
// second keeps a restart visible within about a second, and an attempt is a single dial to a fixed
// address, so retrying that often costs nothing worth saving.
var reconnectBackoff = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: backoff.DefaultConfig.Multiplier,
		Jitter:     backoff.DefaultConfig.Jitter,
		MaxDelay:   time.Second,
	},
	// gRPC's default; WithConnectParams would otherwise set it to zero.
	MinConnectTimeout: 20 * time.Second,
}

// connect dials the harness once and reuses the connection. gRPC reconnects underneath, with the
// delay between attempts capped by reconnectBackoff, so a harness that restarts is picked up again
// within about a second without this backend tracking its lifecycle.
//
// This is deliberately a second connection: the Placer dials Incarnation.Address itself to run a
// turn, and that dial belongs to the Placer because it owns the execution stream and its
// interceptors. The two connections can reach different processes behind one address, which is why
// DescribesLiveHarness has the Placer check the turn's own connection too.
func (b *Backend) connect() (api.Harness, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errClosed
	}
	if b.harness != nil {
		return b.harness, nil
	}
	if b.addr == "" {
		return nil, errNoAddress
	}
	target, opts := b.addr, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(reconnectBackoff),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
	}
	// gRPC's resolver reads unix://rel as an authority and rejects it, while the Placer strips the
	// prefix and dials the rest as a path. Dial the same way here so both connections agree on what
	// an address means.
	if sock, ok := strings.CutPrefix(b.addr, "unix://"); ok {
		target = "passthrough:///remote-harness"
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("remote: dial harness at %s: %w", b.addr, err)
	}
	b.conn = conn
	b.harness = harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
	return b.harness, nil
}

// Close releases the connection to the harness. The harness itself is not ours to stop. A closed
// backend stays closed: a later Describe fails rather than opening a connection nobody will close.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.conn == nil {
		return nil
	}
	err := b.conn.Close()
	b.conn, b.harness = nil, nil
	return err
}

// Describe asks the harness what it is, so placement gates on the harness's own declaration rather
// than on a tier assumed by configuration.
func (b *Backend) Describe(ctx context.Context) (desc api.Descriptor, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "describe_harness", "address", b.addr)
	defer func() { finish(err, "error_kind", remoteErrorKind(err), "harness_id", desc.ID) }()

	h, err := b.connect()
	if err != nil {
		return api.Descriptor{}, err
	}
	// Name the address: an unreachable harness otherwise surfaces as a bare gRPC error that does not
	// say which harness failed. %w keeps the gRPC status, which is how placement tells an outage
	// (codes.Unavailable) from a harness that answered with an error.
	desc, err = h.Describe(ctx)
	if err != nil {
		return api.Descriptor{}, fmt.Errorf("remote: describe harness at %s: %w", b.addr, err)
	}
	return desc, nil
}

// DescribesLiveHarness reports that Describe asks whichever harness answers at the address, so the
// Placer re-checks the harness on the connection that runs each turn (placement.LiveDescriber).
func (b *Backend) DescribesLiveHarness() bool { return true }

// Create hands back an incarnation pointing at the configured address. Nothing is provisioned: the
// harness is already running, which is the whole point of this backend.
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (inc api.Incarnation, err error) {
	sessionUID := ""
	if s != nil {
		sessionUID = s.SessionUID
	}
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "create_compute", "session_uid", sessionUID)
	defer func() {
		finish(err, "error_kind", remoteErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if s == nil {
		return api.Incarnation{}, errCreateRequiresSessionSpec
	}
	if s.SessionUID == "" {
		return api.Incarnation{}, errCreateRequiresSessionUID
	}
	if b.addr == "" {
		return api.Incarnation{}, errNoAddress
	}
	return api.Incarnation{ID: s.SessionUID, Address: b.addr, Runtime: "remote"}, nil
}

// Snapshot names the session so Restore can re-attach. Memory=false: this backend does not own the
// harness process and cannot capture its memory.
func (b *Backend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (ref api.SnapshotRef, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "snapshot",
		"incarnation_id", in.ID, "snapshot_kind", kind)
	defer func() { finish(err, "error_kind", remoteErrorKind(err), "memory_snapshot", ref.Memory) }()

	if kind == api.SnapshotLocal {
		return api.SnapshotRef{}, fmt.Errorf("remote: warm (LOCAL) snapshot not supported; the journal is the durable state — use EXTERNAL")
	}
	return api.SnapshotRef{Local: in.ID, Memory: false}, nil
}

// Restore re-attaches to the same harness. Harness-visible state is reconstructed by the host
// replaying the journal, which is what STATELESS_REPLAY means.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "restore_compute", "session_uid", ref.Local)
	defer func() {
		finish(err, "error_kind", remoteErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if ref.Local == "" {
		return api.Incarnation{}, fmt.Errorf("remote: snapshot ref missing session handle")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: ref.Local})
}

// Fork is a replay-fork: the child attaches to the same harness and its copied log prefix is
// replayed into it. There is no clone-from-memory path without owning the sandbox.
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "fork_compute",
		"parent_session_uid", ref.Local, "child_session_uid", opts.ChildSessionUID, "strategy", "replay")
	defer func() {
		finish(err, "error_kind", remoteErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if opts.ChildSessionUID == "" {
		return api.Incarnation{}, fmt.Errorf("remote: fork requires a child session uid")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: opts.ChildSessionUID})
}

// Stop detaches the session. The harness process belongs to whoever started it, so stopping one
// incarnation must not stop the harness; other sessions are using it.
func (b *Backend) Stop(ctx context.Context, in api.Incarnation) error {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "stop_compute", "incarnation_id", in.ID)
	finish(nil)
	return nil
}

// Status reports the compute-lifecycle state. The log and the placement layer are the lifecycle
// authority, so a known incarnation is LIVE.
func (b *Backend) Status(ctx context.Context, in api.Incarnation) (api.ComputeState, error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.remote", "get_compute_status", "incarnation_id", in.ID)
	finish(nil, "compute_state", api.ComputeLive)
	return api.ComputeLive, nil
}

// Capabilities reports a backend that owns no sandbox: no memory snapshot, CoW fork, attestation,
// or GPU state. See the package comment for why MemorySnapshot=false is structural here.
func (b *Backend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: false, CoWFork: false, Attest: false, GPUState: false}
}

func remoteErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errNoAddress):
		return "no_address"
	case errors.Is(err, errClosed):
		return "closed"
	case errors.Is(err, errCreateRequiresSessionSpec), errors.Is(err, errCreateRequiresSessionUID):
		return "invalid_request"
	default:
		return "remote_error"
	}
}
