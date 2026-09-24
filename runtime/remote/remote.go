// Package remote is a Runtime backend for a harness somebody else is already running.
//
// Every other backend provisions the compute it hands back: runtime/local starts the harness in
// process, runtime/substrate creates an actor. That makes a harness a build-time decision, because
// the binary has to import it. This one provisions nothing. It is given an address where a harness
// is already listening and returns an incarnation pointing at it, so a harness can be registered by
// configuration instead of by recompiling the server.
//
// The harness self-describes: Describe dials it and asks, so placement gates on the tier the harness
// actually declares rather than on anything configured here.
//
// Capabilities report MemorySnapshot=false, and that is not a limitation to be lifted later. This
// backend does not own the process it points at, so it cannot capture its memory. A harness that
// declares REQUIRES_MEMORY_SNAPSHOT is therefore refused by CanPlace before any compute is touched,
// which is the correct answer rather than a degraded one: run that harness on a backend that owns
// the sandbox.
package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
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
)

// Backend attaches sessions to a harness reachable at a fixed address.
type Backend struct {
	addr   string
	logger *slog.Logger

	mu      sync.Mutex
	conn    *grpc.ClientConn
	harness api.Harness
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

// connect dials the harness once and reuses the connection. gRPC reconnects underneath, so a
// harness that restarts is picked up again without this backend tracking its lifecycle.
func (b *Backend) connect() (api.Harness, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.harness != nil {
		return b.harness, nil
	}
	if b.addr == "" {
		return nil, errNoAddress
	}
	conn, err := grpc.NewClient(
		b.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("remote: dial harness at %s: %w", b.addr, err)
	}
	b.conn = conn
	b.harness = harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
	return b.harness, nil
}

// Close releases the connection to the harness. The harness itself is not ours to stop.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
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
	return h.Describe(ctx)
}

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
	case errors.Is(err, errCreateRequiresSessionSpec), errors.Is(err, errCreateRequiresSessionUID):
		return "invalid_request"
	default:
		return "remote_error"
	}
}
