// Package local implements the agentsessions api.Runtime compute SPI as a filesystem-only backend —
// the honest "plain pod" counterpart to runtime/substrate. It captures no RAM/process state: the
// event log IS the durable state, so resume is realized by STATELESS_REPLAY (the host replays the
// journal into a fresh incarnation). Capabilities report MemorySnapshot=false, which is what makes
// CanPlace meaningful — a REQUIRES_MEMORY_SNAPSHOT harness is refused here and accepted on substrate
// (honest degradation across two backends behind one SPI).
//
// The harness runs behind a real harnesswire gRPC server on a unix socket: Create returns an
// incarnation whose Address the placement layer dials via Harness.Connect — the SAME transport
// substrate uses — so determinism holds over the wire for local exactly as it will for substrate.
// Describe exposes the harness's static contract IN-PROCESS so the placement gate runs before Create.
package local

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/observability"
)

// Backend implements api.Runtime by serving a single harness over a unix socket (M0). Later the
// harness is selected per SessionSpec.Harness/Image from a registry.
type Backend struct {
	harness api.Harness
	worker  string

	mu   sync.Mutex
	srv  *grpc.Server
	sock string
	addr string

	logger *slog.Logger
}

var _ api.Runtime = (*Backend)(nil)

var socketSeq atomic.Int64

var (
	errCreateRequiresSessionSpec = errors.New("local: create requires a session spec")
	errCreateRequiresSessionUID  = errors.New("local: create requires a session uid")
)

// Option configures a local Backend.
type Option func(*Backend)

// WithLogger enables structured operational logs.
func WithLogger(logger *slog.Logger) Option { return func(b *Backend) { b.logger = logger } }

// New builds a backend that serves harness.
func New(harness api.Harness, opts ...Option) *Backend {
	host, _ := os.Hostname()
	if host == "" {
		host = "local"
	}
	b := &Backend{
		harness: harness,
		worker:  fmt.Sprintf("%s/%d", host, os.Getpid()),
		logger:  slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.logger == nil {
		b.logger = slog.New(slog.DiscardHandler)
	}
	return b
}

// Describe returns the placed harness's static contract. The placement layer reads it IN-PROCESS to
// gate CanPlace before Create — placement must not provision compute to learn a harness's needs.
func (b *Backend) Describe(ctx context.Context) (api.Descriptor, error) {
	return b.harness.Describe(ctx)
}

// start lazily stands up the harness as a harnesswire gRPC server on a unix socket (once per
// backend). All incarnations share it: the harness is stateless and each turn opens its own
// Harness.Connect stream, so a single server is the real transport the placement layer dials. Close
// tears it down.
func (b *Backend) start(ctx context.Context) (addr string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	decision := "started"
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "resolve_harness_server", "transport", "unix")
	defer func() { finish(err, "error_kind", localErrorKind(err), "decision", decision) }()

	if b.srv != nil {
		decision = "reused"
		return b.addr, nil
	}

	sock := filepath.Join(os.TempDir(), fmt.Sprintf("agentlocal-%d-%d.sock", os.Getpid(), socketSeq.Add(1)))
	_ = os.Remove(sock)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		return "", fmt.Errorf("local: listen: %w", err)
	}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(b.logger)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(b.logger)),
	)
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(b.harness))
	// The harness address is returned before Serve is known to have succeeded, so a failure here
	// would otherwise surface only as the Placer failing to dial an address this backend handed it.
	go func() {
		if err := srv.Serve(lis); err != nil {
			b.logger.Error("local harness server stopped", "error", err, "socket", sock)
		}
	}()
	b.srv, b.sock, b.addr = srv, sock, "unix://"+sock
	return b.addr, nil
}

// Close stops the shared harness server and removes its socket. Callers own the backend lifetime.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.srv != nil {
		b.srv.Stop()
		b.srv = nil
	}
	if b.sock != "" {
		_ = os.Remove(b.sock)
		b.sock = ""
	}
	return nil
}

// Create provisions an incarnation: it ensures the harness server is up and returns the unix-socket
// address the placement layer dials via Harness.Connect.
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (inc api.Incarnation, err error) {
	sessionUID := ""
	if s != nil {
		sessionUID = s.SessionUID
	}
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "create_compute", "session_uid", sessionUID)
	defer func() {
		finish(err, "error_kind", localErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if s == nil {
		return api.Incarnation{}, errCreateRequiresSessionSpec
	}
	if s.SessionUID == "" {
		return api.Incarnation{}, errCreateRequiresSessionUID
	}
	addr, err := b.start(ctx)
	if err != nil {
		return api.Incarnation{}, err
	}
	inc = api.Incarnation{
		ID:      s.SessionUID,
		Worker:  b.worker,
		Address: addr,
		Runtime: "local",
	}
	return inc, nil
}

// Snapshot is filesystem-only: the event log is the durable state, so there is nothing beyond it to
// capture. Only EXTERNAL (cold/suspend) is meaningful, and it just names the session so Restore can
// re-provision; Memory=false marks it as STATELESS_REPLAY. LOCAL (warm) is unsupported, mirroring
// substrate's inverse (substrate supports memory, not warm-local).
func (b *Backend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (ref api.SnapshotRef, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "snapshot",
		"incarnation_id", in.ID,
		"snapshot_kind", kind,
	)
	defer func() { finish(err, "error_kind", localErrorKind(err), "memory_snapshot", ref.Memory) }()

	if kind == api.SnapshotLocal {
		return api.SnapshotRef{}, fmt.Errorf("local: warm (LOCAL) snapshot not supported; the journal is the durable state — use EXTERNAL")
	}
	return api.SnapshotRef{Local: in.ID, Memory: false}, nil
}

// Restore re-provisions a fresh in-process incarnation; the host reconstructs harness-visible state
// by replaying the journal (STATELESS_REPLAY). The placement layer mints a new fence so the fresh
// incarnation supersedes any zombie writer.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "restore_compute", "session_uid", ref.Local)
	defer func() {
		finish(err, "error_kind", localErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if ref.Local == "" {
		return api.Incarnation{}, fmt.Errorf("local: snapshot ref missing session handle")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: ref.Local})
}

// Fork is a replay-fork: a fresh incarnation for the child, whose log prefix is copied by
// controller.Fork. Same shape as substrate's replay-fork — a filesystem-only backend has no
// clone-from-memory-snapshot path.
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "fork_compute",
		"parent_session_uid", ref.Local,
		"child_session_uid", opts.ChildSessionUID,
		"strategy", "replay",
	)
	defer func() {
		finish(err, "error_kind", localErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if opts.ChildSessionUID == "" {
		return api.Incarnation{}, fmt.Errorf("local: fork requires a child session uid")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: opts.ChildSessionUID})
}

// Stop tears the incarnation down. In-process there is no sandbox or socket to close; the worker is
// freed by the process, so this is a no-op that keeps the SPI contract.
func (b *Backend) Stop(ctx context.Context, in api.Incarnation) error {
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "stop_compute", "incarnation_id", in.ID)
	finish(nil)
	return nil
}

// Status reports the compute-lifecycle state. The in-process backend keeps no separate compute state
// machine — the log and placement layer are the lifecycle authority — so a known incarnation is LIVE.
func (b *Backend) Status(ctx context.Context, in api.Incarnation) (api.ComputeState, error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.local", "get_compute_status", "incarnation_id", in.ID)
	finish(nil, "compute_state", api.ComputeLive)
	return api.ComputeLive, nil
}

// Capabilities reports a filesystem-only backend: no memory snapshot, CoW fork, attestation, or GPU
// state. MemorySnapshot=false is the degradation counterpart that makes CanPlace meaningful.
func (b *Backend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: false, CoWFork: false, Attest: false, GPUState: false}
}

func localErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, errCreateRequiresSessionSpec), errors.Is(err, errCreateRequiresSessionUID):
		return "invalid_spec"
	default:
		return "runtime_operation_failed"
	}
}
