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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harnesswire"
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
}

var _ api.Runtime = (*Backend)(nil)

var socketSeq atomic.Int64

// New builds a backend that serves harness.
func New(harness api.Harness) *Backend {
	host, _ := os.Hostname()
	if host == "" {
		host = "local"
	}
	return &Backend{harness: harness, worker: fmt.Sprintf("%s/%d", host, os.Getpid())}
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
func (b *Backend) start() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.srv != nil {
		return b.addr, nil
	}
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("agentlocal-%d-%d.sock", os.Getpid(), socketSeq.Add(1)))
	_ = os.Remove(sock)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		return "", fmt.Errorf("local: listen: %w", err)
	}
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(b.harness))
	go srv.Serve(lis)
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
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	if s == nil || s.SessionUID == "" {
		return api.Incarnation{}, fmt.Errorf("local: create requires a session uid")
	}
	addr, err := b.start()
	if err != nil {
		return api.Incarnation{}, err
	}
	return api.Incarnation{
		ID:      s.SessionUID,
		Worker:  b.worker,
		Address: addr,
		Runtime: "local",
	}, nil
}

// Snapshot is filesystem-only: the event log is the durable state, so there is nothing beyond it to
// capture. Only EXTERNAL (cold/suspend) is meaningful, and it just names the session so Restore can
// re-provision; Memory=false marks it as STATELESS_REPLAY. LOCAL (warm) is unsupported, mirroring
// substrate's inverse (substrate supports memory, not warm-local).
func (b *Backend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	if kind == api.SnapshotLocal {
		return api.SnapshotRef{}, fmt.Errorf("local: warm (LOCAL) snapshot not supported; the journal is the durable state — use EXTERNAL")
	}
	return api.SnapshotRef{Local: in.ID, Memory: false}, nil
}

// Restore re-provisions a fresh in-process incarnation; the host reconstructs harness-visible state
// by replaying the journal (STATELESS_REPLAY). The placement layer mints a new fence so the fresh
// incarnation supersedes any zombie writer.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	if ref.Local == "" {
		return api.Incarnation{}, fmt.Errorf("local: snapshot ref missing session handle")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: ref.Local})
}

// Fork is a replay-fork: a fresh incarnation for the child, whose log prefix is copied by
// controller.Fork. Same shape as substrate's replay-fork — a filesystem-only backend has no
// clone-from-memory-snapshot path.
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (api.Incarnation, error) {
	if opts.ChildSessionUID == "" {
		return api.Incarnation{}, fmt.Errorf("local: fork requires a child session uid")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: opts.ChildSessionUID})
}

// Stop tears the incarnation down. In-process there is no sandbox or socket to close; the worker is
// freed by the process, so this is a no-op that keeps the SPI contract.
func (b *Backend) Stop(ctx context.Context, in api.Incarnation) error { return nil }

// Status reports the compute-lifecycle state. The in-process backend keeps no separate compute state
// machine — the log and placement layer are the lifecycle authority — so a known incarnation is LIVE.
func (b *Backend) Status(ctx context.Context, in api.Incarnation) (api.ComputeState, error) {
	return api.ComputeLive, nil
}

// Capabilities reports a filesystem-only backend: no memory snapshot, CoW fork, attestation, or GPU
// state. MemorySnapshot=false is the degradation counterpart that makes CanPlace meaningful.
func (b *Backend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: false, CoWFork: false, Attest: false, GPUState: false}
}
