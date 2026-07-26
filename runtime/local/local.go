// Package local implements the agentsessions api.Runtime compute SPI as a filesystem-only,
// in-process backend — the honest "plain pod" counterpart to runtime/substrate. It captures no
// RAM/process state: the event log IS the durable state, so resume is realized by STATELESS_REPLAY
// (the host replays the journal into a fresh incarnation). Capabilities report MemorySnapshot=false,
// which is what makes CanPlace meaningful — a REQUIRES_MEMORY_SNAPSHOT harness is refused here and
// accepted on substrate (honest degradation across two backends behind one SPI).
//
// This first cut runs the harness IN-PROCESS: Create returns an incarnation with a synthetic address
// and the placement layer drives the held api.Harness directly (Backend.Harness). A follow-up swaps
// this for a harnesswire unix-socket server so the real Harness.Connect transport — the same seam
// substrate uses — is exercised (design note §4/§10 step 6).
package local

import (
	"context"
	"fmt"
	"os"

	"github.com/aramase/agentsessions/api"
)

// Backend implements api.Runtime by placing a single harness in-process (M0). Later the harness is
// selected per SessionSpec.Harness/Image from a registry.
type Backend struct {
	harness api.Harness
	worker  string
}

var _ api.Runtime = (*Backend)(nil)

// New builds an in-process backend that places harness.
func New(harness api.Harness) *Backend {
	host, _ := os.Hostname()
	if host == "" {
		host = "local"
	}
	return &Backend{harness: harness, worker: fmt.Sprintf("%s/%d", host, os.Getpid())}
}

// Harness returns the in-process harness handle the placement layer drives directly, in lieu of
// dialing Incarnation.Address over the wire. (The socket-server variant replaces this — §10 step 6.)
func (b *Backend) Harness() api.Harness { return b.harness }

// Create provisions an in-process incarnation. There is no sandbox to boot; the address is synthetic
// (inproc://) and signals that the placement layer uses Harness() rather than Harness.Connect.
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	if s == nil || s.SessionUID == "" {
		return api.Incarnation{}, fmt.Errorf("local: create requires a session uid")
	}
	return api.Incarnation{
		ID:      s.SessionUID,
		Worker:  b.worker,
		Address: "inproc://" + s.SessionUID,
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
