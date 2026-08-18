package api

import "context"

// Runtime is the compute/sandbox SPI. Pod, Kata Pod Sandboxing, Cloud Hypervisor,
// Dragonball, Hyperlight, and agent-substrate all implement it. There is no gVisor
// requirement — backends are interchangeable, and the host reads Capabilities to place
// harnesses and degrade honestly.
//
// This is the seam that lets "different folks on top" (via producer adapters + the
// Sessions API) run on "substrate underneath" (or pod, Kata, ...) with no API change.
type Runtime interface {
	// Create starts a cold incarnation (sandbox) for a session and returns a handle
	// including the address where the harness server listens.
	Create(ctx context.Context, s *SessionSpec) (Incarnation, error)

	// Snapshot captures the incarnation's state. SnapshotLocal (warm/pause) keeps it
	// node-resident; SnapshotExternal (cold/suspend) writes to object storage. A backend
	// without memory snapshot returns a filesystem-only SnapshotRef.
	Snapshot(ctx context.Context, in Incarnation, kind SnapshotKind) (SnapshotRef, error)

	// Restore brings an incarnation back from a snapshot. Sub-second on memory-capable
	// backends; a filesystem-only backend restores the workspace and relies on the host
	// replaying the event log.
	Restore(ctx context.Context, ref SnapshotRef) (Incarnation, error)

	// Fork creates a child incarnation. A backend with MemorySnapshot clones ref's captured state so
	// the child continues from the parent's RAM; a filesystem-only backend returns a fresh incarnation
	// and relies on the host replaying the child's copied log prefix. Implementations must not leak a
	// partially-provisioned child: on any failure after the child exists, tear it down before returning.
	Fork(ctx context.Context, ref SnapshotRef, opts ForkOpts) (Incarnation, error)

	// Stop destroys the incarnation and frees the worker.
	Stop(ctx context.Context, in Incarnation) error

	// Status reports the incarnation's compute-lifecycle state.
	Status(ctx context.Context, in Incarnation) (ComputeState, error)

	// Capabilities reports what this backend supports.
	Capabilities() RuntimeCapabilities
}

// SnapshotKind selects local (warm/pause) vs external (cold/suspend) storage.
type SnapshotKind string

const (
	SnapshotLocal    SnapshotKind = "LOCAL"
	SnapshotExternal SnapshotKind = "EXTERNAL"
)

// SnapshotRef points at a captured incarnation state. This Go SPI type is the canonical, WIRED
// representation: Runtime backends produce/consume it, and the durable log carries it inline on the
// SUSPEND Lifecycle event (api.Lifecycle.Snapshot -> proto Lifecycle.snapshot_* fields). The proto
// message agentsessions.v1.SnapshotRef (session.proto, used by ComputeRef) is a SEPARATE, not-yet-
// wired session-status shape — see its TODO. Unlike that oneof, this holds both Local and ExternalURI
// (a substrate ref sets both).
type SnapshotRef struct {
	Local       string // node/handle-local ref, or (filesystem-only) the session handle to replay
	ExternalURI string // external blob (cold/suspend)
	Memory      bool   // RAM/process captured? kata/clh: true, pod: false
	Sealed      bool   // encrypted + attested (confidential)
}

// Incarnation is a live compute instance of a session (a sandbox).
type Incarnation struct {
	ID         string
	Worker     string // pod/worker id
	Address    string // where the harness server listens
	Runtime    string // "pod" | "kata" | "clh" | "substrate"
	FenceToken int64  // monotonic; the log rejects appends from a superseded incarnation
}

// SessionSpec is what a Runtime needs to create an incarnation.
type SessionSpec struct {
	SessionUID string
	Image      string
	Harness    string
	Identity   IdentityRef
	// resource requests, env, mounts elided for v0
}

// ForkOpts parameterizes a fork.
type ForkOpts struct {
	ChildSessionUID string
	// No copy-on-write knob: every backend reports CoWFork=false today (substrate clones via a full
	// snapshot restore), and if that changes it is likely a property of how restores work rather than
	// a per-call request. Read RuntimeCapabilities.CoWFork for what a backend can actually do.
}

// RuntimeCapabilities is what a backend can do. The host matches these against a
// harness's Capabilities before scheduling.
type RuntimeCapabilities struct {
	MemorySnapshot bool // can capture/restore RAM+process (pod: false, kata/clh: true)
	CoWFork        bool // copy-on-write fork
	Attest         bool // attestation on restore (confidential)
	GPUState       bool // accelerator state capture
}
