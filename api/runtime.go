package api

import (
	"context"
	"errors"
)

// ErrSnapshotSuperseded reports that a fork's source snapshot is no longer the one the parent holds,
// because the parent was resumed (and possibly checkpointed again) after the snapshot was taken. The
// fork cannot clone it without pairing the children's journal prefix with different RAM; the
// caller can retry the fork at the parent's new head.
var ErrSnapshotSuperseded = errors.New("runtime: fork source snapshot was superseded")

// ErrMemoryStateLost reports that a backend cannot give a session whose harness keeps its state
// only in RAM (REQUIRES_MEMORY_SNAPSHOT) the compute that held that state, because the compute is
// gone. Create returns it rather than create fresh compute when the journal already records a
// started turn on that harness (SessionSpec.StartedTurns). Restore returns it for any such session
// whose compute no longer exists, whatever the journal records, since the snapshot it would restore
// went with the compute; the recorded compute, still there and holding the session's snapshot,
// keeps the restore error (see InstanceRestorer). The host returns it when the compute a turn or a
// Resume was placed on reports another Incarnation.ComputeInstance than the journal recorded: the
// compute was replaced under the same name. Snapshot returns it when the compute behind
// Incarnation.ID is not the instance the caller required (a stateful fork). The journal cannot
// rebuild the state (I4), so fresh compute would continue the journal with a harness that has
// forgotten it. The caller cannot fix this by retrying; the session service reports it as
// FAILED_PRECONDITION.
var ErrMemoryStateLost = errors.New("runtime: session's in-memory harness state is lost")

// ErrComputeBusy reports that the session's compute is in the middle of another operation (another
// host resuming or suspending it, say), so the backend could not place onto it or act on it right
// now. Nothing was journaled, and the state settles on its own, so the session service reports it
// as ABORTED and the caller retries.
var ErrComputeBusy = errors.New("runtime: compute is busy with another operation")

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
	// node-resident. SnapshotExternal owns the cold transition: it captures required state,
	// releases dedicated per-incarnation compute where applicable, and preserves the state
	// and any backend handles Restore needs. A backend without memory snapshots may return
	// a filesystem/journal handle; without dedicated per-session compute, its shared harness
	// server may remain running.
	//
	// A non-empty in.ComputeInstance is the instance the caller requires behind in.ID (the
	// latest one the journal recorded). A backend that sets ComputeInstance refuses with
	// ErrMemoryStateLost when another instance is there, or none is, before it captures anything
	// if it can tell, and in any case before returning a ref: a snapshot of a replacement would
	// hand its callers compute that holds none of the session's state.
	Snapshot(ctx context.Context, in Incarnation, kind SnapshotKind) (SnapshotRef, error)

	// Restore brings an incarnation back from a snapshot. Sub-second on memory-capable
	// backends; a filesystem-only backend restores the workspace and relies on the host
	// replaying the event log.
	Restore(ctx context.Context, ref SnapshotRef) (Incarnation, error)

	// Fork creates a child incarnation. A backend with MemorySnapshot clones ref's captured state so
	// the child continues from the parent's RAM; a filesystem-only backend returns a fresh incarnation
	// and relies on the host replaying the child's copied log prefix. Implementations must not leak a
	// partially-provisioned child: on any failure after the child exists, tear it down before returning.
	// A backend that finds ref no longer held by the parent returns an error wrapping
	// ErrSnapshotSuperseded.
	Fork(ctx context.Context, ref SnapshotRef, opts ForkOpts) (Incarnation, error)

	// Stop is destructive teardown: it destroys the incarnation and frees its dedicated compute.
	// Callers must not assume a captured SnapshotRef remains restorable after Stop.
	Stop(ctx context.Context, in Incarnation) error

	// Status reports the incarnation's compute-lifecycle state.
	Status(ctx context.Context, in Incarnation) (ComputeState, error)

	// Capabilities reports what this backend supports.
	Capabilities() RuntimeCapabilities
}

// InstanceRestorer is an optional extension of Runtime for a backend that sets
// Incarnation.ComputeInstance. RestoreInstance is Restore with a way to read the compute instance the
// journal last recorded for the session's harness, so that when the restore fails the backend can
// tell whether that instance is gone or replaced. recorded returns "" when no instance is recorded;
// the backend calls it only after a failed restore, when it decides, so a turn another host journaled
// before then is seen. A backend reports ErrMemoryStateLost from RestoreInstance only when it can
// show the recorded instance gone or replaced or, with none recorded, finds an instance it cannot
// tell from a replacement, and otherwise returns the restore error: an instance that is still there
// may fail a restore for another reason. The host calls RestoreInstance in place of Restore when the
// backend implements it.
type InstanceRestorer interface {
	RestoreInstance(ctx context.Context, ref SnapshotRef, recorded func() (string, error)) (Incarnation, error)
}

// SnapshotKind selects local (warm/pause) vs external (cold/suspend) storage.
type SnapshotKind string

const (
	// SnapshotLocal keeps the captured state warm and node-resident.
	SnapshotLocal SnapshotKind = "LOCAL"
	// SnapshotExternal transitions to cold, releasing dedicated compute while retaining restore state.
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
	Address    string // where the harness server listens, or the ingress that routes to it
	Runtime    string // "pod" | "kata" | "clh" | "substrate"
	FenceToken int64  // monotonic; the log rejects appends from a superseded incarnation
	// CallMetadata is request metadata the host must attach to every call it makes to the harness at
	// Address. A backend whose harness sits behind a shared ingress uses it to name the target
	// sandbox (substrate: ate-target-actor); a backend that hands out a direct address leaves it
	// empty. It is routing, not authentication: nothing here proves the caller's identity.
	CallMetadata map[string]string
	// ComputeInstance is an opaque identity of the compute instance behind ID that differs between two
	// instances reusing one ID (substrate: the actor's server-assigned UID, while ID is the actor
	// name). A backend sets it only when the harness keeps its state in that instance's memory
	// (REQUIRES_MEMORY_SNAPSHOT). The host records it in the turn's EXECUTION_START and refuses a turn
	// or a Resume placed on an instance other than the latest one the journal recorded for that
	// harness since the session's FORK marker (ErrMemoryStateLost): a replacement holds none of the
	// state. A stateful fork passes that recorded instance to Snapshot, which refuses another one.
	// Empty records nothing and skips the check.
	ComputeInstance string
}

// SessionSpec is what a Runtime needs to create an incarnation.
type SessionSpec struct {
	SessionUID string
	Image      string
	Harness    string
	Identity   IdentityRef
	// StartedTurns counts the turns the session's journal records as started (an EXECUTION_START) on
	// the harness being placed, whether they completed, failed or were interrupted: any of them may
	// have changed that harness's in-memory state. For a forked child it counts the turns of the copied
	// prefix that ran on this harness. A turn another harness ran (an ExecRequest.harness override) is
	// not counted, since it left no state in this harness's compute; a turn the host cannot attribute
	// (no recorded harness name, or a name it does not serve) is.
	//
	// A backend uses it to refuse creating fresh compute for a REQUIRES_MEMORY_SNAPSHOT session that
	// already has state only its old compute held (ErrMemoryStateLost). It is a function rather than a
	// number so that the backend counts only after it has seen the compute is missing: a turn is
	// journaled before it runs, so every turn that ran on the lost compute is then visible, including
	// one another host sharing the journal recorded while this call was being placed. A backend that
	// does not need it does not call it. Nil declares a session with no started turns on this harness.
	StartedTurns func() (int, error)
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
