// Package substrate implements the agentsessions api.Runtime compute SPI on top of agent-substrate
// (the "actors on pre-warmed workers" runtime that memory-snapshots RAM+disk). It is a CLIENT of
// substrate's control plane, not a K8s/pod manager.
//
// Neutrality by design: the backend depends only on the ControlClient interface DEFINED HERE — a
// small subset of substrate's ateapi.Control gRPC. A real deployment provides a thin adapter
// implementing ControlClient over the generated substrate proto client; agentsessions does not
// import the (still largely aspirational) substrate module, so the core stays vendor-neutral and
// substrate is just one Runtime backend beside pod/Kata/CLH.
//
// Mapping (spike §2/§6): Create→CreateActor+ResumeActor{boot:true}; Snapshot(EXTERNAL)→SuspendActor;
// Restore→ResumeActor{boot:false}; Stop→SuspendActor+DeleteActor; Status→GetActor; Fork→replay-fork
// for a stateless harness, or TagActorSnapshot+CreateActor{source_snapshot}+ResumeActor{boot:false}
// to clone the parent's RAM for a REQUIRES_MEMORY_SNAPSHOT harness.
// Capabilities report MemorySnapshot=true — the headline differentiator vs a plain pod, which lets
// substrate host REQUIRES_MEMORY_SNAPSHOT harnesses a pod must refuse (the honest-degradation proof).
package substrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/observability"
)

// ActorStatus is substrate's actor lifecycle, narrowed to what the backend maps.
type ActorStatus int

const (
	StatusUnknown ActorStatus = iota
	StatusRunning
	StatusSuspended
	StatusTerminated
)

// ActorRef identifies an actor within an atespace (substrate's tenant/namespace).
type ActorRef struct {
	Atespace string
	Name     string
}

// ObjectRef is a namespaced substrate object (e.g. an ActorTemplate).
type ObjectRef struct {
	Namespace string
	Name      string
}

// ActorInfo is the subset of an actor's state the backend needs.
type ActorInfo struct {
	Status  ActorStatus
	PodIP   string // ateom_pod_ip once resumed onto a worker
	MeshDNS string // <actor>.<atespace>.actors... — where Harness.Connect reaches it
}

// ControlClient is the subset of agent-substrate's ateapi.Control gRPC that the backend uses. A
// real substrate deployment implements this over pkg/proto/ateapipb; keeping it an interface here
// is what keeps agentsessions free of a substrate dependency.
type ControlClient interface {
	CreateActor(ctx context.Context, actor ActorRef, template ObjectRef) error
	// ResumeActor schedules the actor onto a worker. boot=true cold-boots (bypassing any snapshot,
	// the STATELESS_REPLAY realization); boot=false restores the RAM+disk snapshot.
	ResumeActor(ctx context.Context, actor ActorRef, boot bool) (ActorInfo, error)
	// SuspendActor snapshots RAM+disk to durable storage and frees the worker, returning the
	// snapshot's handle. Substrate keeps the physical storage location private, so the handle is
	// the resulting ActorSnapshot's atespace-scoped NAME, not a storage URI.
	SuspendActor(ctx context.Context, actor ActorRef) (snapshot string, err error)
	DeleteActor(ctx context.Context, actor ActorRef) error
	GetActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
}

// ErrActorNotFound is what a ControlClient must return (or wrap) from GetActor when the actor does
// not exist. It is a sentinel rather than a transport code so the seam stays neutral: the backend
// decides "create it" vs "attach to it" without knowing that the client underneath speaks gRPC.
var ErrActorNotFound = errors.New("substrate: actor not found")

// SnapshotID addresses a durable ActorSnapshot, or an atespace-owned tag pointing at one. Substrate
// scopes both by atespace, unlike the k8s-namespaced ObjectRef used for an ActorTemplate.
type SnapshotID struct {
	Atespace string
	Name     string
}

// SnapshotCloner is the OPTIONAL clone-from-snapshot extension of ControlClient, backed by
// substrate's ActorSnapshot lifecycle APIs. Keeping it separate from ControlClient means a control
// client written against a substrate that predates those APIs still satisfies the base interface:
// stateless sessions keep forking (they never need a clone), and a stateful fork refuses loudly
// instead of silently losing the in-RAM state it exists to carry.
//
// Substrate's CreateActor accepts a snapshot only BY TAG (a canonical snapshot reference is
// rejected with FailedPrecondition), so cloning is always tag → actor.
type SnapshotCloner interface {
	// TagSnapshot gives an ActorSnapshot a stable, atespace-owned name. The tag is also a retention
	// pin: the snapshot becomes garbage-collectable when its last tag is deleted.
	TagSnapshot(ctx context.Context, snapshot, tag SnapshotID) error
	// CreateActorFromSnapshot creates actor initialized from the snapshot behind tag. Substrate
	// requires the actor's template to be the snapshot's exact source ActorTemplate.
	CreateActorFromSnapshot(ctx context.Context, actor ActorRef, template ObjectRef, tag SnapshotID) error
	// DeleteSnapshotTag removes a tag, releasing the retention pin it holds on the snapshot.
	DeleteSnapshotTag(ctx context.Context, tag SnapshotID) error
}

// Backend implements api.Runtime over a substrate ControlClient.
type Backend struct {
	ctl        ControlClient
	atespace   string
	template   ObjectRef      // the ActorTemplate whose OCI image is the harness
	descriptor api.Descriptor // the harness's declared contract (see Describe)
	logger     *slog.Logger
}

var _ api.Runtime = (*Backend)(nil)

// Option configures a substrate Backend.
type Option func(*Backend)

// WithLogger enables structured operational logs.
func WithLogger(logger *slog.Logger) Option { return func(b *Backend) { b.logger = logger } }

// New builds the backend targeting one atespace and harness ActorTemplate. descriptor is the
// harness's declared contract, used by the placement gate (see Describe).
func New(ctl ControlClient, atespace string, template ObjectRef, descriptor api.Descriptor, opts ...Option) *Backend {
	b := &Backend{
		ctl:        ctl,
		atespace:   atespace,
		template:   template,
		descriptor: descriptor,
		logger:     slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.logger == nil {
		b.logger = slog.New(slog.DiscardHandler)
	}
	return b
}

// Describe returns the harness's declared contract. Substrate's harness runs REMOTELY inside the
// actor, so — unlike runtime/local which reads it in-process — the descriptor is CONFIGURED at
// construction (from the ActorTemplate/registry). This is the M0 form of the spike's
// registry-resolution: the placement gate reads it BEFORE Create, without provisioning compute.
func (b *Backend) Describe(ctx context.Context) (api.Descriptor, error) {
	return b.descriptor, nil
}

func (b *Backend) ref(name string) ActorRef { return ActorRef{Atespace: b.atespace, Name: name} }

// Create places the session's actor and returns a live incarnation, whether or not the actor already
// exists. It is deliberately IDEMPOTENT, because the Placer calls it on every Exec and substrate's
// CreateActor rejects a repeat with AlreadyExists:
//
//   - absent — create it and cold boot (boot:true). The first placement of a session; there is no
//     durable state yet, so there is nothing a boot could destroy.
//   - RUNNING — attach. Return the actor's current address without touching it. This covers the
//     second and later turns of a session, and the first turn of a FORKED CHILD, whose actor was
//     created from the parent's snapshot and resumed before it was ever handed out.
//   - SUSPENDED — restore (boot:false), so the RAM snapshot is what comes back.
//
// The distinction matters most for a REQUIRES_MEMORY_SNAPSHOT harness: cold-booting an actor that
// already holds live state would silently discard exactly the state fork and suspend exist to carry,
// and the harness never rebuilds it from Start.History (I4).
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "create_compute",
		"session_uid", s.SessionUID,
		"atespace", b.atespace,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	ref := b.ref(s.SessionUID)
	switch info, err := b.ctl.GetActor(ctx, ref); {
	case err == nil && info.Status == StatusRunning:
		return b.incarnation(s.SessionUID, info)
	case err == nil && info.Status == StatusSuspended:
		info, err := b.ctl.ResumeActor(ctx, ref, false) // restore RAM, do not boot over it
		if err != nil {
			return api.Incarnation{}, fmt.Errorf("substrate: resume suspended actor: %w", err)
		}
		return b.incarnation(s.SessionUID, info)
	case err == nil:
		return api.Incarnation{}, fmt.Errorf("substrate: actor %q is in state %v, not placeable", ref.Name, info.Status)
	case !errors.Is(err, ErrActorNotFound):
		return api.Incarnation{}, fmt.Errorf("substrate: get actor: %w", err)
	}

	createFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "create_actor",
		"actor", ref.Name,
		"atespace", ref.Atespace,
	)
	if err := b.ctl.CreateActor(ctx, ref, b.template); err != nil {
		createFinished(err, "error_kind", "create_actor_failed")
		return api.Incarnation{}, fmt.Errorf("substrate: create actor: %w", err)
	}
	createFinished(nil)

	resumeFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "resume_actor",
		"actor", ref.Name,
		"atespace", ref.Atespace,
		"boot", true,
	)
	info, err := b.ctl.ResumeActor(ctx, ref, true) // cold boot: a brand-new actor has no state to lose
	if err != nil {
		resumeFinished(err, "error_kind", "resume_actor_failed")
		return api.Incarnation{}, fmt.Errorf("substrate: resume actor: %w", err)
	}
	resumeFinished(nil, "actor_status", info.Status)
	inc, err = b.incarnation(s.SessionUID, info)
	return inc, err
}

// Snapshot suspends the actor: RAM+disk snapshot to external storage, worker freed. Only the
// external (cold/suspend) kind is supported; substrate has no node-local warm checkpoint API today.
func (b *Backend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (snapshot api.SnapshotRef, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "snapshot_compute",
		"actor", in.ID,
		"atespace", b.atespace,
		"snapshot_kind", kind,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "memory_snapshot", snapshot.Memory)
	}()

	if kind == api.SnapshotLocal {
		return api.SnapshotRef{}, fmt.Errorf("substrate: local (warm) snapshot not supported; use EXTERNAL")
	}
	suspendFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "suspend_actor",
		"actor", in.ID,
		"atespace", b.atespace,
		"reason", "snapshot",
	)
	uri, err := b.ctl.SuspendActor(ctx, b.ref(in.ID))
	if err != nil {
		suspendFinished(err, "error_kind", "suspend_actor_failed")
		return api.SnapshotRef{}, fmt.Errorf("substrate: suspend actor: %w", err)
	}
	suspendFinished(nil)
	// Local carries the actor name (the handle Restore/ResumeActor needs); ExternalURI carries the
	// durable ActorSnapshot's name, which is what Fork tags and clones from. It is a substrate
	// object name rather than a storage URI because substrate keeps the physical location private.
	snapshot = api.SnapshotRef{Local: in.ID, ExternalURI: uri, Memory: true}
	return snapshot, nil
}

// Restore brings the actor back from its snapshot onto a (possibly different) worker.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "restore_compute",
		"actor", ref.Local,
		"atespace", b.atespace,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	name := ref.Local
	if name == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: snapshot ref missing actor name")
	}
	resumeFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "resume_actor",
		"actor", name,
		"atespace", b.atespace,
		"boot", false,
	)
	info, err := b.ctl.ResumeActor(ctx, b.ref(name), false) // restore snapshot
	if err != nil {
		resumeFinished(err, "error_kind", "resume_actor_failed")
		return api.Incarnation{}, fmt.Errorf("substrate: resume (restore) actor: %w", err)
	}
	resumeFinished(nil, "actor_status", info.Status)
	inc, err = b.incarnation(name, info)
	return inc, err
}

// ErrNoSnapshotToClone is returned when a REQUIRES_MEMORY_SNAPSHOT harness is forked without a
// parent memory snapshot to clone. Such a harness's live state exists only in RAM — the journal
// cannot reconstruct it (I4) — so cold-booting the child would silently produce a divergent branch.
// Failing loudly is the honest-degradation counterpart to the CanPlace gate.
var ErrNoSnapshotToClone = errors.New("substrate: fork of a memory-snapshot harness needs a parent snapshot")

// Fork branches a session's compute into a child incarnation.
//
// Two realizations, chosen by the harness's declared resumability:
//
//   - STATELESS_REPLAY — a replay-fork: the child is a fresh cold actor and the host replays the
//     copied journal prefix into it. The parent's RAM holds nothing the journal lacks, so cloning a
//     snapshot would only add a restore for no gain.
//   - REQUIRES_MEMORY_SNAPSHOT — a snapshot clone: the parent's in-RAM state is NOT in the journal,
//     so the child is created from the parent's durable ActorSnapshot (tag it, CreateActor from the
//     tag, then ResumeActor{boot:false} to restore the cloned RAM).
//
// Cloning is NOT copy-on-write: substrate restores each actor into a private per-actor directory,
// so an N-way fan-out costs N snapshot restores. Capabilities() reports CoWFork=false accordingly.
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (inc api.Incarnation, err error) {
	// Report which realization ran, since the two have very different costs: a replay-fork is one
	// CreateActor, a clone is a tag plus a full snapshot restore per child.
	strategy := "replay"
	if b.descriptor.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot {
		strategy = "clone"
	}
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "fork_compute",
		"parent_actor", ref.Local,
		"child_actor", opts.ChildSessionUID,
		"atespace", b.atespace,
		"strategy", strategy,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	child := opts.ChildSessionUID
	if child == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: fork requires a child session uid")
	}
	if b.descriptor.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		inc, err := b.Create(ctx, &api.SessionSpec{SessionUID: child})
		if err != nil {
			// Create's usual caller owns the session UID and can retry with it; a fork child's UID is
			// minted per request and dropped on error, so a half-created actor here would be
			// unreachable. Tear it down while its name is still known.
			b.destroyChild(ctx, child)
		}
		return inc, err
	}

	cloner, ok := b.ctl.(SnapshotCloner)
	if !ok {
		return api.Incarnation{}, fmt.Errorf("%w: control client does not implement SnapshotCloner", ErrNoSnapshotToClone)
	}
	if ref.ExternalURI == "" {
		return api.Incarnation{}, fmt.Errorf("%w: snapshot the parent before forking", ErrNoSnapshotToClone)
	}

	// Substrate clones only from a TAG, so pin the parent snapshot under a per-child name. The tag
	// is atespace-scoped, matching the child actor's atespace, so the default (ATESPACE) scope is
	// enough — no cross-atespace publication needed.
	source := SnapshotID{Atespace: b.atespace, Name: ref.ExternalURI}
	tag := SnapshotID{Atespace: b.atespace, Name: forkTag(child)}
	if err := cloner.TagSnapshot(ctx, source, tag); err != nil {
		return api.Incarnation{}, fmt.Errorf("substrate: tag snapshot %q: %w", source.Name, err)
	}
	if err := cloner.CreateActorFromSnapshot(ctx, b.ref(child), b.template, tag); err != nil {
		// No actor exists yet, but the pin does. Release it: the tag name is derived from a child UID
		// the caller is about to discard, so this is the last moment it can be named.
		b.releaseForkTag(ctx, cloner, child)
		return api.Incarnation{}, fmt.Errorf("substrate: create actor from snapshot tag %q: %w", tag.Name, err)
	}
	// boot=false restores the cloned RAM; boot=true would discard exactly the state we forked for.
	info, err := b.ctl.ResumeActor(ctx, b.ref(child), false)
	if err != nil {
		b.destroyChild(ctx, child)
		return api.Incarnation{}, fmt.Errorf("substrate: resume cloned actor: %w", err)
	}
	inc, err = b.incarnation(child, info)
	if err != nil {
		// The clone is created AND placed on a worker but reports no dialable address. Fork returns
		// no handle, so without this the actor would hold that worker forever under a name nobody
		// upstream still has.
		b.destroyChild(ctx, child)
		return api.Incarnation{}, err
	}
	return inc, nil
}

// cleanupTimeout bounds the best-effort teardown of a fork that failed partway.
const cleanupTimeout = 30 * time.Second

// cleanupContext detaches teardown from the caller's context. The most likely reason a fork fails
// midway is that the caller's deadline expired or it went away, which is exactly when a cleanup
// inheriting that context would fail instantly and strand everything it was written to reclaim.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
}

// destroyChild best-effort undoes everything a failed Fork provisioned for one child. Stop suspends
// before deleting (substrate rejects deleting a RUNNING actor) and releases the fork tag, so it
// covers a child that failed at any point after CreateActor.
func (b *Backend) destroyChild(ctx context.Context, child string) {
	ctx, cancel := cleanupContext(ctx)
	defer cancel()
	_ = b.Stop(ctx, api.Incarnation{ID: child})
}

// releaseForkTag drops the retention pin a fork placed on the parent snapshot, for the case where no
// child actor was ever created.
func (b *Backend) releaseForkTag(ctx context.Context, cloner SnapshotCloner, child string) {
	ctx, cancel := cleanupContext(ctx)
	defer cancel()
	_ = cloner.DeleteSnapshotTag(ctx, SnapshotID{Atespace: b.atespace, Name: forkTag(child)})
}

// forkTag names the retention pin a fork places on the parent snapshot. It is derived from the child
// session UID so concurrent forks of one parent never collide, and so Stop can name the pin later
// without carrying extra state.
//
// TODO(spike): substrate defers snapshot GC/retention (upstream #664), and a tag is the retention
// pin, so a live child's tag is held for as long as the child exists. Revisit once upstream
// retention semantics land.
func forkTag(childUID string) string { return "fork-" + childUID }

// Stop suspends then deletes the actor (substrate requires SUSPENDED before delete), then releases
// any retention pin the actor's own fork placed on its parent snapshot. The tag name is derived from
// the session UID, so teardown is the last moment it can be named; a session that was never forked
// simply has no such tag, which is why the release is best-effort.
func (b *Backend) Stop(ctx context.Context, in api.Incarnation) (err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "stop_compute",
		"actor", in.ID,
		"atespace", b.atespace,
	)
	defer func() { finish(err, "error_kind", substrateErrorKind(err)) }()

	ref := b.ref(in.ID)
	suspendFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "suspend_actor",
		"actor", ref.Name,
		"atespace", ref.Atespace,
	)
	if _, err := b.ctl.SuspendActor(ctx, ref); err != nil {
		suspendFinished(err, "error_kind", "suspend_actor_failed")
		return fmt.Errorf("substrate: suspend before delete: %w", err)
	}
	suspendFinished(nil)
	deleteFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "delete_actor",
		"actor", ref.Name,
		"atespace", ref.Atespace,
	)
	if err := b.ctl.DeleteActor(ctx, ref); err != nil {
		deleteFinished(err, "error_kind", "delete_actor_failed")
		return fmt.Errorf("substrate: delete actor: %w", err)
	}
	deleteFinished(nil)
	if cloner, ok := b.ctl.(SnapshotCloner); ok {
		_ = cloner.DeleteSnapshotTag(ctx, SnapshotID{Atespace: b.atespace, Name: forkTag(in.ID)})
	}
	return nil
}

// Status maps the actor's substrate status onto the agentsessions compute-lifecycle axis.
func (b *Backend) Status(ctx context.Context, in api.Incarnation) (state api.ComputeState, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "get_actor_status",
		"actor", in.ID,
		"atespace", b.atespace,
	)
	defer func() { finish(err, "error_kind", substrateErrorKind(err), "compute_state", state) }()

	info, err := b.ctl.GetActor(ctx, b.ref(in.ID))
	if err != nil {
		return api.ComputeState(""), fmt.Errorf("substrate: get actor: %w", err)
	}
	switch info.Status {
	case StatusRunning:
		state = api.ComputeLive
	case StatusSuspended:
		state = api.ComputeCold
	case StatusTerminated:
		state = api.ComputeTerminated
	default:
		state = api.ComputeNone
	}
	return state, nil
}

// Capabilities reports what substrate supports. MemorySnapshot=true is the headline differentiator
// vs a plain pod. CoWFork stays false deliberately: substrate can clone an actor from a durable
// ActorSnapshot, but each restore materializes a private per-actor copy (upstream #690 — there is no
// node-local snapshot cache yet), so a fork is a full restore, not a copy-on-write share.
// Attestation and GPU-state capture are not surfaced by the control API today (spike §3/§7).
func (b *Backend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{
		MemorySnapshot: true,
		CoWFork:        false,
		Attest:         false,
		GPUState:       false,
	}
}

// HarnessPort is the TCP port the in-sandbox harness (cmd/harnessnode) serves harnesswire on, and the
// port an in-cluster driver dials directly on the actor's pod IP. The atenet mesh is HTTP/1.1-only to
// actors, so gRPC bypasses the router and reaches PodIP:HarnessPort over h2c (matching google/ax's
// direct-dial path). Keep in sync with cmd/harnessnode's HARNESS_ADDR default.
const HarnessPort = "80"

// incarnation maps a resumed actor onto the compute handle the Placer drives. Address is the actor's
// pod IP + HarnessPort — the direct h2c dial target — not the mesh DNS, because the router cannot
// proxy gRPC. A missing pod IP (actor not scheduled onto a worker) is a loud error, never a silent
// dial to ":80".
func (b *Backend) incarnation(uid string, info ActorInfo) (api.Incarnation, error) {
	if info.PodIP == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: actor %q has no pod IP (not scheduled onto a worker?)", uid)
	}
	return api.Incarnation{
		ID:      uid,
		Worker:  info.PodIP,
		Address: net.JoinHostPort(info.PodIP, HarnessPort),
		Runtime: "substrate",
	}, nil
}

func substrateErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "runtime_operation_failed"
	}
}
