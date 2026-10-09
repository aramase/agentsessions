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
// Mapping: Create→CreateActor+ResumeActor; Snapshot(EXTERNAL)→SuspendActor; Restore→ResumeActor;
// Stop→SuspendActor+DeleteActor; Status→GetActor; Fork→replay-fork for a stateless harness, or
// CreateTag{source_actor}+CreateActor{source_tag}+ResumeActor to clone the parent's RAM for a
// REQUIRES_MEMORY_SNAPSHOT harness.
//
// Transport: the harness is reached through substrate's atenet-router, the only supported ingress
// to an actor. Every incarnation addresses the router and carries the ate-target-actor metadata that
// selects the actor; the router resumes the actor if needed and proxies the h2c gRPC stream to it.
// Capabilities report MemorySnapshot=true — the headline differentiator vs a plain pod, which lets
// substrate host REQUIRES_MEMORY_SNAPSHOT harnesses a pod must refuse (the honest-degradation proof).
package substrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// ObjectRef is an atespace-scoped substrate object other than an actor (e.g. an ActorTemplate).
type ObjectRef struct {
	Atespace string
	Name     string
}

// ActorInfo is the subset of an actor's state the backend needs.
type ActorInfo struct {
	Status ActorStatus
	// Worker names the worker hosting the actor while it holds one, for diagnostics only. The backend
	// never dials it: the harness is reached through the router, which resolves the worker per call.
	Worker string
	// Snapshot is the actor's current external snapshot handle, or "" before its first suspend. It
	// changes on every suspend, so comparing it against a recorded handle tells whether the actor
	// still holds the snapshot a caller took.
	Snapshot string
}

// ControlClient is the subset of agent-substrate's ateapi.Control gRPC that the backend uses. A
// real substrate deployment implements this over pkg/proto/ateapipb; keeping it an interface here
// is what keeps agentsessions free of a substrate dependency.
type ControlClient interface {
	CreateActor(ctx context.Context, actor ActorRef, template ObjectRef) error
	// ResumeActor schedules the actor onto a worker. Substrate picks the source itself: the actor's
	// own snapshot if it has one, else the template's golden snapshot (a freshly started harness),
	// else a cold boot from the template spec. There is no per-call boot flag.
	ResumeActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
	// SuspendActor snapshots the actor to durable storage and frees the worker, returning the handle
	// of the external snapshot it now holds (ActorInfo.Snapshot).
	SuspendActor(ctx context.Context, actor ActorRef) (snapshot string, err error)
	DeleteActor(ctx context.Context, actor ActorRef) error
	GetActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
}

// ErrActorNotFound is what a ControlClient must return (or wrap) from GetActor when the actor does
// not exist. It is a sentinel rather than a transport code so the seam stays neutral: the backend
// decides "create it" vs "attach to it" without knowing that the client underneath speaks gRPC.
var ErrActorNotFound = errors.New("substrate: actor not found")

// ErrTagExists is what a SnapshotCloner must return (or wrap) from TagActor when a tag of that name
// already exists, finished or left pending by an earlier attempt. It tells the backend the tag is
// not this attempt's to clean up.
var ErrTagExists = errors.New("substrate: tag already exists")

// SnapshotID addresses an atespace-owned substrate Tag.
type SnapshotID struct {
	Atespace string
	Name     string
}

// SnapshotCloner is the OPTIONAL clone-from-snapshot extension of ControlClient, backed by
// substrate's Tag APIs. Keeping it separate from ControlClient means a control client that does not
// implement tags still satisfies the base interface: stateless sessions keep forking (they never need
// a clone), and a stateful fork refuses loudly instead of silently losing the in-RAM state it exists
// to carry.
//
// Substrate's CreateActor seeds an actor only from a Tag, and a Tag names a SUSPENDED source actor
// rather than a snapshot, so cloning is always actor → tag → actor.
type SnapshotCloner interface {
	// TagActor tags the external snapshot the SUSPENDED source actor holds right now. The tag gets
	// its own copy of that snapshot, so suspending or deleting the source afterwards cannot collect
	// it. The tag lives in the source actor's atespace. Substrate reserves the tag's name before it
	// copies, so a call that fails may still leave a pending tag behind; a name that was already
	// taken fails with ErrTagExists.
	TagActor(ctx context.Context, source ActorRef, tag SnapshotID) error
	// CreateActorFromTag creates actor seeded from the snapshot behind tag. Substrate requires the
	// actor's template to be the one the snapshot was taken under.
	CreateActorFromTag(ctx context.Context, actor ActorRef, template ObjectRef, tag SnapshotID) error
	// DeleteTag removes a tag and collects its copy of the snapshot. An actor seeded from the tag
	// borrows that copy until its own first suspend, so a tag must outlive its unsuspended clones.
	DeleteTag(ctx context.Context, tag SnapshotID) error
}

// Backend implements api.Runtime over a substrate ControlClient.
type Backend struct {
	ctl        ControlClient
	atespace   string
	template   ObjectRef      // the ActorTemplate whose OCI image is the harness
	descriptor api.Descriptor // the harness's declared contract (see Describe)
	router     string         // host:port of the atenet-router ingress the harness is reached through
	logger     *slog.Logger
}

var _ api.Runtime = (*Backend)(nil)

var (
	errCreateRequiresSessionSpec = errors.New("substrate: create requires a session spec")
	errCreateRequiresSessionUID  = errors.New("substrate: create requires a session uid")
)

// Option configures a substrate Backend.
type Option func(*Backend)

// WithLogger enables structured operational logs.
func WithLogger(logger *slog.Logger) Option { return func(b *Backend) { b.logger = logger } }

// WithRouter overrides the atenet-router address (host:port) incarnations point at. The default,
// DefaultRouterAddress, is the in-cluster Service a stock substrate install creates.
func WithRouter(address string) Option { return func(b *Backend) { b.router = address } }

// New builds the backend targeting one atespace and harness ActorTemplate. descriptor is the
// harness's declared contract, used by the placement gate (see Describe).
func New(ctl ControlClient, atespace string, template ObjectRef, descriptor api.Descriptor, opts ...Option) *Backend {
	b := &Backend{
		ctl:        ctl,
		atespace:   atespace,
		template:   template,
		descriptor: descriptor,
		router:     DefaultRouterAddress,
		logger:     slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.logger == nil {
		b.logger = slog.New(slog.DiscardHandler)
	}
	if b.router == "" {
		b.router = DefaultRouterAddress
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
//   - absent — create it, then resume it. A new actor holds no snapshot of its own, so substrate
//     starts it from the template's golden snapshot (a harness captured right after it became ready,
//     before any session touched it) or, if that is not built yet, cold-boots the template spec.
//     Either way the harness carries no session state, which is what the first placement needs.
//   - RUNNING — attach. Return the actor's incarnation without touching it. This covers the second
//     and later turns of a session, and the first turn of a FORKED CHILD, whose actor was created
//     from the parent's snapshot and resumed before it was ever handed out.
//   - SUSPENDED — resume, which restores the actor's own snapshot, so its RAM is what comes back.
//
// The distinction matters most for a REQUIRES_MEMORY_SNAPSHOT harness: recreating an actor that
// already holds live state would silently discard exactly the state fork and suspend exist to carry,
// and the harness never rebuilds it from Start.History (I4).
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (inc api.Incarnation, err error) {
	sessionUID := ""
	if s != nil {
		sessionUID = s.SessionUID
	}
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "create_compute",
		"session_uid", sessionUID,
		"atespace", b.atespace,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if s == nil {
		return api.Incarnation{}, errCreateRequiresSessionSpec
	}
	if s.SessionUID == "" {
		return api.Incarnation{}, errCreateRequiresSessionUID
	}

	ref := b.ref(sessionUID)
	switch info, err := b.ctl.GetActor(ctx, ref); {
	case err == nil && info.Status == StatusRunning:
		return b.incarnation(sessionUID, info)
	case err == nil && info.Status == StatusSuspended:
		info, err := b.ctl.ResumeActor(ctx, ref) // restores the actor's own snapshot
		if err != nil {
			return api.Incarnation{}, fmt.Errorf("substrate: resume suspended actor: %w", err)
		}
		return b.incarnation(sessionUID, info)
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
	)
	info, err := b.ctl.ResumeActor(ctx, ref) // a brand-new actor has no session state to lose
	if err != nil {
		resumeFinished(err, "error_kind", "resume_actor_failed")
		return api.Incarnation{}, fmt.Errorf("substrate: resume actor: %w", err)
	}
	resumeFinished(nil, "actor_status", info.Status)
	inc, err = b.incarnation(sessionUID, info)
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
	// external snapshot the suspend produced. Fork clones by tagging the ACTOR, not this handle, and
	// uses the handle to check that the actor still holds this exact snapshot when it is tagged.
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
	)
	info, err := b.ctl.ResumeActor(ctx, b.ref(name)) // restores the actor's own snapshot
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

// ErrSnapshotSuperseded is returned when a stateful fork finds that the parent no longer holds the
// snapshot the fork took. Substrate tags whatever snapshot an actor holds at tag time, and the router
// resumes an actor on any request addressed to it, so a parent woken and suspended again between the
// checkpoint and the tag would hand the children RAM that does not match their copied journal prefix.
var ErrSnapshotSuperseded = errors.New("substrate: parent snapshot was superseded before the fork could tag it")

// Fork branches a session's compute into a child incarnation.
//
// Two realizations, chosen by the harness's declared resumability:
//
//   - STATELESS_REPLAY — a replay-fork: the child is a fresh cold actor and the host replays the
//     copied journal prefix into it. The parent's RAM holds nothing the journal lacks, so cloning a
//     snapshot would only add a restore for no gain.
//   - REQUIRES_MEMORY_SNAPSHOT — a snapshot clone: the parent's in-RAM state is NOT in the journal,
//     so the child is created from the parent's external snapshot (tag the suspended parent,
//     CreateActor from the tag, then ResumeActor to restore the cloned RAM).
//
// Cloning is NOT copy-on-write: each tag copies the snapshot in object storage and each child
// restores into a private per-actor directory, so an N-way fan-out costs N copies and N restores.
// Capabilities() reports CoWFork=false accordingly.
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

	// Substrate clones only from a TAG, and a tag names the suspended parent ACTOR, capturing
	// whichever snapshot it holds at that moment. Bracket the tag with checks that the parent still
	// holds the snapshot this fork took: a suspend always writes a new snapshot, so seeing the same
	// handle on both sides proves the tag copied it. The tag is per child and atespace-scoped,
	// matching the child actor's atespace, so no cross-atespace publication is needed.
	parent := b.ref(ref.Local)
	tag := SnapshotID{Atespace: b.atespace, Name: forkTag(child)}
	if err := b.requireSnapshot(ctx, parent, ref.ExternalURI); err != nil {
		return api.Incarnation{}, err
	}
	if err := cloner.TagActor(ctx, parent, tag); err != nil {
		// Substrate reserves the tag before it copies the snapshot, so a failed or timed-out create
		// can leave a pending tag holding a partial copy, and the atespace cannot be deleted while it
		// remains. Release it, unless the name was already taken: that tag is not this attempt's.
		if !errors.Is(err, ErrTagExists) {
			b.releaseForkTag(ctx, cloner, child)
		}
		return api.Incarnation{}, fmt.Errorf("substrate: tag parent %q: %w", parent.Name, err)
	}
	if err := b.requireSnapshot(ctx, parent, ref.ExternalURI); err != nil {
		b.releaseForkTag(ctx, cloner, child)
		return api.Incarnation{}, err
	}
	if err := cloner.CreateActorFromTag(ctx, b.ref(child), b.template, tag); err != nil {
		// No actor exists yet, but the tag does. Release it: the tag name is derived from a child UID
		// the caller is about to discard, so this is the last moment it can be named.
		b.releaseForkTag(ctx, cloner, child)
		return api.Incarnation{}, fmt.Errorf("substrate: create actor from tag %q: %w", tag.Name, err)
	}
	// The child holds the tag's snapshot, so resuming it restores the cloned RAM.
	info, err := b.ctl.ResumeActor(ctx, b.ref(child))
	if err != nil {
		b.destroyChild(ctx, child)
		return api.Incarnation{}, fmt.Errorf("substrate: resume cloned actor: %w", err)
	}
	inc, err = b.incarnation(child, info)
	if err != nil {
		// The clone was created and resumed but is not reported RUNNING. Fork returns no handle, so
		// without this the actor could hold a worker forever under a name nobody upstream still has.
		b.destroyChild(ctx, child)
		return api.Incarnation{}, err
	}
	return inc, nil
}

// requireSnapshot fails with ErrSnapshotSuperseded unless the actor is SUSPENDED holding exactly
// snapshot.
func (b *Backend) requireSnapshot(ctx context.Context, actor ActorRef, snapshot string) error {
	info, err := b.ctl.GetActor(ctx, actor)
	if err != nil {
		return fmt.Errorf("substrate: get fork parent %q: %w", actor.Name, err)
	}
	if info.Status != StatusSuspended || info.Snapshot != snapshot {
		return fmt.Errorf("%w: parent %q is %v holding %q, want suspended holding %q",
			ErrSnapshotSuperseded, actor.Name, info.Status, info.Snapshot, snapshot)
	}
	return nil
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
// before deleting (substrate rejects deleting a RUNNING actor) and then deletes the fork tag, so it
// covers a child that failed at any point after CreateActor.
func (b *Backend) destroyChild(ctx context.Context, child string) {
	ctx, cancel := cleanupContext(ctx)
	defer cancel()
	_ = b.Stop(ctx, api.Incarnation{ID: child})
}

// releaseForkTag deletes the tag a fork took of the parent, for the case where no child actor was
// ever created from it.
func (b *Backend) releaseForkTag(ctx context.Context, cloner SnapshotCloner, child string) {
	ctx, cancel := cleanupContext(ctx)
	defer cancel()
	_ = cloner.DeleteTag(ctx, SnapshotID{Atespace: b.atespace, Name: forkTag(child)})
}

// forkTag names the tag a fork takes of the parent for one child. It is derived from the child
// session UID so concurrent forks of one parent never collide, and so Stop can name the tag later
// without carrying extra state.
//
// The tag owns a full copy of the parent's snapshot, and the child borrows that copy until its own
// first suspend, so the tag is kept for the child's lifetime and deleted by Stop.
// TODO(spike): one tag per fan-out instead of per child would save N-1 snapshot copies, but a shared
// tag can only be deleted once every child has suspended at least once; measure the copy cost first.
func forkTag(childUID string) string { return "fork-" + childUID }

// Stop suspends then deletes the actor (substrate requires SUSPENDED before delete), then deletes
// the tag the actor's own fork took of its parent. The tag name is derived from the session UID, so
// teardown is the last moment it can be named. The actor is deleted first because an actor that
// never suspended still borrows the tag's snapshot. A session that was never forked simply has no
// such tag, which is why the delete is best-effort.
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
		_ = cloner.DeleteTag(ctx, SnapshotID{Atespace: b.atespace, Name: forkTag(in.ID)})
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
// vs a plain pod. CoWFork stays false deliberately: substrate can clone an actor from a tagged
// snapshot, but the tag copies the snapshot and each restore materializes a private per-actor copy,
// so a fork is a full copy and restore, not a copy-on-write share.
// Attestation and GPU-state capture are not surfaced by the control API today (spike §3/§7).
func (b *Backend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{
		MemorySnapshot: true,
		CoWFork:        false,
		Attest:         false,
		GPUState:       false,
	}
}

// HarnessPort is the TCP port the in-sandbox harness (cmd/harnessnode) serves harnesswire on. The
// router's default ingress forwards to port 80 in the actor, so this is not configurable per
// incarnation. Keep in sync with cmd/harnessnode's HARNESS_ADDR default.
const HarnessPort = "80"

// DefaultRouterAddress is the atenet-router Service a stock substrate install creates. The router
// accepts h2c on port 80 and carries gRPC unary and bidi streams, trailers included, to the actor's
// HarnessPort.
const DefaultRouterAddress = "atenet-router.ate-system.svc:80"

// TargetActorHeader is the request header the atenet-router reads to pick the actor, with the value
// "<atespace>/<actor>". The router ignores the host and authority, so without it a call reaches no
// actor. It mirrors substrate internal/atenet.TargetActorHeader, which is not importable.
const TargetActorHeader = "ate-target-actor"

// incarnation maps a resumed actor onto the compute handle the Placer drives. Address is the router
// and CallMetadata names the actor: substrate removed the direct pod-IP ingress, so the router is the
// only path to the harness. An actor that is not RUNNING after a resume is a loud error rather than a
// handle the Placer would dial only to have the router resume it behind the backend's back.
func (b *Backend) incarnation(uid string, info ActorInfo) (api.Incarnation, error) {
	if info.Status != StatusRunning {
		return api.Incarnation{}, fmt.Errorf("substrate: actor %q is %v after resume, want running", uid, info.Status)
	}
	return api.Incarnation{
		ID:           uid,
		Worker:       info.Worker,
		Address:      b.router,
		CallMetadata: map[string]string{TargetActorHeader: b.atespace + "/" + uid},
		Runtime:      "substrate",
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
	case errors.Is(err, errCreateRequiresSessionSpec), errors.Is(err, errCreateRequiresSessionUID):
		return "invalid_spec"
	case errors.Is(err, ErrSnapshotSuperseded):
		return "snapshot_superseded"
	default:
		return "runtime_operation_failed"
	}
}
