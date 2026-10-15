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

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/observability"
)

// ActorStatus is substrate's actor lifecycle, narrowed to what the backend maps.
type ActorStatus int

const (
	StatusUnknown ActorStatus = iota
	StatusRunning
	StatusSuspended
	StatusTerminated
	// StatusTransitioning is an actor on its way between states (resuming, suspending, pausing),
	// usually because another host or the router is acting on it. It settles on its own, so the
	// backend reports it as ErrActorBusy rather than as a permanent failure.
	StatusTransitioning
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
	// UID is the actor's server-assigned identity (substrate ResourceMetadata.uid). It is fixed for the
	// actor's lifetime, suspends and resumes included, and differs for an actor created later under the
	// same name, which is how the backend tells a replacement apart. A REQUIRES_MEMORY_SNAPSHOT harness
	// is placed only on an actor that reports one.
	UID string
}

// ControlClient is the subset of agent-substrate's ateapi.Control gRPC that the backend uses. A
// real substrate deployment implements this over pkg/proto/ateapipb; keeping it an interface here
// is what keeps agentsessions free of a substrate dependency.
type ControlClient interface {
	// CreateActor creates a SUSPENDED actor holding no snapshot of its own. A name that is already
	// taken must be reported as (or wrap) ErrActorExists.
	CreateActor(ctx context.Context, actor ActorRef, template ObjectRef) error
	// ResumeActor schedules the actor onto a worker. Substrate picks the source itself: the actor's
	// own snapshot if it has one, else the template's golden snapshot (a freshly started harness),
	// else a cold boot from the template spec. There is no per-call boot flag.
	ResumeActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
	// SuspendActor snapshots the actor to durable storage and frees the worker, returning the actor
	// as the suspend left it: ActorInfo.Snapshot is the external snapshot it now holds, and
	// ActorInfo.UID tells which actor under the name was suspended. UID must be set: a memory-tier
	// Snapshot refuses a suspend that reports none, since it cannot tell which actor it captured.
	// An actor that does not exist must be reported as (or wrap) ErrActorNotFound.
	SuspendActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
	DeleteActor(ctx context.Context, actor ActorRef) error
	GetActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
}

// ErrActorNotFound is what a ControlClient must return (or wrap) from GetActor and SuspendActor when
// the actor does not exist, and should from ResumeActor. After a resume that reports it, Restore
// asks GetActor and reports a memory-tier session's state lost unless the actor still holds the
// snapshot being restored, so a replacement created between the two calls does not hide the loss.
// It is a sentinel rather than a transport code so the seam stays neutral: the backend decides
// "create it" vs "attach to it", and whether a memory-tier session's in-RAM state is gone, without
// knowing that the client underneath speaks gRPC.
var ErrActorNotFound = errors.New("substrate: actor not found")

// ErrActorExists is what a ControlClient must return (or wrap) from CreateActor when an actor of that
// name already exists. Create treats it as another host having placed the session first, and
// attaches to that actor instead of failing the turn.
var ErrActorExists = errors.New("substrate: actor already exists")

// ErrActorBusy is what a ControlClient must return (or wrap) when substrate refuses an operation
// because another one holds the actor (substrate reports ABORTED: another host is resuming or
// suspending it). The backend also returns it for an actor it finds mid-transition, and for one
// replaced or deleted while Create was attaching to it. It wraps api.ErrComputeBusy, which the
// session service reports as ABORTED, so the caller retries once the actor settles.
var ErrActorBusy = fmt.Errorf("substrate: actor is busy with another operation: %w", api.ErrComputeBusy)

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
//     For a REQUIRES_MEMORY_SNAPSHOT harness whose session already has started turns on this
//     harness (SessionSpec.StartedTurns, counted after GetActor reports the actor missing), that is
//     exactly wrong: the state those turns built lived only in the missing actor's RAM, so Create
//     refuses with ErrMemoryStateLost and creates nothing.
//   - RUNNING — attach. Return the actor's incarnation without touching it. This covers the second
//     and later turns of a session, and the first turn of a FORKED CHILD, whose actor was created
//     from the parent's snapshot and resumed before it was ever handed out.
//   - SUSPENDED — resume, which restores the actor's own snapshot, so its RAM is what comes back.
//
// When CreateActor finds the name taken (ErrActorExists), another host placed the session between
// this call's GetActor and its CreateActor, so Create attaches to or resumes that actor as above.
//
// For a REQUIRES_MEMORY_SNAPSHOT harness the incarnation carries the actor's UID as ComputeInstance,
// and the host refuses the turn if an earlier turn recorded another one (api.ErrMemoryStateLost).
// That catches an actor created under the session's name after the actor its turns ran on was lost:
// a host that counted no started turns can create one before another host's first turn is journaled,
// and the count above cannot see that turn.
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
	if inc, found, err := b.attach(ctx, ref); found || err != nil {
		return inc, err
	}

	// The actor is gone (deleted out of band, or the backend now points at another atespace) and no
	// snapshot or tag of it is reachable from here. A stateless harness rebuilds from the journal; a
	// memory-tier one would silently continue the journal with none of the state it recorded. Count
	// the turns only now that the actor is known to be missing: every turn that ran on it was journaled
	// before it ran, so a turn another host journaled before this point is counted too. One journaled
	// after it is left to the host's compute-instance check (see the doc comment above).
	if b.descriptor.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot && s.StartedTurns != nil {
		turns, err := s.StartedTurns()
		if err != nil {
			return api.Incarnation{}, fmt.Errorf("substrate: count the session's started turns: %w", err)
		}
		if turns > 0 {
			return api.Incarnation{}, fmt.Errorf("%w: session %q has %d started turn(s) on harness %q but actor %s/%s does not exist, "+
				"so its in-RAM state cannot be reconstructed; no actor was created",
				ErrMemoryStateLost, sessionUID, turns, b.descriptor.ID, ref.Atespace, ref.Name)
		}
	}

	createFinished := observability.StartDebug(ctx, b.logger, "runtime.substrate", "create_actor",
		"actor", ref.Name,
		"atespace", ref.Atespace,
	)
	if err := b.ctl.CreateActor(ctx, ref, b.template); err != nil {
		if errors.Is(err, ErrActorExists) {
			// Another host created the actor after this call looked. Use it as that host does; the
			// host's compute-instance check still refuses it if it replaced a lost actor.
			createFinished(nil, "actor_exists", true)
			inc, found, aerr := b.attach(ctx, ref)
			if !found && aerr == nil {
				// Deleted again since. A retry looks afresh, and refuses a lost memory-tier session.
				aerr = fmt.Errorf("%w: actor %s/%s already existed at create and is now gone: %w", ErrActorBusy, ref.Atespace, ref.Name, err)
			}
			return inc, aerr
		}
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

// attach returns the incarnation of the session's existing actor: a RUNNING actor as it is, a
// SUSPENDED one after resuming it, which restores its own snapshot. found is false, with no error,
// when the actor does not exist, including when it is deleted between the look and the resume (see
// resumeFailed). An actor mid-transition is ErrActorBusy, as is a resume substrate refuses because
// another operation holds the actor (the ControlClient wraps it), so a host that lost a race for a
// session's first turn gets a retryable error, not a permanent one.
func (b *Backend) attach(ctx context.Context, ref ActorRef) (inc api.Incarnation, found bool, err error) {
	info, err := b.ctl.GetActor(ctx, ref)
	switch {
	case errors.Is(err, ErrActorNotFound):
		return api.Incarnation{}, false, nil
	case err != nil:
		return api.Incarnation{}, false, fmt.Errorf("substrate: get actor: %w", err)
	case info.Status == StatusRunning:
	case info.Status == StatusSuspended:
		seen := info
		if info, err = b.ctl.ResumeActor(ctx, ref); err != nil {
			return b.resumeFailed(ctx, ref, seen, err)
		}
	case info.Status == StatusTransitioning:
		// Usually another host's Create is resuming the actor it just made. Retrying finds it
		// RUNNING (or SUSPENDED); placing onto it now would race that operation.
		return api.Incarnation{}, true, fmt.Errorf("%w: actor %q is mid-transition", ErrActorBusy, ref.Name)
	default:
		return api.Incarnation{}, true, fmt.Errorf("substrate: actor %q is in state %v, not placeable", ref.Name, info.Status)
	}
	inc, err = b.incarnation(ref.Name, info)
	return inc, true, err
}

// resumeFailed classifies a failed resume of the SUSPENDED actor attach found (seen). A resume that
// reports the actor missing (ErrActorNotFound) is checked with GetActor, because the actor can be
// deleted, or deleted and created again, between the look and the resume, and substrate also
// answers NOT_FOUND for an actor that exists (a template volume it cannot find). Gone now, it is
// reported as not found, so Create treats it as any missing actor: a memory-tier session with
// started turns is refused with ErrMemoryStateLost, anything else gets a new actor. Replaced (another
// UID), the resume is ErrActorBusy, which is retryable: the next attempt attaches to the replacement,
// which the host refuses if it recorded another UID. Otherwise the resume error stands.
func (b *Backend) resumeFailed(ctx context.Context, ref ActorRef, seen ActorInfo, resumeErr error) (api.Incarnation, bool, error) {
	if errors.Is(resumeErr, ErrActorNotFound) {
		now, err := b.ctl.GetActor(ctx, ref)
		switch {
		case errors.Is(err, ErrActorNotFound):
			return api.Incarnation{}, false, nil
		case err == nil && now.UID != seen.UID:
			return api.Incarnation{}, true, fmt.Errorf("%w: actor %s/%s was replaced while it was being resumed: %w",
				ErrActorBusy, ref.Atespace, ref.Name, resumeErr)
		}
	}
	return api.Incarnation{}, true, fmt.Errorf("substrate: resume suspended actor: %w", resumeErr)
}

// Snapshot suspends the actor: RAM+disk snapshot to external storage, worker freed. Only the
// external (cold/suspend) kind is supported; substrate has no node-local warm checkpoint API today.
//
// When in.ComputeInstance is set (Placer.Suspend and a stateful fork pass the UID the journal last
// recorded), the actor under the name must be that one. Snapshot checks it before suspending, so a
// replacement is refused with api.ErrMemoryStateLost untouched, and again on the actor the suspend
// reports, since the actor can be replaced between the look and the suspend; substrate's
// SuspendActor has no UID precondition to close that window. In the second case the replacement is
// left suspended, which costs nothing: it holds none of the session's state and the host refuses it
// on every later turn. An actor that is gone is refused with api.ErrMemoryStateLost as well, since
// the state it held went with it. An actor that reports no UID is refused with a plain error: it
// cannot be compared, and that is a ControlClient fault, not evidence of a replacement. A
// REQUIRES_MEMORY_SNAPSHOT backend refuses a suspend that reports no UID even when no instance is
// recorded (a fork child before its first turn).
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
	if in.ComputeInstance != "" {
		info, err := b.ctl.GetActor(ctx, b.ref(in.ID))
		if err != nil {
			err = b.instanceGone(in, "get actor before snapshot", err)
			suspendFinished(err, "error_kind", snapshotErrorKind(err, "get_actor_failed"))
			return api.SnapshotRef{}, err
		}
		if err := b.requireInstance(in, info.UID); err != nil {
			suspendFinished(err, "error_kind", snapshotErrorKind(err, "actor_uid_missing"))
			return api.SnapshotRef{}, err
		}
	}
	info, err := b.ctl.SuspendActor(ctx, b.ref(in.ID))
	if err != nil {
		err = b.instanceGone(in, "suspend actor", err)
		suspendFinished(err, "error_kind", snapshotErrorKind(err, "suspend_actor_failed"))
		return api.SnapshotRef{}, err
	}
	if err := b.requireInstance(in, info.UID); err != nil {
		suspendFinished(err, "error_kind", snapshotErrorKind(err, "actor_uid_missing"))
		return api.SnapshotRef{}, err
	}
	suspendFinished(nil)
	uri := info.Snapshot
	// Local carries the actor name (the handle Restore/ResumeActor needs); ExternalURI carries the
	// external snapshot the suspend produced. Fork clones by tagging the ACTOR, not this handle, and
	// uses the handle to check that the actor still holds this exact snapshot when it is tagged.
	snapshot = api.SnapshotRef{Local: in.ID, ExternalURI: uri, Memory: true}
	return snapshot, nil
}

// instanceGone wraps err, from the control call op, for Snapshot. When the actor is not found and
// in requires a compute instance, or the harness is REQUIRES_MEMORY_SNAPSHOT (a fork child before
// its first turn has no recorded instance yet), the actor that held the session's in-RAM state is
// gone for good, so the result wraps api.ErrMemoryStateLost as well as err, the way Restore reports
// a missing memory-tier actor; any other error is only wrapped.
func (b *Backend) instanceGone(in api.Incarnation, op string, err error) error {
	memory := in.ComputeInstance != "" || b.descriptor.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot
	if !memory || !errors.Is(err, ErrActorNotFound) {
		return fmt.Errorf("substrate: %s: %w", op, err)
	}
	instance := ""
	if in.ComputeInstance != "" {
		instance = fmt.Sprintf(", instance %q", in.ComputeInstance)
	}
	return fmt.Errorf("%w: actor %s/%s%s, which held the session's in-RAM state, no longer exists, "+
		"so that state cannot be captured; nothing was snapshotted for the session: %s: %w",
		api.ErrMemoryStateLost, b.atespace, in.ID, instance, op, err)
}

// snapshotErrorKind is the debug error_kind for a failed step of Snapshot: memory_state_lost when
// err wraps api.ErrMemoryStateLost, else the step's own kind.
func snapshotErrorKind(err error, step string) string {
	if errors.Is(err, api.ErrMemoryStateLost) {
		return "memory_state_lost"
	}
	return step
}

// requireInstance returns an error wrapping api.ErrMemoryStateLost when in requires a compute
// instance and the actor under in.ID reports another UID. An actor that reports no UID cannot be
// compared at all, which is a ControlClient that breaks its contract rather than a replacement, so
// that is a plain error. A REQUIRES_MEMORY_SNAPSHOT backend refuses a missing UID even when in
// records no instance (a fork child before its first turn): the ControlClient contract requires
// one, and the snapshot would otherwise be taken of an actor nobody could identify.
func (b *Backend) requireInstance(in api.Incarnation, uid string) error {
	memory := in.ComputeInstance != "" || b.descriptor.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot
	if !memory {
		return nil
	}
	if uid == "" {
		why := "so the snapshot of the session's in-RAM state could not be tied to the actor that took it"
		if in.ComputeInstance != "" {
			why = fmt.Sprintf("so it could not be checked against instance %q, which holds the session's in-RAM state", in.ComputeInstance)
		}
		return fmt.Errorf("substrate: actor %s/%s reported no UID, %s; no checkpoint was recorded for the session",
			b.atespace, in.ID, why)
	}
	if in.ComputeInstance == "" || uid == in.ComputeInstance {
		return nil
	}
	return fmt.Errorf("%w: actor %s/%s is instance %q, but the session's turns ran on instance %q, which has been replaced, "+
		"so its in-RAM state cannot be captured; nothing was snapshotted for the session",
		api.ErrMemoryStateLost, b.atespace, in.ID, uid, in.ComputeInstance)
}

// Restore brings the actor back from its snapshot onto a (possibly different) worker.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	return b.RestoreInstance(ctx, ref, nil)
}

// RestoreInstance is Restore with a reader for the UID of the actor the session's journal last
// recorded (api.InstanceRestorer); nil reads none. After a failed resume, that UID is what tells the
// session's actor, still there, from a replacement when ref carries no snapshot handle to compare
// (see memoryActorGone).
func (b *Backend) RestoreInstance(ctx context.Context, ref api.SnapshotRef, recorded func() (string, error)) (inc api.Incarnation, err error) {
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
		if lost := b.memoryActorGone(ctx, ref, recorded, err); lost != nil {
			return api.Incarnation{}, lost
		}
		return api.Incarnation{}, fmt.Errorf("substrate: resume (restore) actor: %w", err)
	}
	resumeFinished(nil, "actor_status", info.Status)
	inc, err = b.incarnation(name, info)
	return inc, err
}

// memoryActorGone reports, after resumeErr, that the actor holding a REQUIRES_MEMORY_SNAPSHOT
// session's state is gone, as ErrMemoryStateLost wrapping resumeErr too: the snapshot Restore would
// resume belonged to that actor, so the state went with it and there is nothing to retry. It asks
// GetActor, whose ControlClient contract reports a missing actor as ErrActorNotFound. The actor is
// gone when GetActor finds none. Substrate also answers a resume with NOT_FOUND for an actor that
// still exists (a template volume it cannot find), so after a resume that reported the actor missing
// (ErrActorNotFound), the actor GetActor finds is compared with the session's record: with a
// snap.ExternalURI, an actor that does not hold it is gone (a replacement created between the two
// calls holds no snapshot of the session's, and the same actor suspended again since, by another
// host, holds a newer one); without one (no SUSPEND recorded), an actor whose UID is not the one the
// journal last recorded, read through recorded after GetActor answers, is a replacement. An actor
// that matches, or one there is nothing to compare with (no ExternalURI, and no recorded UID or no
// way to read it), keeps the resume error. It returns nil for a stateless harness, whose state is in
// the journal, and when GetActor or recorded fails, so the caller keeps the resume error.
func (b *Backend) memoryActorGone(ctx context.Context, snap api.SnapshotRef, recorded func() (string, error), resumeErr error) error {
	if b.descriptor.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		return nil
	}
	ref := b.ref(snap.Local)
	info, err := b.ctl.GetActor(ctx, ref)
	switch {
	case errors.Is(err, ErrActorNotFound):
	case err != nil:
		return nil
	case !errors.Is(resumeErr, ErrActorNotFound):
		return nil
	case snap.ExternalURI != "" && info.Snapshot != snap.ExternalURI:
		return fmt.Errorf("%w: actor %s/%s of session %q on harness %q no longer holds the snapshot the session was suspended to "+
			"(replaced, or suspended again since), so its in-RAM state cannot be restored; no actor was created: resume: %w",
			ErrMemoryStateLost, ref.Atespace, ref.Name, snap.Local, b.descriptor.ID, resumeErr)
	case snap.ExternalURI == "" && recorded != nil:
		instance, err := recorded()
		if err != nil || instance == "" || info.UID == instance {
			return nil
		}
		return fmt.Errorf("%w: actor %s/%s of session %q on harness %q has UID %q, not the %q the session's turns ran on "+
			"(replaced), so its in-RAM state cannot be restored; no actor was created: resume: %w",
			ErrMemoryStateLost, ref.Atespace, ref.Name, snap.Local, b.descriptor.ID, info.UID, instance, resumeErr)
	default:
		return nil
	}
	return fmt.Errorf("%w: actor %s/%s of session %q on harness %q does not exist, so its in-RAM state cannot be restored; no actor was created: resume: %w",
		ErrMemoryStateLost, ref.Atespace, ref.Name, snap.Local, b.descriptor.ID, resumeErr)
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
// It wraps api.ErrSnapshotSuperseded, which the session service reports as ABORTED.
var ErrSnapshotSuperseded = fmt.Errorf("substrate: parent snapshot was superseded before the fork could tag it: %w", api.ErrSnapshotSuperseded)

// ErrMemoryStateLost is returned by Create when a REQUIRES_MEMORY_SNAPSHOT session that already has a
// started turn on this harness has no actor to attach to or resume. Its state existed only in that
// actor's RAM, so a new actor from the template would continue the journal with a harness that
// remembers none of it. Restore returns it for every such session whose actor is gone, whatever its
// journal records (an interrupted first turn included), since the snapshot went with the actor.
// It wraps api.ErrMemoryStateLost, which the session service reports as FAILED_PRECONDITION.
var ErrMemoryStateLost = fmt.Errorf("substrate: refusing to cold-create an actor: %w", api.ErrMemoryStateLost)

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
			// The parent may have been deleted, or woken and suspended again, while the tag ran:
			// substrate reports a missing source as FAILED_PRECONDITION, which says nothing typed.
			// Read the parent again so those are api.ErrMemoryStateLost and ErrSnapshotSuperseded,
			// as on either side of the tag.
			if gone := b.requireSnapshot(ctx, parent, ref.ExternalURI); errors.Is(gone, api.ErrMemoryStateLost) || errors.Is(gone, ErrSnapshotSuperseded) {
				return api.Incarnation{}, fmt.Errorf("%w; the tag of parent %q failed: %v", gone, parent.Name, err)
			}
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
// snapshot. A parent that no longer exists took the snapshot with it, so that is
// api.ErrMemoryStateLost: no child can be cloned from it, and retrying cannot help.
func (b *Backend) requireSnapshot(ctx context.Context, actor ActorRef, snapshot string) error {
	info, err := b.ctl.GetActor(ctx, actor)
	if errors.Is(err, ErrActorNotFound) {
		return fmt.Errorf("%w: fork parent %s/%s no longer exists, so the snapshot it held cannot be cloned; no child was created: %w",
			api.ErrMemoryStateLost, actor.Atespace, actor.Name, err)
	}
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
//
// For a REQUIRES_MEMORY_SNAPSHOT harness the actor's UID becomes the incarnation's ComputeInstance,
// which the host records and compares per turn; an actor that reports no UID is refused, since the
// host could not tell it from a replacement. A stateless harness's state is in the journal, so a
// replaced actor is fine and no instance is set.
func (b *Backend) incarnation(uid string, info ActorInfo) (api.Incarnation, error) {
	if info.Status != StatusRunning {
		return api.Incarnation{}, fmt.Errorf("substrate: actor %q is %v after resume, want running", uid, info.Status)
	}
	instance := ""
	if b.descriptor.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot {
		if info.UID == "" {
			return api.Incarnation{}, fmt.Errorf("substrate: actor %s/%s reported no UID, so a replacement could not be told apart "+
				"from the actor that holds the session's in-RAM state", b.atespace, uid)
		}
		instance = info.UID
	}
	return api.Incarnation{
		ID:           uid,
		Worker:       info.Worker,
		Address:      b.router,
		CallMetadata: map[string]string{TargetActorHeader: b.atespace + "/" + uid},
		Runtime:      "substrate",
		// The router reaches the actor by name, so this records which actor the name resolved to
		// when the backend looked, not which one the router reaches later.
		ComputeInstance: instance,
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
	case errors.Is(err, api.ErrMemoryStateLost):
		return "memory_state_lost"
	case errors.Is(err, api.ErrComputeBusy):
		return "actor_busy"
	default:
		return "runtime_operation_failed"
	}
}
