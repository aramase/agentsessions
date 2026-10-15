// Package placement wires the Sessions layer to the Runtime compute SPI. The Placer owns the
// incarnation lifecycle for a session: it drives Runtime.Create, mints the fence from the log (the
// single fence authority), stamps it on the incarnation, and binds a controller to that fence. Keeping
// this seam out of session.Service keeps the gRPC layer thin and the SPI logic unit-testable, and lets
// cmd/agentnode reuse it.
package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/eventlog"
	"github.com/agent-primitives/sessions/harnesswire"
	"github.com/agent-primitives/sessions/observability"
)

// ErrUnplaceable is returned when a harness requires a capability the chosen backend cannot provide
// (e.g. REQUIRES_MEMORY_SNAPSHOT on a filesystem-only pod). The gate runs BEFORE Create, so an
// unplaceable harness never provisions compute or writes to the log; for a LiveDescriber backend it
// runs again on the turn's own connection, still before anything is written to the log.
// session.Service surfaces it as codes.FailedPrecondition.
var ErrUnplaceable = errors.New("placement: harness cannot be placed on this runtime")

// ErrSessionBusy is returned when Exec, Suspend, Resume, Fork, or a guarded callback overlaps
// another operation on the same session through this Placer or its Registry.
// No compute or journal transition is attempted;
// session.Service surfaces it as codes.Aborted, independently of any backend transport status.
var ErrSessionBusy = errors.New("placement: another operation is in progress for this session")

// ErrDescriptorMismatch is returned when the harness reports a descriptor id other than the one the
// Placer expects (WithDescriptorID): the address reaches a different harness than the one that was
// configured. Like ErrUnplaceable it is raised by the admission check, before any compute is
// provisioned or anything is written to the log, and again on the turn's own connection for a
// LiveDescriber backend. session.Service surfaces it as codes.FailedPrecondition.
var ErrDescriptorMismatch = errors.New("placement: harness is not the expected one")

// ErrHarnessUnavailable is returned when the harness could not be reached to describe itself during
// admission or the controller's connected-harness identity check. Neither check runs the harness or
// appends journal records; the connected check may follow compute allocation and fence minting.
// session.Service surfaces it as codes.Unavailable. If the caller's own deadline or cancellation
// ended the check, it is ErrAdmissionInterrupted instead: the same expired call cannot succeed.
var ErrHarnessUnavailable = errors.New("placement: harness unavailable")

// describeTimeout bounds how long admission waits for a harness to describe itself. Describe is a
// small unary call that every harness answers without doing any work, so a harness that has not
// answered in this long is treated as unavailable rather than left to hold the call.
var describeTimeout = 10 * time.Second

// ErrAdmissionInterrupted is returned when the caller's own deadline or cancellation ended a
// Describe check before the harness answered. It also wraps the context's error, so errors.Is
// reports context.DeadlineExceeded or context.Canceled. Like ErrHarnessUnavailable it is only raised
// before the harness runs or journal records are appended; session.Service surfaces it as
// codes.DeadlineExceeded or codes.Canceled. An interruption during Run does not wrap it.
var ErrAdmissionInterrupted = errors.New("placement: admission interrupted")

// ErrStatefulOverride is the memory-tier override rule: a turn on a harness other than the
// session's own (an ExecRequest.harness override, or a recovery of a turn recorded on one; see
// WithOverride) is refused when either harness is REQUIRES_MEMORY_SNAPSHOT. Suspend and Fork follow
// the session's own harness, so they would neither checkpoint nor clone the state a memory-tier
// override leaves in RAM; and a memory-tier session's RAM would miss the override's turn while an
// approval park of the override reported the whole session cold. The Placer checks the override's
// half on the descriptor it admits, before any compute is provisioned or anything is written to the
// log, and again on the turn's own connection for a LiveDescriber backend; session.Service checks
// the session harness's half before routing. Both surface as codes.FailedPrecondition.
var ErrStatefulOverride = errors.New("placement: a per-turn harness override cannot involve a REQUIRES_MEMORY_SNAPSHOT harness")

// Backend is the compute Runtime the Placer drives. It exposes the harness DESCRIPTOR so the Placer
// can gate CanPlace before Create (placement must not provision compute to learn a harness's needs);
// the harness itself is reached by dialing Incarnation.Address with Incarnation.CallMetadata on every
// call (Harness.Connect) — the one dial path both runtime/local and substrate use. runtime/local
// satisfies this.
type Backend interface {
	api.Runtime
	Describe(ctx context.Context) (api.Descriptor, error)
}

// LiveDescriber is implemented by a Backend whose Describe asks whichever harness is answering at an
// address, rather than returning a declaration fixed when the backend was built (runtime/remote).
//
// The Placer runs a turn over its own connection to Incarnation.Address, and that connection can
// reach a different process than the one Describe asked: another replica behind a load balancer or
// a Kubernetes Service, or a harness restarted in between. When DescribesLiveHarness reports true,
// the Placer therefore asks again on the connection that will run the turn and applies the same
// CanPlace gate before writing records. Exec and unchecked Resume do this before minting a fence;
// cursor-checked Resume has minted an early fence and retakes its writer fence after this gate.
// This narrows the window but does not close it; see recheck for the limit.
//
// A backend that reports true must provision nothing in Create and Restore: a refusal by the second
// check only closes the dial and never calls Stop, so anything provisioned would leak.
type LiveDescriber interface {
	DescribesLiveHarness() bool
}

// Placer owns the incarnation lifecycle: Create the compute, mint+bind the fence, drive the controller.
type Placer struct {
	backend      Backend
	model        controller.ModelFunc
	stream       controller.StreamFunc
	tool         controller.ToolFunc
	dial         Dialer
	logger       *slog.Logger
	descriptorID string

	// Private for a standalone Placer; NewRegistry and Registry.Add wire the Registry's shared
	// guard before use.
	guard *sessionGuard

	// sessions tracks open harness connections and checkpoints per session UID. New gives each
	// Placer its own; NewRegistry replaces it with one set shared by every Placer it routes to, so a
	// stateful fork's checkpoint taken through one harness's Placer also covers a turn another Placer
	// runs on the same session (ExecRequest.harness can differ from the session's default).
	sessions *sessionSet

	// registry is the Registry this Placer was given to, or nil for a standalone Placer. It is set
	// before the Placer is in use (NewRegistry, Registry.Add) and never changes, so Exec can resolve a
	// recorded harness name to the Placer that ran it.
	registry *Registry
}

// sessionSet is the per-session state a checkpoint coordinates on. It is keyed by session UID
// alone, so Placers that share one see each other's connections and checkpoints.
type sessionSet struct {
	mu   sync.Mutex
	live map[string]map[*liveHarness]struct{} // session UID -> open harness connections
	// checkpoints holds session UIDs fenced by a stateful Fork, Suspend or approval decision.
	// While a session is in it, no harness connection can be opened for the session and no
	// other operation can acquire its checkpoint mark.
	checkpoints map[string]struct{}
	// shared is set when a Registry owns the set, so a Placer cannot join two registries' sets.
	shared bool
}

// busy reports whether any session has an open harness connection or a checkpoint in progress.
// Entries are deleted when they drop to zero, so an idle set is empty.
func (s *sessionSet) busy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.live) > 0 || len(s.checkpoints) > 0
}

// Dialer opens a Harness.Connect client to the harness an incarnation names and returns a closer for
// the connection. runtime/local passes a unix-socket address (unix://…); substrate passes the
// atenet-router's host:port, dialed over h2c, plus CallMetadata naming the actor, which the client
// must attach to every call. The default dialer handles both forms; WithDialer overrides it (tests).
// This is the one transport seam the harness rides unchanged.
type Dialer func(inc api.Incarnation) (api.Harness, func() error, error)

// Option configures a Placer.
type Option func(*Placer)

// WithDialer overrides how the Placer reaches a harness (default: unix-socket dial for runtime/local).
func WithDialer(d Dialer) Option { return func(p *Placer) { p.dial = d } }

// ExecOption configures a single execution.
type ExecOption func(*execConfig)

type execConfig struct {
	harness       string
	observer      controller.Observer
	config        []byte
	resumeFromSeq int64
	deadline      time.Time
	override      bool
}

// WithHarness records the resolved registry name for this execution. Without a name, the
// controller uses the connected harness's descriptor ID. Names are per-call so registry aliases
// can share a Placer without changing one another's identity.
func WithHarness(name string) ExecOption {
	return func(c *execConfig) { c.harness = name }
}

// WithOverride marks the execution as running on a harness other than the session's own (an
// ExecRequest.harness override). The Placer then refuses a REQUIRES_MEMORY_SNAPSHOT harness with
// ErrStatefulOverride before it provisions anything: Suspend and Fork route by the session's own
// harness and would leave the state this turn builds unsnapshotted and uncloned.
func WithOverride() ExecOption {
	return func(c *execConfig) { c.override = true }
}

// ResumeOption configures one recovery attempt.
type ResumeOption func(*resumeConfig)

type resumeConfig struct {
	harness         string
	observer        controller.Observer
	expectedLastSeq *int64
	// override is set when the pending turn was recorded on a harness other than the session's
	// own; see WithOverride.
	override bool
}

// WithResumeObserver reports newly committed recovery records and streaming chunks, including
// SUSPEND on a second approval park. It does not re-emit previously recorded effects.
func WithResumeObserver(o controller.Observer) ResumeOption {
	return func(c *resumeConfig) { c.observer = o }
}

// WithResumeHarness identifies the registry entry selected for this recovery attempt. The
// controller checks it against the pending invocation before running the harness.
func WithResumeHarness(name string) ResumeOption {
	return func(c *resumeConfig) { c.harness = name }
}

// WithObserver relays a turn's records and streaming chunks as they happen, instead of leaving the
// caller to re-read the log once the turn is over.
func WithObserver(o controller.Observer) ExecOption {
	return func(c *execConfig) { c.observer = o }
}

// WithStart carries the caller's opaque per-execution config and resume cursor to the harness.
func WithStart(config []byte, resumeFromSeq int64) ExecOption {
	return func(c *execConfig) {
		c.config = config
		c.resumeFromSeq = resumeFromSeq
	}
}

// WithDeadline bounds the execution. It applies to the whole turn, including the model call, which
// is what makes it enforceable at all: the host owns that call, so cancelling the turn cancels the
// request in flight rather than leaving it to finish unobserved.
func WithDeadline(t time.Time) ExecOption {
	return func(c *execConfig) { c.deadline = t }
}

// controllerOpts is the shared controller configuration, so the exec and resume paths cannot drift
// apart on fencing, logging, or which model and tool executor they drive.
func (p *Placer) controllerOpts(fence int64, sessionUID string, observer controller.Observer) []controller.Option {
	opts := []controller.Option{
		controller.WithFence(fence),
		controller.WithLogger(p.logger),
		controller.WithSessionUID(sessionUID),
		controller.WithObserver(observer),
	}
	if p.stream != nil {
		opts = append(opts, controller.WithStreamingModel(p.stream))
	}
	if p.tool != nil {
		opts = append(opts, controller.WithToolExecutor(p.tool))
	}
	return opts
}

// computeInstance binds the controller to the compute instance inc was placed on, attributing a
// recorded turn to this Placer as startedTurns does (ranHere), so the controller refuses compute that
// replaced the instance this harness's earlier turns ran on.
func (p *Placer) computeInstance(inc api.Incarnation) controller.Option {
	return controller.WithComputeInstance(inc.ComputeInstance, p.ranHere)
}

// WithStreamingModel supplies a model that reports partial output, which the controller relays as
// ephemeral deltas. Without it a turn still runs; the caller just sees the finalized output only.
func WithStreamingModel(fn controller.StreamFunc) Option { return func(p *Placer) { p.stream = fn } }

// WithToolExecutor supplies the host executor for controller-mediated tools in Exec and Resume.
// It receives the session UID bound by the Placer. Without one, tool calls fail closed. The host
// owns session-scoped authorization and durable deduplication by session UID plus idempotency key.
func WithToolExecutor(fn controller.ToolFunc) Option { return func(p *Placer) { p.tool = fn } }

// WithLogger enables structured operational logs. Message contents and fence tokens are never logged.
func WithLogger(logger *slog.Logger) Option { return func(p *Placer) { p.logger = logger } }

// WithDescriptorID makes the Placer refuse a harness whose Describe reports a HarnessDescriptor.id
// other than id, with ErrDescriptorMismatch, wherever it applies the CanPlace gate: Exec, Resume and
// Fork, and the turn's own connection for a LiveDescriber backend. Empty, the default, means the id
// is not checked.
//
// The id is what the harness reports about itself, so the check catches an address that reaches the
// wrong harness, not a harness that lies about what it is. It is not authentication.
func WithDescriptorID(id string) Option { return func(p *Placer) { p.descriptorID = id } }

// DescriptorID returns the id set with WithDescriptorID, or "" when the Placer does not check it.
func (p *Placer) DescriptorID() string { return p.descriptorID }

// New builds a Placer over a compute backend and the live model.
func New(backend Backend, model controller.ModelFunc, opts ...Option) *Placer {
	p := &Placer{
		backend:  backend,
		model:    model,
		dial:     DefaultDial,
		logger:   slog.New(slog.DiscardHandler),
		guard:    new(sessionGuard),
		sessions: &sessionSet{},
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.New(slog.DiscardHandler)
	}
	return p
}

// trySessionLock serializes mutations through their compute and journal transitions.
// Refuse overlap rather than queueing a request whose cursor may be stale by the time it runs.
func (p *Placer) trySessionLock(sessionUID string) (func(), error) {
	return p.guard.tryLock(sessionUID)
}

// RuntimeCapabilities reports the backend's compute capabilities without contacting the harness.
func (p *Placer) RuntimeCapabilities() api.RuntimeCapabilities {
	return p.backend.Capabilities()
}

// Describe asks the placed harness for its descriptor, the same call Exec gates placement on. It
// provisions nothing.
func (p *Placer) Describe(ctx context.Context) (api.Descriptor, error) {
	return p.backend.Describe(ctx)
}

// Exec rejects an owned unreceipted approval before runtime admission; otherwise it creates an
// incarnation and drives the harness under a log-minted fence stamped on the returned incarnation.
// A new approval gate closes the dial, snapshots and commits SUSPEND under that same fence before
// returning *api.ApprovalParkedError. Handoff failures return the actual operational cause.
func (p *Placer) Exec(ctx context.Context, log eventlog.Store, sessionUID string, inputs []api.Message, expectedLastSeq int64, opts ...ExecOption) (inc api.Incarnation, err error) {
	var cfg execConfig
	for _, o := range opts {
		o(&cfg)
	}
	if !cfg.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, cfg.deadline)
		defer cancel()
	}
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "exec",
		"session_uid", sessionUID,
		"expected_last_seq", expectedLastSeq,
		"input_count", len(inputs),
	)
	defer func() {
		finish(err, "error_kind", placementErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return api.Incarnation{}, err
	}
	defer release()

	if err := eventlog.CheckWritable(log); err != nil {
		return api.Incarnation{}, err
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil {
		return api.Incarnation{}, err
	}
	if state != nil && !state.Inherited && state.Receipt == nil {
		return api.Incarnation{}, controller.ErrApprovalBlocked
	}
	desc, err := p.admit(ctx, sessionUID)
	if err != nil {
		return api.Incarnation{}, err
	}
	if err := refuseStatefulOverride(desc, cfg.override); err != nil {
		return api.Incarnation{}, err
	}

	// Refuse before provisioning: on substrate, Create on an actor a stateful fork is checkpointing
	// would either fail or wake it. openHarness re-checks atomically; this only saves the round trip.
	if err = p.refuseDuringCheckpoint(sessionUID); err != nil {
		return api.Incarnation{}, err
	}
	// The backend needs the started-turn count to tell a new session from one whose compute is gone:
	// a memory-tier harness can be cold-started only for the former (api.ErrMemoryStateLost). Only
	// turns this Placer ran count: a turn ExecRequest.harness sent to another harness left no state in
	// this one's compute. The backend counts when it has seen the compute is missing, not here, so a
	// turn another host journals in between is not missed (see api.SessionSpec.StartedTurns).
	spec := &api.SessionSpec{SessionUID: sessionUID, StartedTurns: func() (int, error) {
		return p.startedTurns(log)
	}}
	createFinished := observability.StartDebug(ctx, p.logger, "placement", "create_compute",
		"session_uid", sessionUID)
	inc, err = p.backend.Create(ctx, spec)
	if err != nil {
		createFinished(err, "error_kind", "runtime_create_failed")
		return inc, err
	}
	createFinished(nil, "incarnation_id", inc.ID, "runtime", inc.Runtime)

	dialFinished := observability.StartDebug(ctx, p.logger, "placement", "dial_harness",
		"session_uid", sessionUID,
		"incarnation_id", inc.ID,
		"runtime", inc.Runtime,
		"transport", addressTransport(inc.Address),
	)
	operationCtx := ctx
	ctx, live, err := p.openHarness(ctx, sessionUID, inc)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return inc, err
	}
	dialFinished(nil)
	defer func() {
		// Ordinary cleanup logs close failures without replacing the turn outcome.
		_ = p.releaseHarness(ctx, sessionUID, inc.ID, live)
	}()
	har := live.harness
	closeInvocation := func() error {
		if err := live.checkSuperseded(ctx); err != nil {
			return err
		}
		return p.releaseHarness(ctx, sessionUID, inc.ID, live)
	}
	if err := p.recheck(ctx, sessionUID, har, cfg.override); err != nil {
		return inc, err
	}

	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := log.NewFence()
	live.fenceMinted()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return inc, err
	}
	fenceFinished(nil)
	inc.FenceToken = fence // Placer-owned: the incarnation carries the token Suspend/Resume will need
	// A checkpoint may have begun while the fence was being minted. Stop before the controller writes
	// anything under a fence the checkpoint's does not cover.
	if err := live.checkSuperseded(ctx); err != nil {
		return inc, err
	}
	copts := append(p.controllerOpts(fence, sessionUID, cfg.observer),
		controller.WithStart(cfg.config, cfg.resumeFromSeq),
		controller.WithHarness(cfg.harness),
		p.computeInstance(inc))
	c, err := controller.New(log, p.model, copts...)
	if err != nil {
		return inc, err
	}
	if err := c.Exec(ctx, har, inputs, expectedLastSeq); err != nil {
		var park *api.ApprovalParkedError
		if errors.As(err, &park) && api.IsParkUnwind(err, park.Ref) {
			return inc, p.handoffPark(operationCtx, log, sessionUID, fence, park, closeInvocation, cfg.observer)
		}
		return inc, turnError(ctx, err)
	}
	return inc, nil
}

// admit is the placement gate (honest degradation), shared by every path that drives a harness:
// Exec, Resume and Fork. It reads the harness descriptor and refuses a harness the backend cannot
// host BEFORE provisioning any compute or writing to the log, e.g. a REQUIRES_MEMORY_SNAPSHOT harness
// on a filesystem-only backend.
//
// It runs on every path, not once per session, because the descriptor is not guaranteed to be fixed:
// a backend that asks a live harness reports whatever harness is answering, and that can change
// between an interrupted turn and its Resume. Replaying into a harness the backend cannot host is the "resumed
// wrongly" case this gate exists to prevent.
func (p *Placer) admit(ctx context.Context, sessionUID string) (api.Descriptor, error) {
	return p.gate(ctx, sessionUID, "resolve_execution_path", p.backend.Describe)
}

// recheck repeats the gate on the connection that will run the turn, for a backend whose descriptor
// comes from a live harness (see LiveDescriber). It runs after Create and the dial and before the
// Exec fence is minted, so a refusal still leaves nothing on the log. A cursor-checked Resume
// has already minted an early fence; it retakes/checks its writer fence after this gate.
// The backend's Create and Restore
// provision nothing for such a backend, so there is no compute to release either.
//
// The check has a known limit. Capabilities are checked with a Describe call before the turn's
// Connect stream is opened, and the answer is not bound to that stream: the harness protocol has no
// way to tie the two together. gRPC can close the connection and dial again between the two calls,
// and a proxy can route each call to a different backend, so if a different harness takes over the
// same address between Describe and Connect, the turn runs on it and this check does not detect
// it. Registering a harness by address therefore assumes the operator controls what serves that
// address; docs/security.md states the same limit.
//
// override repeats the ErrStatefulOverride check on the descriptor the connection reports.
func (p *Placer) recheck(ctx context.Context, sessionUID string, har api.Harness, override bool) error {
	if l, ok := p.backend.(LiveDescriber); !ok || !l.DescribesLiveHarness() {
		return nil
	}
	desc, err := p.gate(ctx, sessionUID, "verify_execution_harness", har.Describe)
	if err == nil {
		err = refuseStatefulOverride(desc, override)
	}
	if err != nil && errors.Is(context.Cause(ctx), errSuperseded) {
		// A checkpoint of the session ended this turn's connection during the check. Report the
		// checkpoint, as turnError does for a running turn, and format the check's error with %v so
		// that ErrAdmissionInterrupted is not in the chain: the call is retryable (ABORTED), not
		// cancelled by its caller.
		return fmt.Errorf("%w (harness check ended with: %v)", errSuperseded, err)
	}
	return err
}

// gate describes the harness with describe, refuses one that is not the expected harness
// (WithDescriptorID), and applies CanPlace, mapping a harness that cannot be described to
// ErrHarnessUnavailable or ErrAdmissionInterrupted.
func (p *Placer) gate(ctx context.Context, sessionUID, op string, describe func(context.Context) (api.Descriptor, error)) (desc api.Descriptor, err error) {
	resolveFinished := observability.StartDebug(ctx, p.logger, "placement", op, "session_uid", sessionUID)
	// Bound the check on its own. A harness that accepts the connection and then never answers
	// Describe would otherwise hold a call with no deadline forever. WithTimeout keeps an earlier
	// caller deadline.
	dctx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()
	desc, err = describe(dctx)
	if err != nil {
		err = describeError(ctx, err, dctx.Err() != nil)
		resolveFinished(err, "error_kind", "describe_harness_failed")
		return api.Descriptor{}, err
	}
	// Identity before capabilities: a harness that is not the expected one is refused for that,
	// whatever it declares.
	if p.descriptorID != "" && desc.ID != p.descriptorID {
		err = fmt.Errorf("%w: expected descriptor id %q, the harness reports %q", ErrDescriptorMismatch, p.descriptorID, desc.ID)
		resolveFinished(err,
			"error_kind", "descriptor_mismatch",
			"decision", "refused",
			"harness_id", desc.ID,
			"expected_harness_id", p.descriptorID,
		)
		return api.Descriptor{}, err
	}
	if !controller.CanPlace(desc.Capabilities, p.backend.Capabilities()) {
		err = fmt.Errorf("%w: harness %q needs %s but the runtime provides MemorySnapshot=%v",
			ErrUnplaceable, desc.ID, desc.Capabilities.Resumability, p.backend.Capabilities().MemorySnapshot)
		resolveFinished(err,
			"error_kind", "unplaceable",
			"decision", "refused",
			"harness_id", desc.ID,
			"resumability", desc.Capabilities.Resumability,
			"runtime_memory_snapshot", p.backend.Capabilities().MemorySnapshot,
		)
		return api.Descriptor{}, err
	}
	resolveFinished(nil,
		"decision", "accepted",
		"harness_id", desc.ID,
		"resumability", desc.Capabilities.Resumability,
		"runtime_memory_snapshot", p.backend.Capabilities().MemorySnapshot,
	)
	return desc, nil
}

// refuseStatefulOverride returns ErrStatefulOverride for a REQUIRES_MEMORY_SNAPSHOT harness that
// runs as an override of the session's own harness (see WithOverride).
func refuseStatefulOverride(desc api.Descriptor, override bool) error {
	if !override || desc.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		return nil
	}
	return fmt.Errorf("%w: harness %q is %s and the session's own harness is another one; Suspend and Fork follow the session's harness and would not checkpoint or clone this harness's state",
		ErrStatefulOverride, desc.ID, desc.Capabilities.Resumability)
}

// controllerError maps only the controller's Describe boundary. Run, model and runtime errors
// with identical transport codes must keep their existing classification.
func controllerError(ctx context.Context, err error) error {
	if !errors.Is(err, controller.ErrHarnessDescribeFailed) {
		return err
	}
	return describeError(ctx, err, errors.Is(err, context.DeadlineExceeded))
}

// describeError is shared by admission and connected-harness identity checks.
func describeError(ctx context.Context, err error, timedOut bool) error {
	switch c := status.Code(err); {
	case ctx.Err() != nil:
		// The caller's cause wins over a peer timeout. Format the Describe error with %v so its
		// context error cannot change the service's cancellation/deadline classification.
		return fmt.Errorf("%w: describe harness: %w: %v", ErrAdmissionInterrupted, ctx.Err(), err)
	case timedOut || c == codes.Unavailable || c == codes.DeadlineExceeded:
		// A refused connection or a harness timeout while the caller is live is a retryable outage.
		return fmt.Errorf("%w: %w", ErrHarnessUnavailable, err)
	default:
		return err
	}
}

// DefaultDial reaches a harnesswire server by address form: runtime/local passes a unix-socket
// address (unix://…); substrate passes the atenet-router's host:port. Both ride the same harnesswire
// gRPC client with the incarnation's CallMetadata attached to every call; only the transport
// differs. One dial path, two address forms.
func DefaultDial(inc api.Incarnation) (api.Harness, func() error, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
	}
	if len(inc.CallMetadata) > 0 {
		md := metadata.New(inc.CallMetadata)
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				return invoker(withCallMetadata(ctx, md), method, req, reply, cc, callOpts...)
			}),
			grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(withCallMetadata(ctx, md), desc, cc, method, callOpts...)
			}),
		)
	}
	if sock, ok := strings.CutPrefix(inc.Address, "unix://"); ok {
		return unixDial(sock, opts)
	}
	return tcpDial(inc.Address, opts)
}

// withCallMetadata attaches the incarnation's routing metadata to an outgoing call, replacing any
// value the caller set for the same keys: the incarnation, not the call site, decides which sandbox
// a call reaches.
func withCallMetadata(ctx context.Context, md metadata.MD) context.Context {
	out, _ := metadata.FromOutgoingContext(ctx)
	out = out.Copy()
	for k, v := range md {
		out[k] = v
	}
	return metadata.NewOutgoingContext(ctx, out)
}

// unixDial connects to a harnesswire server on a unix socket (runtime/local).
func unixDial(sock string, opts []grpc.DialOption) (api.Harness, func() error, error) {
	conn, err := grpc.NewClient(
		"passthrough:///agentlocal",
		append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}))...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("placement: dial unix %s: %w", sock, err)
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}

// tcpDial connects to a harnesswire server at a TCP host:port over h2c (cleartext HTTP/2). For
// substrate that is the atenet-router, which selects the actor from the call metadata and carries
// the gRPC stream to the actor over mTLS between router and worker. The leg from this process to the
// router is plaintext and the router does not authenticate callers, so this path belongs on a
// trusted network only (see docs/security.md).
func tcpDial(address string, opts []grpc.DialOption) (api.Harness, func() error, error) {
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("placement: dial %s: %w", address, err)
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}

// ErrCheckpointing is returned when another operation holds the session's checkpoint mark,
// or a checkpoint ended the harness connection a turn was using. It is retryable once that
// operation returns; session.Service maps it to codes.Aborted.
var ErrCheckpointing = errors.New("placement: session is being checkpointed")

// errSuperseded is the cancellation cause of a turn whose harness connection a checkpoint ended. The
// checkpoint mints its fence before it ends the connection, so the turn has been superseded exactly
// as a fenced writer is, and the error says both.
var errSuperseded = fmt.Errorf("%w: in-flight turn superseded and its harness connection closed: %w", ErrCheckpointing, eventlog.ErrFenced)

// turnError reports why a turn failed. When a checkpoint ended the turn's harness connection, the
// error the turn itself surfaced is incidental (a cancelled model call, a closed stream, a cut
// Describe), so the checkpoint is reported as the cause and the incidental error is kept for
// diagnosis; controllerError is not applied, because it would report the cut Describe as the
// caller's cancellation. Otherwise the controller's Describe boundary is mapped by controllerError.
func turnError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, errSuperseded) {
		if errors.Is(err, errSuperseded) {
			return err
		}
		return fmt.Errorf("%w (turn ended with: %w)", cause, err)
	}
	return controllerError(ctx, err)
}

// liveHarness is one open harness connection and the means to end it from outside the turn that
// owns it.
type liveHarness struct {
	harness api.Harness
	cancel  context.CancelCauseFunc
	once    sync.Once
	close   func() error
	err     error
	// superseded is set, under sessionSet.mu, by the checkpoint that collects this connection, BEFORE
	// that checkpoint mints its fence. A turn whose fence is newer than the checkpoint's is not
	// fenced by it, but is guaranteed to observe this flag, so it checks the flag after minting.
	superseded atomic.Bool
	// minted is closed once the turn can no longer mint a fence: right after its one NewFence call
	// returns, or when the turn releases the connection without having made it. A checkpoint waits
	// on it so that no turn's fence can land after the fence the checkpoint records under.
	minted     chan struct{}
	mintedOnce sync.Once
}

// fenceMinted records that the turn has made its one NewFence call, successful or not.
func (l *liveHarness) fenceMinted() {
	l.mintedOnce.Do(func() { close(l.minted) })
}

// checkSuperseded fails with errSuperseded if a checkpoint has claimed this connection or already
// ended it. The turn calls it right after minting its fence and before it writes anything.
func (l *liveHarness) checkSuperseded(ctx context.Context) error {
	if l.superseded.Load() {
		return errSuperseded
	}
	if ctx.Err() != nil {
		return turnError(ctx, ctx.Err())
	}
	return nil
}

// end cancels the turn's context, which ends its Connect stream, and closes the connection. It is
// idempotent, so the owning turn and a checkpoint can both call it.
func (l *liveHarness) end(cause error) error {
	l.once.Do(func() {
		l.cancel(cause)
		l.err = l.close()
	})
	return l.err
}

// openHarness dials the incarnation's harness and registers the connection under the session, so a
// checkpoint of that session can end it. The returned context is the turn's: it is cancelled when
// the connection is ended from outside.
//
// Registration is refused with ErrCheckpointing while a checkpoint of the session is in progress.
// The check and the registration happen under the same lock beginCheckpoint takes, so every turn
// either registered before the checkpoint began (and is ended by it) or never opens a stream.
func (p *Placer) openHarness(ctx context.Context, sessionUID string, inc api.Incarnation) (context.Context, *liveHarness, error) {
	har, closeHarness, err := p.dial(inc)
	if err != nil {
		return ctx, nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	l := &liveHarness{harness: har, cancel: cancel, close: closeHarness, minted: make(chan struct{})}
	p.sessions.mu.Lock()
	if _, ok := p.sessions.checkpoints[sessionUID]; ok {
		p.sessions.mu.Unlock()
		// Nothing was sent on the connection, so the close error carries no information.
		_ = l.end(context.Canceled)
		return ctx, nil, fmt.Errorf("%w: session %q", ErrCheckpointing, sessionUID)
	}
	if p.sessions.live == nil {
		p.sessions.live = map[string]map[*liveHarness]struct{}{}
	}
	if p.sessions.live[sessionUID] == nil {
		p.sessions.live[sessionUID] = map[*liveHarness]struct{}{}
	}
	p.sessions.live[sessionUID][l] = struct{}{}
	p.sessions.mu.Unlock()
	return ctx, l, nil
}

// releaseHarness deregisters and closes a turn's harness connection when the turn returns.
func (p *Placer) releaseHarness(ctx context.Context, sessionUID, incarnationID string, l *liveHarness) error {
	p.sessions.mu.Lock()
	delete(p.sessions.live[sessionUID], l)
	if len(p.sessions.live[sessionUID]) == 0 {
		delete(p.sessions.live, sessionUID)
	}
	p.sessions.mu.Unlock()
	l.fenceMinted() // a turn that returns before minting never will
	finish := observability.StartDebug(context.WithoutCancel(ctx), p.logger, "placement", "close_harness",
		"session_uid", sessionUID,
		"incarnation_id", incarnationID,
	)
	err := l.end(context.Canceled)
	finish(err, "error_kind", closeErrorKind(err))
	return err
}

// refuseDuringCheckpoint fails with ErrCheckpointing while a checkpoint of the session is in
// progress. It is an early exit only; openHarness is where the refusal is enforced.
func (p *Placer) refuseDuringCheckpoint(sessionUID string) error {
	p.sessions.mu.Lock()
	defer p.sessions.mu.Unlock()
	if _, ok := p.sessions.checkpoints[sessionUID]; ok {
		return fmt.Errorf("%w: session %q", ErrCheckpointing, sessionUID)
	}
	return nil
}

// beginCheckpoint marks the session as being checkpointed, so openHarness refuses new connections
// for it, and returns the connections already open, each marked superseded. Taking the mark and the
// set under one lock is what leaves no window for a turn to slip a stream in between. The caller
// mints its fence after this returns, so a turn that mints a newer fence still sees the superseded
// mark (checkSuperseded) and writes nothing. A connection stays marked even if the checkpoint
// aborts. Every successful call must be paired with endCheckpoint.
//
// Checkpoints do not nest: while another operation holds the mark, beginCheckpoint fails with
// ErrCheckpointing and changes nothing. A second fork would otherwise mint a fence that fails the
// first one's SUSPEND record after the parent is already cold, or checkpoint the parent again and
// replace the snapshot the first one's remaining children clone from; a fork beside a Suspend
// would fail the Suspend's record the same way.
func (p *Placer) beginCheckpoint(sessionUID string) ([]*liveHarness, error) {
	p.sessions.mu.Lock()
	defer p.sessions.mu.Unlock()
	if err := p.sessions.setCheckpointLocked(sessionUID); err != nil {
		return nil, err
	}
	open := make([]*liveHarness, 0, len(p.sessions.live[sessionUID]))
	for l := range p.sessions.live[sessionUID] {
		l.superseded.Store(true)
		open = append(open, l)
	}
	return open, nil
}

// markCheckpoint excludes a stateful Fork during a guarded Suspend, approval decision,
// or checked Resume fence acquisition.
// The caller holds the session guard; marking does not close harness connections. It fails with
// ErrCheckpointing, changing nothing, while another operation holds the mark.
// Pair success with endCheckpoint.
func (s *sessionSet) markCheckpoint(sessionUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setCheckpointLocked(sessionUID)
}

// setCheckpointLocked sets the shared mark, or fails if another operation holds it.
// The caller holds sessionSet.mu.
func (s *sessionSet) setCheckpointLocked(sessionUID string) error {
	if _, ok := s.checkpoints[sessionUID]; ok {
		return fmt.Errorf("%w: session %q checkpoint mark is held by another operation", ErrCheckpointing, sessionUID)
	}
	if s.checkpoints == nil {
		s.checkpoints = map[string]struct{}{}
	}
	s.checkpoints[sessionUID] = struct{}{}
	return nil
}

// endCheckpoint lifts the mark beginCheckpoint or markCheckpoint set.
func (s *sessionSet) endCheckpoint(sessionUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.checkpoints, sessionUID)
}

// endHarnesses ends the harness connections beginCheckpoint returned, and must run before a stateful
// fork checkpoints the session's compute. Substrate drains the actor's in-flight requests before it
// snapshots, and an open Connect stream idling on a model call counts as one: left open, it stalls
// the checkpoint until that drain times out. Ending the stream also stops a turn the checkpoint has
// just fenced from talking to a harness that will be frozen mid-call.
//
// Suspend does not call it: it refuses to run beside an Exec or Resume of the session
// (ErrSessionBusy), and they hold the session guard until their harness connection is closed.
//
// The caller mints its fence first, so the turn being ended can no longer write: it records no
// ERROR for an interruption the host caused, and it returns errSuperseded. A turn that minted a newer
// fence in between is stopped by the superseded mark beginCheckpoint set (checkSuperseded).
//
// Ending a stream only cancels the turn's context, and a turn that registered but has not yet
// minted its fence still mints one. So endHarnesses then waits, bounded by ctx, until every ended
// turn has made its fence call or returned. After it returns nil, no turn run by a Placer sharing
// this session set can mint a fence on the session, and the fence the caller mints next is the
// newest one the checkpoint will see. If ctx ends first, it returns an error and the caller must
// abort before checkpointing.
//
// It covers connections opened by every Placer sharing this Placer's session set: all Placers of
// one Registry, so a turn routed by ExecRequest.harness to another harness's Placer is ended too.
// A turn driven by another process is fenced by the log as before, but its stream stays open until
// that turn next touches the log or returns.
func (p *Placer) endHarnesses(ctx context.Context, sessionUID string, open []*liveHarness) (err error) {
	if len(open) == 0 {
		return nil
	}
	finish := observability.StartDebug(ctx, p.logger, "placement", "end_harness_streams",
		"session_uid", sessionUID,
		"connections", len(open),
	)
	var closeErrs []error
	for _, l := range open {
		closeErrs = append(closeErrs, l.end(errSuperseded))
	}
	defer func() {
		if err != nil {
			finish(err, "error_kind", "superseded_turn_not_settled")
			return
		}
		closeErr := errors.Join(closeErrs...)
		finish(closeErr, "error_kind", closeErrorKind(closeErr))
	}()
	for _, l := range open {
		select {
		case <-l.minted:
		case <-ctx.Done():
			return fmt.Errorf("placement: waiting for the superseded turn on session %q to finish minting its fence: %w", sessionUID, context.Cause(ctx))
		}
	}
	return nil
}

// Suspend transitions the incarnation to cold via SnapshotExternal, then records the SnapshotRef
// in a SUSPEND lifecycle event so Resume can recover it from the tamper-evident chain (§5.1).
// Snapshot owns dedicated compute release and retains the handles Restore needs; Stop would
// destructively tear them down. Snapshot keys on the session id plus the compute instance the
// journal last recorded, so a backend can refuse to checkpoint compute that replaced the one the
// session's turns ran on (api.ErrMemoryStateLost); nothing is journaled then.
//
// Suspend does not interrupt a running turn. While an Exec or Resume of the session runs through
// this Placer or its Registry, Suspend fails with ErrSessionBusy without touching compute or the
// journal, and the caller retries once the turn has returned. So the checkpoint never runs under an
// open harness stream of a turn this Registry drives.
//
// Suspend also holds the session's checkpoint mark, the one a stateful Fork holds, from before it
// snapshots until it has recorded the SUSPEND event. While a fork of the session holds the mark,
// from its checkpoint until its last child is cloned, Suspend fails with ErrCheckpointing without
// touching compute or the journal; a fork that arrives during a Suspend is refused the same way. So
// a Suspend can neither fail the fork's SUSPEND record nor replace the snapshot its children clone
// from.
func (p *Placer) Suspend(ctx context.Context, log eventlog.Store, sessionUID string) (ref api.SnapshotRef, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "suspend", "session_uid", sessionUID)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	defer release()
	// The guard is taken first and never waited on (tryLock), and the mark is set under
	// sessionSet.mu, which is never held while taking the guard, so the two cannot deadlock.
	if err := p.sessions.markCheckpoint(sessionUID); err != nil {
		return api.SnapshotRef{}, err
	}
	defer p.sessions.endCheckpoint(sessionUID)

	return p.suspend(ctx, log, sessionUID, nil)
}

// suspend runs with the shared session guard already held. A nil cursor preserves public
// Suspend's fresh-fence behavior; automatic parks supply the invocation fence and captured head.
type suspendCursor struct {
	head, fence int64
	observer    controller.Observer
}

func (p *Placer) suspend(ctx context.Context, log eventlog.Store, sessionUID string, cursor *suspendCursor) (ref api.SnapshotRef, err error) {
	// Applies to public, recorded-route and automatic approval snapshots alike.
	if err := eventlog.CheckWritable(log); err != nil {
		return api.SnapshotRef{}, err
	}
	// The snapshot must capture the compute the session's turns ran on, exactly as a stateful fork's
	// does (forkSource): a SUSPEND recording a replacement's snapshot would put a checkpoint without
	// the session's state on the chain. Pass the instance the journal last recorded for this
	// Placer's harness so the backend refuses another actor (api.ErrMemoryStateLost); the session
	// guard the caller holds keeps any turn of this Registry from recording a newer one meanwhile.
	recs, err := log.Read(1)
	if err != nil {
		return api.SnapshotRef{}, fmt.Errorf("placement: read session journal: %w", err)
	}
	instance, _ := controller.LatestComputeInstance(recs, p.ranHere)
	inc := api.Incarnation{ID: sessionUID, ComputeInstance: instance}
	ref, err = p.backend.Snapshot(ctx, inc, api.SnapshotExternal)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	appendFinished := observability.StartDebug(ctx, p.logger, "placement", "append_suspend_event", "session_uid", sessionUID)
	lc := api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}
	if cursor == nil {
		err = appendLifecycle(log, lc)
	} else {
		var record eventlog.Record
		record, err = log.Append(cursor.head, cursor.fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &lc})
		if err == nil && cursor.observer.OnRecord != nil {
			cursor.observer.OnRecord(record)
		}
	}
	appendFinished(err, "error_kind", placementErrorKind(err))
	if err != nil {
		return api.SnapshotRef{}, err
	}
	return ref, nil
}

// handoffPark releases the invocation connection before compute, keeping the same guard and
// fence through the captured-head SUSPEND append. Only a fully committed handoff returns park.
func (p *Placer) handoffPark(ctx context.Context, log eventlog.Store, sessionUID string, fence int64, park *api.ApprovalParkedError, closeInvocation func() error, observer controller.Observer) error {
	if err := p.sessions.markCheckpoint(sessionUID); err != nil {
		return err
	}
	defer p.sessions.endCheckpoint(sessionUID)
	state, err := controller.InspectApproval(log, 0)
	if err != nil {
		return err
	}
	if state == nil || state.Inherited || state.Receipt != nil || state.Request == nil || state.Decision != nil ||
		park.Ref != (api.ApprovalRef{ExecutionID: state.ExecutionID, ToolCallID: state.Call.ID, RequestSeq: state.Request.Seq}) {
		return fmt.Errorf("%w: park does not match the recorded owned request", controller.ErrReplayDiverged)
	}
	if err := closeInvocation(); err != nil {
		return err
	}
	if _, err := p.suspend(ctx, log, sessionUID, &suspendCursor{head: state.Head, fence: fence, observer: observer}); err != nil {
		return err
	}
	return park
}

// Resume returns *api.ApprovalParkedError for an owned pending request without runtime IO or a
// new fence. A call-only cut mints a fence and repairs one request at the captured head, also
// without runtime IO. Neither park proves cold compute: only a committed SUSPEND records that.
// Otherwise Resume restores the recorded SnapshotRef (or the session handle when no owned
// SUSPEND exists), mints a new fence, re-drives the turn and records RESUME on ordinary success.
// A new gate instead closes the dial, snapshots and commits SUSPEND under that recovery fence,
// returning a typed park without RESUME. Repair/handoff failures return their actual cause.
func (p *Placer) Resume(ctx context.Context, log eventlog.Store, sessionUID string, opts ...ResumeOption) error {
	var cfg resumeConfig
	for _, o := range opts {
		o(&cfg)
	}
	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return err
	}
	defer release()
	return p.resume(ctx, log, sessionUID, cfg)
}

// resumeApproval answers an unresolved gate without allocating an actor. Inspection/route checks
// precede this helper under the shared guard; only a missing request needs a new writer fence.
func (p *Placer) resumeApproval(log eventlog.Store, sessionUID string, state *controller.ApprovalState, cfg resumeConfig) (bool, error) {
	if state == nil || state.Receipt != nil {
		return false, nil
	}
	if state.Inherited {
		return true, controller.ErrInheritedToolIntent
	}
	if state.Decision != nil {
		return false, nil
	}
	if state.Request != nil {
		return true, &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: state.ExecutionID, ToolCallID: state.Call.ID, RequestSeq: state.Request.Seq}}
	}
	fence, err := p.resumeFence(log, sessionUID, cfg.expectedLastSeq)
	if err != nil {
		return true, err
	}
	record, err := log.Append(state.Head, fence, api.Event{ExecutionID: state.ExecutionID, Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: state.Call.ID}})
	if err != nil {
		return true, err
	}
	if cfg.observer.OnRecord != nil {
		cfg.observer.OnRecord(record)
	}
	return true, &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: state.ExecutionID, ToolCallID: state.Call.ID, RequestSeq: record.Seq}}
}

// resumeFence checks the cursor after fencing older writers. A checked early mint has no
// registered live connection yet, so hold the checkpoint mark until it completes: Fork must not
// miss that mint and snapshot under a fence it can invalidate. Never hold the mark through Restore
// or openHarness, which must still be able to register the turn.
func (p *Placer) resumeFence(log eventlog.Store, sessionUID string, expected *int64) (int64, error) {
	if expected != nil {
		if err := p.sessions.markCheckpoint(sessionUID); err != nil {
			return 0, err
		}
		defer p.sessions.endCheckpoint(sessionUID)
	}
	fence, err := log.NewFence()
	if err != nil {
		return 0, err
	}
	if err := checkResumeHead(log, expected); err != nil {
		return 0, err
	}
	return fence, nil
}

// resume runs the recovery transition with the session guard already held by Placer or Registry.
func (p *Placer) resume(ctx context.Context, log eventlog.Store, sessionUID string, cfg resumeConfig) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	var inc api.Incarnation
	finish := observability.StartDebug(ctx, p.logger, "placement", "resume", "session_uid", sessionUID)
	defer func() {
		finish(err, "error_kind", placementErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	if err := eventlog.CheckWritable(log); err != nil {
		return err
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil {
		return err
	}
	if handled, err := p.resumeApproval(log, sessionUID, state, cfg); handled {
		return err
	}
	// Gate before Restore, exactly as Exec does before Create: Resume re-drives an interrupted turn,
	// so it must not reach a harness the backend cannot host.
	desc, err := p.admit(ctx, sessionUID)
	if err != nil {
		return err
	}
	if err := refuseStatefulOverride(desc, cfg.override); err != nil {
		return err
	}
	sourceFinished := observability.StartDebug(ctx, p.logger, "placement", "resolve_resume_source", "session_uid", sessionUID)
	recs, err := log.Read(1)
	if err != nil {
		sourceFinished(err, "error_kind", placementErrorKind(err))
		return err
	}
	ref := lastSuspendRef(recs, sessionUID)
	sourceFinished(nil, "memory_snapshot", ref.Memory)
	if err := p.refuseDuringCheckpoint(sessionUID); err != nil {
		return err
	}
	if cfg.expectedLastSeq != nil {
		// Refuse known movement before waking compute. This is not yet the turn's writer fence:
		// a Fork can finish during Restore, before openHarness registers this turn.
		if _, err := p.resumeFence(log, sessionUID, cfg.expectedLastSeq); err != nil {
			return err
		}
	}
	// A backend that can tell instances apart reads the one this harness's turns last ran on, so a
	// failed restore is reported as lost state only when that instance is gone or replaced. It reads
	// the journal when it decides, not here, so a turn another host journals meanwhile is seen.
	if r, ok := p.backend.(api.InstanceRestorer); ok {
		inc, err = r.RestoreInstance(ctx, ref, func() (string, error) {
			recs, err := log.Read(1)
			if err != nil {
				return "", fmt.Errorf("placement: read session journal: %w", err)
			}
			instance, _ := controller.LatestComputeInstance(recs, p.ranHere)
			return instance, nil
		})
	} else {
		inc, err = p.backend.Restore(ctx, ref)
	}
	if err != nil {
		return err
	}
	dialFinished := observability.StartDebug(ctx, p.logger, "placement", "dial_harness",
		"session_uid", sessionUID,
		"incarnation_id", inc.ID,
		"runtime", inc.Runtime,
		"transport", addressTransport(inc.Address),
	)
	operationCtx := ctx
	ctx, live, err := p.openHarness(ctx, sessionUID, inc)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return err
	}
	dialFinished(nil)
	defer func() {
		// Ordinary cleanup logs close failures without replacing the turn outcome.
		_ = p.releaseHarness(ctx, sessionUID, inc.ID, live)
	}()
	har := live.harness
	closeInvocation := func() error {
		if err := live.checkSuperseded(ctx); err != nil {
			return err
		}
		return p.releaseHarness(ctx, sessionUID, inc.ID, live)
	}
	if err := p.recheck(ctx, sessionUID, har, cfg.override); err != nil {
		return err
	}
	// Retake the writer fence now that checkpoints can see this connection. Rechecking also
	// catches a completed Fork during Restore, before any harness/model/tool redrive.
	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := p.resumeFence(log, sessionUID, cfg.expectedLastSeq)
	live.fenceMinted()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return err
	}
	fenceFinished(nil)
	inc.FenceToken = fence
	if err := live.checkSuperseded(ctx); err != nil {
		return err
	}
	copts := append(p.controllerOpts(fence, sessionUID, cfg.observer), controller.WithHarness(cfg.harness),
		p.computeInstance(inc))
	c, err := controller.New(log, p.model, copts...)
	if err != nil {
		return err
	}
	if _, err := c.Resume(ctx, har); err != nil {
		var park *api.ApprovalParkedError
		if errors.As(err, &park) && api.IsParkUnwind(err, park.Ref) {
			return p.handoffPark(operationCtx, log, sessionUID, fence, park, closeInvocation, cfg.observer)
		}
		return turnError(ctx, err)
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	// RESUME marker under the incarnation's fence (controller.Resume used the same token).
	appendFinished := observability.StartDebug(ctx, p.logger, "placement", "append_resume_event", "session_uid", sessionUID)
	record, err := log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleResume}})
	appendFinished(err, "error_kind", placementErrorKind(err))
	if err == nil && cfg.observer.OnRecord != nil {
		cfg.observer.OnRecord(record)
	}
	return err
}

// ForkChild is one child of a fan-out fork: the child's session UID and its (empty) log.
type ForkChild struct {
	UID string
	Log eventlog.Store
}

func addressTransport(address string) string {
	if strings.HasPrefix(address, "unix://") {
		return "unix"
	}
	return "tcp"
}

func placementErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrUnplaceable):
		return "unplaceable"
	case errors.Is(err, ErrDescriptorMismatch):
		return "descriptor_mismatch"
	case errors.Is(err, ErrStatefulOverride):
		return "stateful_override"
	case errors.Is(err, ErrHarnessUnavailable):
		return "harness_unavailable"
	case errors.Is(err, ErrCheckpointing):
		return "checkpointing"
	case errors.Is(err, api.ErrMemoryStateLost):
		return "memory_state_lost"
	case errors.Is(err, ErrSessionBusy), errors.Is(err, eventlog.ErrConflict):
		return "conflict"
	case errors.Is(err, eventlog.ErrFenced):
		return "fenced"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "operation_failed"
	}
}

func closeErrorKind(err error) string {
	if err == nil {
		return ""
	}
	return "harness_close_failed"
}

// Fork branches a session into one or more children: it provisions each child's compute through the
// Runtime SPI and copies the parent's log prefix up to atSeq so every child shares the parent's hash
// chain.
//
// The compute half depends on the harness's resumability. A STATELESS_REPLAY harness replay-forks —
// each child cold-boots and the copied journal reconstructs it, leaving the parent untouched. A
// REQUIRES_MEMORY_SNAPSHOT harness holds live state the journal cannot reconstruct (I4), so the
// parent is checkpointed first and every child is cloned from that snapshot.
//
// The whole fan-out shares ONE parent checkpoint: N children cost one snapshot and N clones, and all
// of them branch from the identical state. The parent's checkpoint mark is held until the last child
// is cloned, so a turn on the parent is refused (ErrCheckpointing) for the whole fan-out instead of
// waking the parent and replacing the snapshot the remaining children clone from.
func (p *Placer) Fork(ctx context.Context, parent eventlog.Store, parentUID string, children []ForkChild, atSeq int64) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "fork",
		"parent_session_uid", parentUID,
		"children", len(children),
		"at_seq", atSeq,
	)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	guardRelease, err := p.guard.tryReadLock(parentUID)
	if err != nil {
		return err
	}
	defer guardRelease()
	if err := eventlog.CheckWritable(parent); err != nil {
		return err
	}
	// Validate the entire fan-out before checkpointing or provisioning. A refused target
	// must not alter its parent or earlier siblings, or trigger rollback Stop.
	for _, child := range children {
		if err := eventlog.CheckWritable(child.Log); err != nil {
			return fmt.Errorf("placement: fork child %q: %w", child.UID, err)
		}
	}
	ref, release, err := p.forkSource(ctx, parent, parentUID, atSeq)
	if err != nil {
		return err
	}
	defer release()
	// A failed fan-out must not strand compute. The child UIDs are minted per request and returned
	// only on success, so anything provisioned before the failure would otherwise be unreachable —
	// unstoppable workers nobody can name.
	created := make([]string, 0, len(children))
	abort := func(err error) error {
		// Release the parent first. Nothing in the teardown reads it, and holding the mark through
		// up to MaxForkChildren × rollbackTimeout of Stops would refuse its turns for that long.
		release()
		p.rollbackFork(ctx, created)
		return err
	}
	for _, child := range children {
		if _, err := p.backend.Fork(ctx, ref, api.ForkOpts{ChildSessionUID: child.UID}); err != nil {
			return abort(err)
		}
		created = append(created, child.UID)
		copyFinished := observability.StartDebug(ctx, p.logger, "placement", "copy_fork_log",
			"parent_session_uid", parentUID,
			"child_session_uid", child.UID,
			"at_seq", atSeq,
		)
		err := controller.Fork(parent, child.Log, atSeq)
		copyFinished(err, "error_kind", placementErrorKind(err))
		if err != nil {
			return abort(err)
		}
	}
	return nil
}

// rollbackFork best-effort destroys the incarnations an aborted fan-out already provisioned. Each
// child gets its OWN budget: a Stop streams a full RAM+disk image to durable storage, so one shared
// deadline across a wide fan-out would expire partway and strand the tail — the same failure the
// detached context exists to prevent, just moved further down the list.
//
// The budget is detached from the caller's context because the likeliest cause of a mid-fan-out
// failure is the caller's deadline expiring, which is precisely when a rollback inheriting that
// context would reclaim nothing. Errors are ignored: the fork is failing regardless, and a stuck
// child must not mask the original cause.
//
// The cost of per-child budgets is that a wedged control plane parks this goroutine for up to
// MaxForkChildren × rollbackTimeout. Shared deletion admission stays held through rollback,
// excluding Delete but allowing new parent turns; no database handle is held. An overall
// cap tight enough to matter would abandon the tail, stranding the workers this exists to reclaim.
func (p *Placer) rollbackFork(ctx context.Context, childUIDs []string) {
	for _, uid := range childUIDs {
		p.stopOrphan(ctx, uid)
	}
}

func (p *Placer) stopOrphan(ctx context.Context, uid string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	_ = p.backend.Stop(ctx, api.Incarnation{ID: uid})
}

// rollbackTimeout bounds the best-effort teardown of ONE child of a failed fan-out.
const rollbackTimeout = 30 * time.Second

// forkSource produces the SnapshotRef the child is forked from, and a release func the caller must
// call once every child is cloned (it is idempotent, and a no-op for a stateless fork).
//
// For a stateless harness that is just the parent's handle (nothing to clone). For a memory harness
// it is a FRESH snapshot of the parent, recorded as a SUSPEND lifecycle event so the ref stays
// recoverable from the tamper-evident chain — the same shape Suspend records. It deliberately calls
// backend.Snapshot rather than Placer.Suspend so the fork can fence before checkpointing and commit
// the SUSPEND event with a CAS on the validated fork point.
//
// Checkpointing the parent is not undoable, so this is where a stateful fork commits: once it
// returns, the parent is cold with a SUSPEND event on its chain whether or not the children go on to
// provision. That is a recoverable state (Resume brings the parent back), not a leak, but it does
// mean a retry must fork at the new head rather than the original seq.
//
// For a stateful fork that returns a ref, the session's checkpoint mark is still held, so no turn
// opens a harness stream on the parent until release is called. If it returns an error or panics,
// the mark is already lifted.
func (p *Placer) forkSource(ctx context.Context, parent eventlog.Store, parentUID string, atSeq int64) (_ api.SnapshotRef, _ func(), err error) {
	desc, err := p.admit(ctx, parentUID)
	if err != nil {
		return api.SnapshotRef{}, nil, err
	}
	if desc.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		return api.SnapshotRef{Local: parentUID}, func() {}, nil
	}
	// Check the head BEFORE minting a fence. Minting is an autocommitted bump that supersedes any
	// in-flight writer, so doing it first would let a fork that is then REFUSED as unplaceable still
	// kill a turn racing on the parent — a request that changes nothing would leave the session worse
	// off than before it was asked.
	head, err := parent.Head()
	if err != nil {
		return api.SnapshotRef{}, nil, err
	}
	// A memory snapshot captures the parent's RAM as it is NOW, so it can only realize a fork taken
	// at the log head. Branching a stateful session at a historical seq would pair an old prefix with
	// present-day RAM; refuse — with no side effects at all — instead of producing that mismatch.
	if atSeq != head {
		return api.SnapshotRef{}, nil, fmt.Errorf("%w: harness %q requires %s, which can only fork at the log head (%d), not seq %d",
			ErrUnplaceable, desc.ID, desc.Capabilities.Resumability, head, atSeq)
	}
	// Past this point the fork is committing, so superseding the current writer is intended.
	// Fencing before the snapshot stops an in-flight turn advancing the head underneath an
	// irreversible checkpoint. The mark refuses new harness streams until every child is cloned.
	open, err := p.beginCheckpoint(parentUID)
	if err != nil {
		return api.SnapshotRef{}, nil, err
	}
	release := sync.OnceFunc(func() { p.sessions.endCheckpoint(parentUID) })
	// Lift the mark on every path but the one that hands release to the caller, a panic included:
	// a recovered panic in the runtime's Snapshot must not leave the parent refused for good.
	handedOff := false
	defer func() {
		if !handedOff {
			release()
		}
	}()
	if _, err := parent.NewFence(); err != nil {
		return api.SnapshotRef{}, nil, err
	}
	// A turn could still have committed in the window before that fence landed. Re-check now, while
	// aborting is free: after the snapshot the parent is cold whether or not the fork proceeds.
	if head, err = parent.Head(); err != nil {
		return api.SnapshotRef{}, nil, err
	}
	if atSeq != head {
		return api.SnapshotRef{}, nil, fmt.Errorf("%w: parent advanced from seq %d to %d while the fork was being prepared", eventlog.ErrConflict, atSeq, head)
	}
	if err := p.endHarnesses(ctx, parentUID, open); err != nil {
		return api.SnapshotRef{}, nil, err
	}
	// A turn ended above may have minted a newer fence after ours. It wrote nothing
	// (checkSuperseded), but its fence would leave ours stale and fail the SUSPEND append after the
	// parent is already cold. endHarnesses returned only once every such turn had minted, and no new
	// turn can open, so mint the fence the record is written under now, and re-check the head while
	// aborting is still free.
	fence, err := parent.NewFence()
	if err != nil {
		return api.SnapshotRef{}, nil, err
	}
	if head, err = parent.Head(); err != nil {
		return api.SnapshotRef{}, nil, err
	}
	if atSeq != head {
		return api.SnapshotRef{}, nil, fmt.Errorf("%w: parent advanced from seq %d to %d while the fork was being prepared", eventlog.ErrConflict, atSeq, head)
	}
	// The snapshot must capture the compute the parent's turns ran on. If that was replaced under
	// the same name, the replacement holds none of the parent's state, and every child cloned from
	// it would run without it: a child's own compute is a clone with its own instance, and the FORK
	// marker resets the comparison its turns make. Pass the instance the journal last recorded so
	// the backend refuses a replacement (api.ErrMemoryStateLost). The records read here are the
	// history the SUSPEND append below extends under its CAS on atSeq.
	recs, err := parent.Read(1)
	if err != nil {
		return api.SnapshotRef{}, nil, fmt.Errorf("placement: read parent journal: %w", err)
	}
	instance, _ := controller.LatestComputeInstance(recs, p.ranHere)
	ref, err := p.backend.Snapshot(ctx, api.Incarnation{ID: parentUID, ComputeInstance: instance}, api.SnapshotExternal)
	if err != nil {
		return api.SnapshotRef{}, nil, err
	}
	// Record the checkpoint before validating it: the parent is already cold, and the SUSPEND event
	// is what keeps that fact on the chain. CAS on atSeq — the seq the head check validated — rather
	// than a re-read head, so a writer that minted an even newer fence in the meantime aborts the
	// fork instead of silently widening the children's prefix past the RAM they were cloned from.
	if err := appendLifecycleAt(parent, atSeq, fence, api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}); err != nil {
		return api.SnapshotRef{}, nil, err
	}
	// Only now refuse an unusable checkpoint. A memory-capable runtime that yields no external handle
	// cannot clone, and cold-booting the children instead is the silent divergence this whole path
	// exists to prevent.
	if ref.ExternalURI == "" {
		return api.SnapshotRef{}, nil, fmt.Errorf("%w: runtime %q produced no external snapshot handle to clone from", ErrUnplaceable, desc.ID)
	}
	handedOff = true
	return ref, release, nil
}

// appendLifecycle mints a fresh fence and records a lifecycle event at the head observed afterward.
// Suspend holds the session guard across snapshot and append, so it cannot interrupt an Exec or
// Resume through the same guard. The fence still rejects stale writers outside that guard; a
// separate Registry or host can race this append and make it fail.
func appendLifecycle(log eventlog.Store, lc api.Lifecycle) error {
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	return appendLifecycleAt(log, head, fence, lc)
}

// appendLifecycleAt records a lifecycle event under the caller's fence, but only if the log head is
// still expectedLastSeq (single-writer CAS). It is the form to use when the caller already made a
// decision based on a specific head and must not have that decision invalidated underneath it. The
// fence is supplied rather than minted here so the caller can fence first and then observe a stable
// head.
func appendLifecycleAt(log eventlog.Store, expectedLastSeq, fence int64, lc api.Lifecycle) error {
	_, err := log.Append(expectedLastSeq, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &lc})
	return err
}

// startedTurns counts the turns the journal records as started that ran on this Placer, the
// inherited prefix of a forked child included: every EXECUTION_START, whether its turn completed,
// failed or was interrupted, since a turn that did not complete may still have changed the harness's
// state.
//
// A turn is attributed by the harness name its EXECUTION_START recorded (ranHere). A turn that cannot
// be attributed (a turn with no recorded name, a name this host does not serve, any turn on a Placer
// outside a Registry, or a legacy turn written before EXECUTION_START existed) is counted:
// undercounting would let a memory-tier backend cold-create compute for a session whose state it
// held, while overcounting can only refuse.
func (p *Placer) startedTurns(log eventlog.Store) (int, error) {
	recs, err := log.Read(1)
	if err != nil {
		return 0, fmt.Errorf("placement: read journal: %w", err)
	}
	n := 0
	inLegacyTurn := false
	for _, rec := range recs {
		ev := rec.Event
		switch {
		case ev.Kind == api.EventExecutionStart:
			inLegacyTurn = false
			recorded := ""
			if ev.ExecutionStart != nil {
				recorded = ev.ExecutionStart.Harness
			}
			if p.ranHere(recorded) {
				n++
			}
		case ev.ExecutionID == "" && isLegacyTurnEvent(ev.Kind):
			// A legacy turn has no EXECUTION_START: it is a run of events with no execution ID that
			// an END or ERROR closes, or that is still open at the tail.
			if !inLegacyTurn {
				n++
				inLegacyTurn = true
			}
			if ev.Kind == api.EventEnd || ev.Kind == api.EventError {
				inLegacyTurn = false
			}
		}
	}
	return n, nil
}

// isLegacyTurnEvent reports whether an event of kind k with no execution ID belongs to a turn written
// before EXECUTION_START existed (the kinds controller.legacyPrefix accepts, lifecycle excluded).
func isLegacyTurnEvent(k api.EventKind) bool {
	switch k {
	case api.EventInput, api.EventModelCall, api.EventOutput, api.EventToolCall,
		api.EventToolResult, api.EventUsage, api.EventEnd, api.EventError:
		return true
	}
	return false
}

// ranHere reports whether a turn recorded under the harness name recorded may have run on this
// Placer. In a Registry the answer is whether recorded resolves to this Placer, so a name that
// aliases it counts. A standalone Placer has one backend and no map from names to backends, so a
// turn recorded under any name may have run on it: every turn counts. Every case it cannot decide
// counts as this Placer's.
func (p *Placer) ranHere(recorded string) bool {
	if recorded == "" || p.registry == nil {
		return true
	}
	q, ok := p.registry.lookup(recorded)
	return !ok || q == p
}

// lastSuspendRef returns the SnapshotRef from the most recent SUSPEND event THIS session wrote, or —
// when there is none (a crash mid-turn, not a clean suspend) — a trivial ref naming the session so a
// filesystem-only backend re-provisions and replays the journal.
//
// The ownership check is load-bearing: a forked child inherits its parent's log prefix verbatim, so a
// parent checkpoint can sit in the child's history. Restore keys off SnapshotRef.Local (the actor
// handle), so accepting an inherited ref would restore the PARENT's incarnation under the child's
// session — two sessions bound to one actor. Every backend records its own session UID in Local, so
// a ref that names another session is history, not this session's checkpoint.
func lastSuspendRef(recs []eventlog.Record, sessionUID string) api.SnapshotRef {
	for i := len(recs) - 1; i >= 0; i-- {
		ev := recs[i].Event
		if ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleSuspend && ev.Lifecycle.Snapshot != nil {
			if ev.Lifecycle.Snapshot.Local != sessionUID {
				continue // inherited from a forked-from parent, not this session's checkpoint
			}
			return *ev.Lifecycle.Snapshot
		}
	}
	return api.SnapshotRef{Local: sessionUID}
}
