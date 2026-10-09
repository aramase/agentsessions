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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/observability"
)

// ErrUnplaceable is returned when a harness requires a capability the chosen backend cannot provide
// (e.g. REQUIRES_MEMORY_SNAPSHOT on a filesystem-only pod). The gate runs BEFORE Create, so an
// unplaceable harness never provisions compute or writes to the log; for a LiveDescriber backend it
// runs again on the turn's own connection, still before anything is written to the log.
// session.Service surfaces it as codes.FailedPrecondition.
var ErrUnplaceable = errors.New("placement: harness cannot be placed on this runtime")

// ErrHarnessUnavailable is returned when the harness could not be reached to describe itself. It is
// only ever raised by the admission check, which runs before any compute is provisioned or anything
// is written to the log (for a LiveDescriber, whose Create provisions nothing, it also runs after
// Create), so retrying is safe; session.Service surfaces it as codes.Unavailable. It is
// not raised when the caller's own deadline or cancellation ended the check: that is
// ErrAdmissionInterrupted, because resending the same expired call cannot succeed.
var ErrHarnessUnavailable = errors.New("placement: harness unavailable")

// describeTimeout bounds how long admission waits for a harness to describe itself. Describe is a
// small unary call that every harness answers without doing any work, so a harness that has not
// answered in this long is treated as unavailable rather than left to hold the call.
var describeTimeout = 10 * time.Second

// ErrAdmissionInterrupted is returned when the caller's own deadline or cancellation ended the
// admission check before the harness answered. It also wraps the context's error, so errors.Is
// reports context.DeadlineExceeded or context.Canceled. Like ErrHarnessUnavailable it is only raised
// before any compute is provisioned or anything is written to the log; session.Service surfaces it as
// codes.DeadlineExceeded or codes.Canceled. A turn interrupted after admission does not wrap it.
var ErrAdmissionInterrupted = errors.New("placement: admission interrupted")

// Backend is the compute Runtime the Placer drives. It exposes the harness DESCRIPTOR so the Placer
// can gate CanPlace before Create (placement must not provision compute to learn a harness's needs);
// the harness itself is reached by dialing Incarnation.Address (Harness.Connect) — the one dial path
// both runtime/local and substrate use. runtime/local satisfies this.
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
// CanPlace gate, before it mints a fence or writes anything to the log. That narrows the window but
// does not close it; see recheck for the limit.
//
// A backend that reports true must provision nothing in Create and Restore: a refusal by the second
// check only closes the dial and never calls Stop, so anything provisioned would leak.
type LiveDescriber interface {
	DescribesLiveHarness() bool
}

// Placer owns the incarnation lifecycle: Create the compute, mint+bind the fence, drive the controller.
type Placer struct {
	backend Backend
	model   controller.ModelFunc
	stream  controller.StreamFunc
	tool    controller.ToolFunc
	dial    Dialer
	logger  *slog.Logger
}

// Dialer opens a Harness.Connect client to the harness at a runtime-specific address and returns a
// closer for the connection. runtime/local passes a unix-socket address (unix://…); substrate passes
// the actor's pod IP as host:port (PodIP:80), dialed directly over h2c — the atenet mesh is
// HTTP/1.1-only to actors, so gRPC bypasses the router. The default dialer handles both forms;
// WithDialer overrides it (tests). This is the one transport seam the harness rides unchanged.
type Dialer func(address string) (api.Harness, func() error, error)

// Option configures a Placer.
type Option func(*Placer)

// WithDialer overrides how the Placer reaches a harness (default: unix-socket dial for runtime/local).
func WithDialer(d Dialer) Option { return func(p *Placer) { p.dial = d } }

// ExecOption configures a single execution.
type ExecOption func(*execConfig)

type execConfig struct {
	observer      controller.Observer
	config        []byte
	resumeFromSeq int64
	deadline      time.Time
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

// WithStreamingModel supplies a model that reports partial output, which the controller relays as
// ephemeral deltas. Without it a turn still runs; the caller just sees the finalized output only.
func WithStreamingModel(fn controller.StreamFunc) Option { return func(p *Placer) { p.stream = fn } }

// WithToolExecutor supplies the host executor for controller-mediated tools in Exec and Resume.
// It receives the session UID bound by the Placer. Without one, tool calls fail closed. The host
// owns session-scoped authorization and durable deduplication by session UID plus idempotency key.
func WithToolExecutor(fn controller.ToolFunc) Option { return func(p *Placer) { p.tool = fn } }

// WithLogger enables structured operational logs. Message contents and fence tokens are never logged.
func WithLogger(logger *slog.Logger) Option { return func(p *Placer) { p.logger = logger } }

// New builds a Placer over a compute backend and the live model.
func New(backend Backend, model controller.ModelFunc, opts ...Option) *Placer {
	p := &Placer{
		backend: backend,
		model:   model,
		dial:    defaultDial,
		logger:  slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.New(slog.DiscardHandler)
	}
	return p
}

// Exec places one turn: Create the incarnation, mint the fence from the log and stamp it on the
// incarnation, bind a controller to that same token, and drive the (placed) harness. The log stays the
// single fence authority; the returned incarnation carries the fence for Suspend/Resume (step 5).
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

	if _, err := p.admit(ctx, sessionUID); err != nil {
		return api.Incarnation{}, err
	}

	createFinished := observability.StartDebug(ctx, p.logger, "placement", "create_compute", "session_uid", sessionUID)
	inc, err = p.backend.Create(ctx, &api.SessionSpec{SessionUID: sessionUID})
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
	har, closeHarness, err := p.dial(inc.Address)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return inc, err
	}
	dialFinished(nil)
	defer p.closeHarness(ctx, sessionUID, inc.ID, closeHarness)
	if err := p.recheck(ctx, sessionUID, har); err != nil {
		return inc, err
	}

	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := log.NewFence()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return inc, err
	}
	fenceFinished(nil)
	inc.FenceToken = fence // Placer-owned: the incarnation carries the token Suspend/Resume will need
	copts := append(p.controllerOpts(fence, sessionUID, cfg.observer),
		controller.WithStart(cfg.config, cfg.resumeFromSeq))
	c, err := controller.New(log, p.model, copts...)
	if err != nil {
		return inc, err
	}
	if err := c.Exec(ctx, har, inputs, expectedLastSeq); err != nil {
		return inc, err
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
// fence is minted, so a refusal still leaves nothing on the log. The backend's Create and Restore
// provision nothing for such a backend, so there is no compute to release either.
//
// The check has a known limit. Capabilities are checked with a Describe call before the turn's
// Connect stream is opened, and the answer is not bound to that stream: the harness protocol has no
// way to tie the two together. gRPC can close the connection and dial again between the two calls,
// and a proxy can route each call to a different backend, so if a different harness takes over the
// same address between Describe and Connect, the turn runs on it and this check does not detect
// it. Registering a harness by address therefore assumes the operator controls what serves that
// address; docs/security.md states the same limit.
func (p *Placer) recheck(ctx context.Context, sessionUID string, har api.Harness) error {
	if l, ok := p.backend.(LiveDescriber); !ok || !l.DescribesLiveHarness() {
		return nil
	}
	_, err := p.gate(ctx, sessionUID, "verify_execution_harness", har.Describe)
	return err
}

// gate describes the harness with describe and applies CanPlace, mapping a harness that cannot be
// described to ErrHarnessUnavailable or ErrAdmissionInterrupted.
func (p *Placer) gate(ctx context.Context, sessionUID, op string, describe func(context.Context) (api.Descriptor, error)) (desc api.Descriptor, err error) {
	resolveFinished := observability.StartDebug(ctx, p.logger, "placement", op, "session_uid", sessionUID)
	// Bound the check on its own. A harness that accepts the connection and then never answers
	// Describe would otherwise hold a call with no deadline forever. WithTimeout keeps an earlier
	// caller deadline.
	dctx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()
	desc, err = describe(dctx)
	if err != nil {
		switch c := status.Code(err); {
		case ctx.Err() != nil:
			// The caller's deadline or cancellation ended the check, not the harness: gRPC reports
			// that as DeadlineExceeded or Canceled too, so the context decides. Keep the caller's
			// cause so the service answers DEADLINE_EXCEEDED or CANCELLED rather than an outage.
			// The backend's error is formatted with %v so that only the caller's cause is in the
			// chain: a backend error wrapping its own context error must not change the code.
			err = fmt.Errorf("%w: describe harness: %w: %v", ErrAdmissionInterrupted, ctx.Err(), err)
		case dctx.Err() != nil || c == codes.Unavailable || c == codes.DeadlineExceeded:
			// A refused or reset connection is Unavailable. A harness that did not answer within
			// describeTimeout, including a peer that accepts the connection but never completes the
			// handshake, is the harness's fault while the caller's context is still live, and so is
			// a DeadlineExceeded the peer reports itself. Nothing has been provisioned or journaled,
			// so all of them are a retryable outage.
			err = fmt.Errorf("%w: %w", ErrHarnessUnavailable, err)
		}
		resolveFinished(err, "error_kind", "describe_harness_failed")
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

// defaultDial reaches a harnesswire server by address form: runtime/local passes a unix-socket
// address (unix://…); substrate passes the actor's pod IP as host:port (PodIP:80). Both ride the same
// harnesswire gRPC client; only the transport differs. One dial path, two address forms.
func defaultDial(address string) (api.Harness, func() error, error) {
	if sock, ok := strings.CutPrefix(address, "unix://"); ok {
		return unixDial(sock)
	}
	return tcpDial(address)
}

// unixDial connects to a harnesswire server on a unix socket (runtime/local).
func unixDial(sock string) (api.Harness, func() error, error) {
	conn, err := grpc.NewClient(
		"passthrough:///agentlocal",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("placement: dial unix %s: %w", sock, err)
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}

// tcpDial connects to a harnesswire server at a TCP host:port over h2c (cleartext HTTP/2). Substrate
// exposes the actor's harness on PodIP:80; an in-cluster caller dials it directly, bypassing the
// HTTP/1.1-only atenet router. No TLS: the harness terminates plaintext gRPC, which is why this
// path belongs on a trusted network only (see docs/security.md).
func tcpDial(address string) (api.Harness, func() error, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("placement: dial %s: %w", address, err)
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}

// Suspend snapshots the incarnation to external storage, records the SnapshotRef in a SUSPEND
// lifecycle event so Resume can recover it from the tamper-evident chain (§5.1), then frees the
// worker via Stop. Snapshot and Stop key on the session id, so a minimal incarnation suffices.
func (p *Placer) Suspend(ctx context.Context, log eventlog.Store, sessionUID string) (ref api.SnapshotRef, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "suspend", "session_uid", sessionUID)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	inc := api.Incarnation{ID: sessionUID}
	ref, err = p.backend.Snapshot(ctx, inc, api.SnapshotExternal)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	appendFinished := observability.StartDebug(ctx, p.logger, "placement", "append_suspend_event", "session_uid", sessionUID)
	if err := appendLifecycle(log, api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}); err != nil {
		appendFinished(err, "error_kind", placementErrorKind(err))
		return api.SnapshotRef{}, err
	}
	appendFinished(nil)
	if err := p.backend.Stop(ctx, inc); err != nil {
		return api.SnapshotRef{}, err
	}
	return ref, nil
}

// Resume recovers the recorded SnapshotRef, Restores an incarnation, mints a NEW fence (superseding
// any zombie writer), binds a controller to it, re-drives any interrupted turn (replay for a
// filesystem-only backend), and records a RESUME marker. A session with no prior SUSPEND (crash mid
// turn) falls back to re-provisioning from the session handle.
func (p *Placer) Resume(ctx context.Context, log eventlog.Store, sessionUID string) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	var inc api.Incarnation
	finish := observability.StartDebug(ctx, p.logger, "placement", "resume", "session_uid", sessionUID)
	defer func() {
		finish(err, "error_kind", placementErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	// Gate before Restore, exactly as Exec does before Create: Resume re-drives an interrupted turn,
	// so it must not reach a harness the backend cannot host.
	if _, err := p.admit(ctx, sessionUID); err != nil {
		return err
	}
	sourceFinished := observability.StartDebug(ctx, p.logger, "placement", "resolve_resume_source", "session_uid", sessionUID)
	ref, err := lastSuspendRef(log, sessionUID)
	if err != nil {
		sourceFinished(err, "error_kind", placementErrorKind(err))
		return err
	}
	sourceFinished(nil, "memory_snapshot", ref.Memory)
	inc, err = p.backend.Restore(ctx, ref)
	if err != nil {
		return err
	}
	dialFinished := observability.StartDebug(ctx, p.logger, "placement", "dial_harness",
		"session_uid", sessionUID,
		"incarnation_id", inc.ID,
		"runtime", inc.Runtime,
		"transport", addressTransport(inc.Address),
	)
	har, closeHarness, err := p.dial(inc.Address)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return err
	}
	dialFinished(nil)
	defer p.closeHarness(ctx, sessionUID, inc.ID, closeHarness)
	if err := p.recheck(ctx, sessionUID, har); err != nil {
		return err
	}
	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := log.NewFence()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return err
	}
	fenceFinished(nil)
	inc.FenceToken = fence
	c, err := controller.New(log, p.model, p.controllerOpts(fence, sessionUID, controller.Observer{})...)
	if err != nil {
		return err
	}
	if _, err := c.Resume(ctx, har); err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	// RESUME marker under the incarnation's fence (controller.Resume used the same token).
	appendFinished := observability.StartDebug(ctx, p.logger, "placement", "append_resume_event", "session_uid", sessionUID)
	_, err = log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleResume}})
	appendFinished(err, "error_kind", placementErrorKind(err))
	return err
}

// ForkChild is one child of a fan-out fork: the child's session UID and its (empty) log.
type ForkChild struct {
	UID string
	Log eventlog.Store
}

func (p *Placer) closeHarness(ctx context.Context, sessionUID, incarnationID string, closeHarness func() error) {
	finish := observability.StartDebug(ctx, p.logger, "placement", "close_harness",
		"session_uid", sessionUID,
		"incarnation_id", incarnationID,
	)
	err := closeHarness()
	finish(err, "error_kind", closeErrorKind(err))
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
	case errors.Is(err, ErrHarnessUnavailable):
		return "harness_unavailable"
	case errors.Is(err, eventlog.ErrConflict):
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
// of them branch from the identical state.
func (p *Placer) Fork(ctx context.Context, parent eventlog.Store, parentUID string, children []ForkChild, atSeq int64) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "fork",
		"parent_session_uid", parentUID,
		"children", len(children),
		"at_seq", atSeq,
	)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	ref, err := p.forkSource(ctx, parent, parentUID, atSeq)
	if err != nil {
		return err
	}
	// A failed fan-out must not strand compute. The child UIDs are minted per request and returned
	// only on success, so anything provisioned before the failure would otherwise be unreachable —
	// unstoppable workers nobody can name.
	created := make([]string, 0, len(children))
	for _, child := range children {
		if _, err := p.backend.Fork(ctx, ref, api.ForkOpts{ChildSessionUID: child.UID}); err != nil {
			p.rollbackFork(ctx, created)
			return err
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
			p.rollbackFork(ctx, created)
			return err
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
// MaxForkChildren × rollbackTimeout. That is bounded and cheap (no lock, no DB handle), and it is
// the right side of the trade: an overall cap tight enough to matter would abandon the tail of the
// unwind, stranding exactly the workers this exists to reclaim.
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

// forkSource produces the SnapshotRef the child is forked from.
//
// For a stateless harness that is just the parent's handle (nothing to clone). For a memory harness
// it is a FRESH snapshot of the parent, recorded as a SUSPEND lifecycle event so the ref stays
// recoverable from the tamper-evident chain — the same shape Suspend records. It deliberately calls
// backend.Snapshot rather than Placer.Suspend, because Suspend also Stops (deletes) the actor, which
// a fork's parent must survive.
//
// Checkpointing the parent is not undoable, so this is where a stateful fork commits: once it
// returns, the parent is cold with a SUSPEND event on its chain whether or not the children go on to
// provision. That is a recoverable state (Resume brings the parent back), not a leak, but it does
// mean a retry must fork at the new head rather than the original seq.
func (p *Placer) forkSource(ctx context.Context, parent eventlog.Store, parentUID string, atSeq int64) (api.SnapshotRef, error) {
	desc, err := p.admit(ctx, parentUID)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	if desc.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		return api.SnapshotRef{Local: parentUID}, nil
	}
	// Check the head BEFORE minting a fence. Minting is an autocommitted bump that supersedes any
	// in-flight writer, so doing it first would let a fork that is then REFUSED as unplaceable still
	// kill a turn racing on the parent — a request that changes nothing would leave the session worse
	// off than before it was asked.
	head, err := parent.Head()
	if err != nil {
		return api.SnapshotRef{}, err
	}
	// A memory snapshot captures the parent's RAM as it is NOW, so it can only realize a fork taken
	// at the log head. Branching a stateful session at a historical seq would pair an old prefix with
	// present-day RAM; refuse — with no side effects at all — instead of producing that mismatch.
	if atSeq != head {
		return api.SnapshotRef{}, fmt.Errorf("%w: harness %q requires %s, which can only fork at the log head (%d), not seq %d",
			ErrUnplaceable, desc.ID, desc.Capabilities.Resumability, head, atSeq)
	}
	// Past this point the fork is committing, so superseding the current writer is the intended
	// semantic (the same one Suspend has). Fencing before the snapshot means an in-flight turn cannot
	// advance the head underneath a checkpoint that is not undoable.
	fence, err := parent.NewFence()
	if err != nil {
		return api.SnapshotRef{}, err
	}
	// A turn could still have committed in the window before that fence landed. Re-check now, while
	// aborting is free: after the snapshot the parent is cold whether or not the fork proceeds.
	if head, err = parent.Head(); err != nil {
		return api.SnapshotRef{}, err
	}
	if atSeq != head {
		return api.SnapshotRef{}, fmt.Errorf("%w: parent advanced from seq %d to %d while the fork was being prepared", eventlog.ErrConflict, atSeq, head)
	}
	ref, err := p.backend.Snapshot(ctx, api.Incarnation{ID: parentUID}, api.SnapshotExternal)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	// Record the checkpoint before validating it: the parent is already cold, and the SUSPEND event
	// is what keeps that fact on the chain. CAS on atSeq — the seq the head check validated — rather
	// than a re-read head, so a writer that minted an even newer fence in the meantime aborts the
	// fork instead of silently widening the children's prefix past the RAM they were cloned from.
	if err := appendLifecycleAt(parent, atSeq, fence, api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}); err != nil {
		return api.SnapshotRef{}, err
	}
	// Only now refuse an unusable checkpoint. A memory-capable runtime that yields no external handle
	// cannot clone, and cold-booting the children instead is the silent divergence this whole path
	// exists to prevent.
	if ref.ExternalURI == "" {
		return api.SnapshotRef{}, fmt.Errorf("%w: runtime %q produced no external snapshot handle to clone from", ErrUnplaceable, desc.ID)
	}
	return ref, nil
}

// appendLifecycle mints a fresh fence (superseding any prior writer) and records a lifecycle event at
// the head observed after that fence. Used where the event has no sequence precondition (Suspend,
// Resume): fencing first is what makes the head stable, so the append cannot lose a CAS to a turn
// that was already in flight.
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

// lastSuspendRef returns the SnapshotRef from the most recent SUSPEND event THIS session wrote, or —
// when there is none (a crash mid-turn, not a clean suspend) — a trivial ref naming the session so a
// filesystem-only backend re-provisions and replays the journal.
//
// The ownership check is load-bearing: a forked child inherits its parent's log prefix verbatim, so a
// parent checkpoint can sit in the child's history. Restore keys off SnapshotRef.Local (the actor
// handle), so accepting an inherited ref would restore the PARENT's incarnation under the child's
// session — two sessions bound to one actor. Every backend records its own session UID in Local, so
// a ref that names another session is history, not this session's checkpoint.
func lastSuspendRef(log eventlog.Store, sessionUID string) (api.SnapshotRef, error) {
	recs, err := log.Read(1)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	for i := len(recs) - 1; i >= 0; i-- {
		ev := recs[i].Event
		if ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleSuspend && ev.Lifecycle.Snapshot != nil {
			if ev.Lifecycle.Snapshot.Local != sessionUID {
				continue // inherited from a forked-from parent, not this session's checkpoint
			}
			return *ev.Lifecycle.Snapshot, nil
		}
	}
	return api.SnapshotRef{Local: sessionUID}, nil
}
