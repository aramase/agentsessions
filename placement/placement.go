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

// ErrSessionBusy is returned when Exec, Suspend, or Resume overlaps another operation on the same
// session through this Placer or its Registry. No compute or journal transition is attempted;
// session.Service surfaces it as codes.Aborted, independently of any backend transport status.
var ErrSessionBusy = errors.New("placement: another operation is in progress for this session")

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

	// Private for a standalone Placer; NewRegistry wires one shared guard before use.
	guard *sessionGuard

	mu   sync.Mutex
	live map[string]map[*liveHarness]struct{} // session UID -> harness connections open in this Placer
	// checkpoints counts the stateful-fork checkpoints of each session UID in progress. While it is
	// non-zero no harness connection can be opened for the session.
	checkpoints map[string]int
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
		dial:    DefaultDial,
		logger:  slog.New(slog.DiscardHandler),
		guard:   new(sessionGuard),
	}
	for _, o := range opts {
		o(p)
	}
	if p.logger == nil {
		p.logger = slog.New(slog.DiscardHandler)
	}
	return p
}

// trySessionLock serializes Exec/Suspend/Resume through their compute and journal transitions.
// Refuse overlap rather than queueing a request whose cursor may be stale by the time it runs.
func (p *Placer) trySessionLock(sessionUID string) (func(), error) {
	return p.guard.tryLock(sessionUID)
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

	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return api.Incarnation{}, err
	}
	defer release()

	if _, err := p.admit(ctx, sessionUID); err != nil {
		return api.Incarnation{}, err
	}

	// Refuse before provisioning: on substrate, Create on an actor a stateful fork is checkpointing
	// would either fail or wake it. openHarness re-checks atomically; this only saves the round trip.
	if err = p.refuseDuringCheckpoint(sessionUID); err != nil {
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
	ctx, live, err := p.openHarness(ctx, sessionUID, inc)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return inc, err
	}
	dialFinished(nil)
	defer p.releaseHarness(ctx, sessionUID, inc.ID, live)
	har := live.harness
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
	// A checkpoint may have begun while the fence was being minted. Stop before the controller writes
	// anything under a fence the checkpoint's does not cover.
	if err := live.checkSuperseded(ctx); err != nil {
		return inc, err
	}
	copts := append(p.controllerOpts(fence, sessionUID, cfg.observer),
		controller.WithStart(cfg.config, cfg.resumeFromSeq))
	c, err := controller.New(log, p.model, copts...)
	if err != nil {
		return inc, err
	}
	if err := c.Exec(ctx, har, inputs, expectedLastSeq); err != nil {
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
	if err != nil && errors.Is(context.Cause(ctx), errSuperseded) {
		// A checkpoint of the session ended this turn's connection during the check. Report the
		// checkpoint, as turnError does for a running turn, and format the check's error with %v so
		// that ErrAdmissionInterrupted is not in the chain: the call is retryable (ABORTED), not
		// cancelled by its caller.
		return fmt.Errorf("%w (harness check ended with: %v)", errSuperseded, err)
	}
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

// ErrCheckpointing is returned when a turn cannot proceed because its session is being
// checkpointed as the parent of a stateful Fork: either the turn tried to open a harness
// connection while the checkpoint ran, or the checkpoint ended the connection the turn was using.
// It is retryable once the checkpoint ends; session.Service maps it to codes.Aborted.
var ErrCheckpointing = errors.New("placement: session is being checkpointed")

// errSuperseded is the cancellation cause of a turn whose harness connection a checkpoint ended. The
// checkpoint mints its fence before it ends the connection, so the turn has been superseded exactly
// as a fenced writer is, and the error says both.
var errSuperseded = fmt.Errorf("%w: in-flight turn superseded and its harness connection closed: %w", ErrCheckpointing, eventlog.ErrFenced)

// turnError reports why a turn failed. When a checkpoint ended the turn's harness connection, the
// error the turn itself surfaced is incidental (a cancelled model call, a closed stream), so the
// checkpoint is reported as the cause and the incidental error is kept for diagnosis.
func turnError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, errSuperseded) && !errors.Is(err, errSuperseded) {
		return fmt.Errorf("%w (turn ended with: %w)", cause, err)
	}
	return err
}

// liveHarness is one open harness connection and the means to end it from outside the turn that
// owns it.
type liveHarness struct {
	harness api.Harness
	cancel  context.CancelCauseFunc
	once    sync.Once
	close   func() error
	err     error
	// superseded is set, under Placer.mu, by the checkpoint that collects this connection, BEFORE
	// that checkpoint mints its fence. A turn whose fence is newer than the checkpoint's is not
	// fenced by it, but is guaranteed to observe this flag, so it checks the flag after minting.
	superseded atomic.Bool
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
	l := &liveHarness{harness: har, cancel: cancel, close: closeHarness}
	p.mu.Lock()
	if p.checkpoints[sessionUID] > 0 {
		p.mu.Unlock()
		// Nothing was sent on the connection, so the close error carries no information.
		_ = l.end(context.Canceled)
		return ctx, nil, fmt.Errorf("%w: session %q", ErrCheckpointing, sessionUID)
	}
	if p.live == nil {
		p.live = map[string]map[*liveHarness]struct{}{}
	}
	if p.live[sessionUID] == nil {
		p.live[sessionUID] = map[*liveHarness]struct{}{}
	}
	p.live[sessionUID][l] = struct{}{}
	p.mu.Unlock()
	return ctx, l, nil
}

// releaseHarness deregisters and closes a turn's harness connection when the turn returns.
func (p *Placer) releaseHarness(ctx context.Context, sessionUID, incarnationID string, l *liveHarness) {
	p.mu.Lock()
	delete(p.live[sessionUID], l)
	if len(p.live[sessionUID]) == 0 {
		delete(p.live, sessionUID)
	}
	p.mu.Unlock()
	finish := observability.StartDebug(context.WithoutCancel(ctx), p.logger, "placement", "close_harness",
		"session_uid", sessionUID,
		"incarnation_id", incarnationID,
	)
	err := l.end(context.Canceled)
	finish(err, "error_kind", closeErrorKind(err))
}

// refuseDuringCheckpoint fails with ErrCheckpointing while a checkpoint of the session is in
// progress. It is an early exit only; openHarness is where the refusal is enforced.
func (p *Placer) refuseDuringCheckpoint(sessionUID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkpoints[sessionUID] > 0 {
		return fmt.Errorf("%w: session %q", ErrCheckpointing, sessionUID)
	}
	return nil
}

// beginCheckpoint marks the session as being checkpointed, so openHarness refuses new connections
// for it, and returns the connections already open, each marked superseded. Taking the mark and the
// set under one lock is what leaves no window for a turn to slip a stream in between. The caller
// mints its fence after this returns, so a turn that mints a newer fence still sees the superseded
// mark (checkSuperseded) and writes nothing. A connection stays marked even if the checkpoint
// aborts. Every call must be paired with endCheckpoint.
func (p *Placer) beginCheckpoint(sessionUID string) []*liveHarness {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkpoints == nil {
		p.checkpoints = map[string]int{}
	}
	p.checkpoints[sessionUID]++
	open := make([]*liveHarness, 0, len(p.live[sessionUID]))
	for l := range p.live[sessionUID] {
		l.superseded.Store(true)
		open = append(open, l)
	}
	return open
}

// endCheckpoint lifts the mark beginCheckpoint set.
func (p *Placer) endCheckpoint(sessionUID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkpoints[sessionUID]--; p.checkpoints[sessionUID] <= 0 {
		delete(p.checkpoints, sessionUID)
	}
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
// It covers connections opened by THIS Placer only. A turn driven by another process is fenced by
// the log as before, but its stream stays open until that turn next touches the log or returns.
func (p *Placer) endHarnesses(ctx context.Context, sessionUID string, open []*liveHarness) {
	if len(open) == 0 {
		return
	}
	finish := observability.StartDebug(ctx, p.logger, "placement", "end_harness_streams",
		"session_uid", sessionUID,
		"connections", len(open),
	)
	var errs []error
	for _, l := range open {
		errs = append(errs, l.end(errSuperseded))
	}
	err := errors.Join(errs...)
	finish(err, "error_kind", closeErrorKind(err))
}

// Suspend transitions the incarnation to cold via SnapshotExternal, then records the SnapshotRef
// in a SUSPEND lifecycle event so Resume can recover it from the tamper-evident chain (§5.1).
// Snapshot owns dedicated compute release and retains the handles Restore needs; Stop would
// destructively tear them down. Snapshot keys on the session id, so a minimal incarnation suffices.
//
// Suspend does not interrupt a running turn. While an Exec or Resume of the session runs through
// this Placer or its Registry, Suspend fails with ErrSessionBusy without touching compute or the
// journal, and the caller retries once the turn has returned. So the checkpoint never runs under an
// open harness stream of a turn this Registry drives.
func (p *Placer) Suspend(ctx context.Context, log eventlog.Store, sessionUID string) (ref api.SnapshotRef, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "suspend", "session_uid", sessionUID)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	defer release()

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

	release, err := p.trySessionLock(sessionUID)
	if err != nil {
		return err
	}
	defer release()

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
	if err := p.refuseDuringCheckpoint(sessionUID); err != nil {
		return err
	}
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
	ctx, live, err := p.openHarness(ctx, sessionUID, inc)
	if err != nil {
		dialFinished(err, "error_kind", "harness_dial_failed")
		return err
	}
	dialFinished(nil)
	defer p.releaseHarness(ctx, sessionUID, inc.ID, live)
	har := live.harness
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
	if err := live.checkSuperseded(ctx); err != nil {
		return err
	}
	c, err := controller.New(log, p.model, p.controllerOpts(fence, sessionUID, controller.Observer{})...)
	if err != nil {
		return err
	}
	if _, err := c.Resume(ctx, har); err != nil {
		return turnError(ctx, err)
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
	case errors.Is(err, ErrCheckpointing):
		return "checkpointing"
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
// backend.Snapshot rather than Placer.Suspend so the fork can fence before checkpointing and commit
// the SUSPEND event with a CAS on the validated fork point.
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
	// Past this point the fork is committing, so superseding the current writer is intended.
	// Unlike Suspend, Fork is not covered by the session guard. Fencing before the snapshot means
	// an in-flight turn cannot advance the head underneath a checkpoint that is not undoable. The
	// checkpoint mark goes first, so no new turn opens a harness stream on the parent until the
	// checkpoint is recorded.
	open := p.beginCheckpoint(parentUID)
	defer p.endCheckpoint(parentUID)
	if _, err := parent.NewFence(); err != nil {
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
	p.endHarnesses(ctx, parentUID, open)
	// A turn this Placer had open may have minted a newer fence between ours and endHarnesses. It
	// wrote nothing (checkSuperseded), but its fence would leave ours stale and fail the SUSPEND
	// append after the parent is already cold. Every local stream is ended and no new one can open,
	// so mint the fence the record is written under now, and re-check the head while aborting is
	// still free.
	fence, err := parent.NewFence()
	if err != nil {
		return api.SnapshotRef{}, err
	}
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
