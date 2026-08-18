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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/observability"
)

// ErrUnplaceable is returned when a harness requires a capability the chosen backend cannot provide
// (e.g. REQUIRES_MEMORY_SNAPSHOT on a filesystem-only pod). The gate runs BEFORE Create, so an
// unplaceable harness never provisions compute or writes to the log; session.Service surfaces it as
// codes.FailedPrecondition.
var ErrUnplaceable = errors.New("placement: harness cannot be placed on this runtime")

// Backend is the compute Runtime the Placer drives. It exposes the harness DESCRIPTOR so the Placer
// can gate CanPlace before Create (placement must not provision compute to learn a harness's needs);
// the harness itself is reached by dialing Incarnation.Address (Harness.Connect) — the one dial path
// both runtime/local and substrate use. runtime/local satisfies this.
type Backend interface {
	api.Runtime
	Describe(ctx context.Context) (api.Descriptor, error)
}

// Placer owns the incarnation lifecycle: Create the compute, mint+bind the fence, drive the controller.
type Placer struct {
	backend Backend
	model   controller.ModelFunc
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
func (p *Placer) Exec(ctx context.Context, log eventlog.Store, sessionUID string, inputs []api.Message, expectedLastSeq int64) (inc api.Incarnation, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "exec",
		"session_uid", sessionUID,
		"expected_last_seq", expectedLastSeq,
		"input_count", len(inputs),
	)
	defer func() {
		finish(err, "error_kind", placementErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	// Placement gate (honest degradation): read the harness descriptor in-process and refuse a
	// harness the backend cannot host BEFORE provisioning any compute or writing to the log — e.g. a
	// REQUIRES_MEMORY_SNAPSHOT harness on a filesystem-only backend.
	resolveFinished := observability.StartDebug(ctx, p.logger, "placement", "resolve_execution_path", "session_uid", sessionUID)
	desc, err := p.backend.Describe(ctx)
	if err != nil {
		resolveFinished(err, "error_kind", "describe_harness_failed")
		return api.Incarnation{}, err
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
		return api.Incarnation{}, err
	}
	resolveFinished(nil,
		"decision", "accepted",
		"harness_id", desc.ID,
		"resumability", desc.Capabilities.Resumability,
		"runtime_memory_snapshot", p.backend.Capabilities().MemorySnapshot,
	)

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

	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := log.NewFence()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return inc, err
	}
	fenceFinished(nil)
	inc.FenceToken = fence // Placer-owned: the incarnation carries the token Suspend/Resume will need
	c, err := controller.New(
		log,
		p.model,
		controller.WithFence(fence),
		controller.WithLogger(p.logger),
		controller.WithSessionUID(sessionUID),
	)
	if err != nil {
		return inc, err
	}
	if err := c.Exec(ctx, har, inputs, expectedLastSeq); err != nil {
		return inc, err
	}
	return inc, nil
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
// HTTP/1.1-only atenet router. No TLS: the harness terminates plaintext gRPC, matching google/ax.
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
	fenceFinished := observability.StartDebug(ctx, p.logger, "placement", "mint_fence", "session_uid", sessionUID)
	fence, err := log.NewFence()
	if err != nil {
		fenceFinished(err, "error_kind", "new_fence_failed")
		return err
	}
	fenceFinished(nil)
	inc.FenceToken = fence
	c, err := controller.New(
		log,
		p.model,
		controller.WithFence(fence),
		controller.WithLogger(p.logger),
		controller.WithSessionUID(sessionUID),
	)
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

// Fork provisions the child's compute (replay-fork for filesystem-only backends) and copies the
// parent's log prefix up to atSeq so the child shares the parent's hash chain.
func (p *Placer) Fork(ctx context.Context, parent, child eventlog.Store, parentUID, childUID string, atSeq int64) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.StartDebug(ctx, p.logger, "placement", "fork",
		"parent_session_uid", parentUID,
		"child_session_uid", childUID,
		"at_seq", atSeq,
	)
	defer func() { finish(err, "error_kind", placementErrorKind(err)) }()

	if _, err := p.backend.Fork(ctx, api.SnapshotRef{Local: parentUID}, api.ForkOpts{ChildSessionUID: childUID}); err != nil {
		return err
	}
	copyFinished := observability.StartDebug(ctx, p.logger, "placement", "copy_fork_log",
		"parent_session_uid", parentUID,
		"child_session_uid", childUID,
		"at_seq", atSeq,
	)
	err = controller.Fork(parent, child, atSeq)
	copyFinished(err, "error_kind", placementErrorKind(err))
	return err
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

// appendLifecycle mints a fresh fence (superseding any prior writer) and records a lifecycle event.
func appendLifecycle(log eventlog.Store, lc api.Lifecycle) error {
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	_, err = log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &lc})
	return err
}

// lastSuspendRef returns the SnapshotRef from the most recent SUSPEND event, or — when there is none
// (a crash mid-turn, not a clean suspend) — a trivial ref naming the session so a filesystem-only
// backend re-provisions and replays the journal.
func lastSuspendRef(log eventlog.Store, sessionUID string) (api.SnapshotRef, error) {
	recs, err := log.Read(1)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	for i := len(recs) - 1; i >= 0; i-- {
		ev := recs[i].Event
		if ev.Kind == api.EventLifecycle && ev.Lifecycle != nil && ev.Lifecycle.Kind == api.LifecycleSuspend && ev.Lifecycle.Snapshot != nil {
			return *ev.Lifecycle.Snapshot, nil
		}
	}
	return api.SnapshotRef{Local: sessionUID}, nil
}
