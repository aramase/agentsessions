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
// Restore→ResumeActor{boot:false}; Stop→SuspendActor+DeleteActor; Status→GetActor; Fork→replay-fork.
// Capabilities report MemorySnapshot=true — the headline differentiator vs a plain pod, which lets
// substrate host REQUIRES_MEMORY_SNAPSHOT harnesses a pod must refuse (the honest-degradation proof).
package substrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

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
	// SuspendActor snapshots RAM+disk to external storage and frees the worker, returning the
	// snapshot location.
	SuspendActor(ctx context.Context, actor ActorRef) (snapshotURI string, err error)
	DeleteActor(ctx context.Context, actor ActorRef) error
	GetActor(ctx context.Context, actor ActorRef) (ActorInfo, error)
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

// Create provisions a cold actor for the session and boots it onto a worker.
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (inc api.Incarnation, err error) {
	sessionUID := s.SessionUID
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "create_compute",
		"session_uid", sessionUID,
		"atespace", b.atespace,
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()
	ref := b.ref(s.SessionUID)
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
	info, err := b.ctl.ResumeActor(ctx, ref, true) // cold boot (STATELESS_REPLAY realization)
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
	// Local carries the actor name (the handle Restore/ResumeActor needs); ExternalURI is the blob.
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

// Fork is realized as a replay-fork: substrate has no clone-from-arbitrary-snapshot API, so a fresh
// cold actor is created for the child and the host replays the journal into it (spike §2, §7).
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (inc api.Incarnation, err error) {
	finish := observability.StartDebug(ctx, b.logger, "runtime.substrate", "fork_compute",
		"parent_actor", ref.Local,
		"child_actor", opts.ChildSessionUID,
		"atespace", b.atespace,
		"strategy", "replay",
	)
	defer func() {
		finish(err, "error_kind", substrateErrorKind(err), "incarnation_id", inc.ID, "runtime", inc.Runtime)
	}()

	child := opts.ChildSessionUID
	if child == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: fork requires a child session uid")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: child})
}

// Stop suspends then deletes the actor (substrate requires SUSPENDED before delete).
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
// vs a plain pod; CoW fork, attestation, and GPU-state capture are not surfaced by the control API
// today (spike §3/§7).
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
