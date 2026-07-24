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
	"fmt"

	"github.com/aramase/agentsessions/api"
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
	ctl      ControlClient
	atespace string
	template ObjectRef // the ActorTemplate whose OCI image is the harness
}

var _ api.Runtime = (*Backend)(nil)

// New builds the backend targeting one atespace and harness ActorTemplate.
func New(ctl ControlClient, atespace string, template ObjectRef) *Backend {
	return &Backend{ctl: ctl, atespace: atespace, template: template}
}

func (b *Backend) ref(name string) ActorRef { return ActorRef{Atespace: b.atespace, Name: name} }

// Create provisions a cold actor for the session and boots it onto a worker.
func (b *Backend) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	ref := b.ref(s.SessionUID)
	if err := b.ctl.CreateActor(ctx, ref, b.template); err != nil {
		return api.Incarnation{}, fmt.Errorf("substrate: create actor: %w", err)
	}
	info, err := b.ctl.ResumeActor(ctx, ref, true) // cold boot (STATELESS_REPLAY realization)
	if err != nil {
		return api.Incarnation{}, fmt.Errorf("substrate: resume actor: %w", err)
	}
	return b.incarnation(s.SessionUID, info), nil
}

// Snapshot suspends the actor: RAM+disk snapshot to external storage, worker freed. Only the
// external (cold/suspend) kind is supported; substrate has no node-local warm checkpoint API today.
func (b *Backend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	if kind == api.SnapshotLocal {
		return api.SnapshotRef{}, fmt.Errorf("substrate: local (warm) snapshot not supported; use EXTERNAL")
	}
	uri, err := b.ctl.SuspendActor(ctx, b.ref(in.ID))
	if err != nil {
		return api.SnapshotRef{}, fmt.Errorf("substrate: suspend actor: %w", err)
	}
	// Local carries the actor name (the handle Restore/ResumeActor needs); ExternalURI is the blob.
	return api.SnapshotRef{Local: in.ID, ExternalURI: uri, Memory: true}, nil
}

// Restore brings the actor back from its snapshot onto a (possibly different) worker.
func (b *Backend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	name := ref.Local
	if name == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: snapshot ref missing actor name")
	}
	info, err := b.ctl.ResumeActor(ctx, b.ref(name), false) // restore snapshot
	if err != nil {
		return api.Incarnation{}, fmt.Errorf("substrate: resume (restore) actor: %w", err)
	}
	return b.incarnation(name, info), nil
}

// Fork is realized as a replay-fork: substrate has no clone-from-arbitrary-snapshot API, so a fresh
// cold actor is created for the child and the host replays the journal into it (spike §2, §7).
func (b *Backend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (api.Incarnation, error) {
	child := opts.ChildSessionUID
	if child == "" {
		return api.Incarnation{}, fmt.Errorf("substrate: fork requires a child session uid")
	}
	return b.Create(ctx, &api.SessionSpec{SessionUID: child})
}

// Stop suspends then deletes the actor (substrate requires SUSPENDED before delete).
func (b *Backend) Stop(ctx context.Context, in api.Incarnation) error {
	ref := b.ref(in.ID)
	if _, err := b.ctl.SuspendActor(ctx, ref); err != nil {
		return fmt.Errorf("substrate: suspend before delete: %w", err)
	}
	if err := b.ctl.DeleteActor(ctx, ref); err != nil {
		return fmt.Errorf("substrate: delete actor: %w", err)
	}
	return nil
}

// Status maps the actor's substrate status onto the agentsessions compute-lifecycle axis.
func (b *Backend) Status(ctx context.Context, in api.Incarnation) (api.ComputeState, error) {
	info, err := b.ctl.GetActor(ctx, b.ref(in.ID))
	if err != nil {
		return api.ComputeState(""), fmt.Errorf("substrate: get actor: %w", err)
	}
	switch info.Status {
	case StatusRunning:
		return api.ComputeLive, nil
	case StatusSuspended:
		return api.ComputeCold, nil
	case StatusTerminated:
		return api.ComputeTerminated, nil
	default:
		return api.ComputeNone, nil
	}
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

func (b *Backend) incarnation(uid string, info ActorInfo) api.Incarnation {
	return api.Incarnation{
		ID:      uid,
		Worker:  info.PodIP,
		Address: info.MeshDNS,
		Runtime: "substrate",
	}
}
