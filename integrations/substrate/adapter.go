// Package ateadapter implements the frozen agentsessions runtime/substrate.ControlClient over the
// REAL agent-substrate ate-api Control gRPC (github.com/agent-substrate/substrate/pkg/proto/ateapipb).
//
// Neutrality (hard rule): this lives in its OWN Go module so the core agentsessions module imports
// zero substrate code and its go.mod stays clean. runtime/substrate.ControlClient is the seam —
// substrate is one backend that adapts to our interface, not a dependency of the neutral core.
//
// Mapping (agentsessions-runtime-on-agent-substrate.md §2, agentsessions-conformance-on-substrate.md
// §4): Create->CreateActor; Resume->ResumeActor{boot}; Suspend->SuspendActor; Delete->DeleteActor;
// Status->GetActor. The interface is frozen our side; this is a pure translation of the generated
// client.
package ateadapter

import (
	"context"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/runtime/substrate"
	"google.golang.org/grpc"
)

// DefaultDNSSuffix is the atenet mesh suffix an actor is reachable at: <actor>.<atespace>.<suffix>.
// Mirrors substrate internal/resources.ActorDNSSuffix (an internal package, not importable here).
const DefaultDNSSuffix = "actors.resources.substrate.ate.dev"

// Adapter implements substrate.ControlClient over the ate-api Control gRPC client.
type Adapter struct {
	ctl       atepb.ControlClient
	dnsSuffix string
}

var _ substrate.ControlClient = (*Adapter)(nil)

// New wraps an ate-api Control client over conn. The caller owns dialing + auth (cert vs token — the
// integration spike settles this, so it is not baked in here). dnsSuffix defaults to
// DefaultDNSSuffix when empty.
func New(conn grpc.ClientConnInterface, dnsSuffix string) *Adapter {
	if dnsSuffix == "" {
		dnsSuffix = DefaultDNSSuffix
	}
	return &Adapter{ctl: atepb.NewControlClient(conn), dnsSuffix: dnsSuffix}
}

// FromClient wraps an already-constructed ate-api Control client (used in tests with a double).
func FromClient(ctl atepb.ControlClient, dnsSuffix string) *Adapter {
	if dnsSuffix == "" {
		dnsSuffix = DefaultDNSSuffix
	}
	return &Adapter{ctl: ctl, dnsSuffix: dnsSuffix}
}

func objectRef(a substrate.ActorRef) *atepb.ObjectRef {
	return &atepb.ObjectRef{Atespace: a.Atespace, Name: a.Name}
}

// CreateActor creates an actor deriving from an ActorTemplate (namespace + name).
func (ad *Adapter) CreateActor(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef) error {
	_, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:               &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplateNamespace: template.Namespace,
			ActorTemplateName:      template.Name,
		},
	})
	return err
}

// ResumeActor schedules the actor onto a worker. boot=true cold-boots, bypassing any golden snapshot
// (STATELESS_REPLAY / axis 1); boot=false restores the RAM+disk snapshot (REQUIRES_MEMORY_SNAPSHOT /
// axis 2).
func (ad *Adapter) ResumeActor(ctx context.Context, actor substrate.ActorRef, boot bool) (substrate.ActorInfo, error) {
	resp, err := ad.ctl.ResumeActor(ctx, &atepb.ResumeActorRequest{Actor: objectRef(actor), Boot: boot})
	if err != nil {
		return substrate.ActorInfo{}, err
	}
	return ad.actorInfo(resp.GetActor()), nil
}

// SuspendActor snapshots RAM+disk to the snapshot store and frees the worker, returning the snapshot
// location.
func (ad *Adapter) SuspendActor(ctx context.Context, actor substrate.ActorRef) (string, error) {
	resp, err := ad.ctl.SuspendActor(ctx, &atepb.SuspendActorRequest{Actor: objectRef(actor)})
	if err != nil {
		return "", err
	}
	return snapshotURI(resp.GetActor()), nil
}

// DeleteActor deletes a suspended actor.
func (ad *Adapter) DeleteActor(ctx context.Context, actor substrate.ActorRef) error {
	_, err := ad.ctl.DeleteActor(ctx, &atepb.DeleteActorRequest{Actor: objectRef(actor)})
	return err
}

// GetActor reports the actor's current state.
func (ad *Adapter) GetActor(ctx context.Context, actor substrate.ActorRef) (substrate.ActorInfo, error) {
	a, err := ad.ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: objectRef(actor)})
	if err != nil {
		return substrate.ActorInfo{}, err
	}
	return ad.actorInfo(a), nil
}

// actorInfo maps a substrate Actor onto the narrowed ActorInfo the backend needs. MeshDNS is the
// atenet router authority the controller dials. Source analysis of the Envoy router config indicates
// h2c end-to-end (explicit HTTP/2 upstream to the actor + AUTO/h2c downstream), so a harnesswire gRPC
// harness should be reachable over the mesh with no HTTP/1 shim — pending the first CI run.
func (ad *Adapter) actorInfo(a *atepb.Actor) substrate.ActorInfo {
	info := substrate.ActorInfo{
		Status: actorStatus(a.GetStatus()),
		PodIP:  a.GetAteomPodIp(),
	}
	if m := a.GetMetadata(); m != nil && m.GetName() != "" {
		info.MeshDNS = m.GetName() + "." + m.GetAtespace() + "." + ad.dnsSuffix
	}
	return info
}

func actorStatus(s atepb.Actor_Status) substrate.ActorStatus {
	switch s {
	case atepb.Actor_STATUS_RUNNING:
		return substrate.StatusRunning
	case atepb.Actor_STATUS_SUSPENDED, atepb.Actor_STATUS_PAUSED:
		return substrate.StatusSuspended
	case atepb.Actor_STATUS_CRASHED:
		return substrate.StatusTerminated
	default:
		return substrate.StatusUnknown
	}
}

// snapshotURI extracts the external snapshot location from a suspended actor (LatestSnapshotInfo, or
// the in-progress handle while SUSPENDING).
func snapshotURI(a *atepb.Actor) string {
	if info := a.GetLatestSnapshotInfo(); info != nil {
		if ext := info.GetExternal(); ext != nil {
			return ext.GetSnapshotUriPrefix()
		}
	}
	return a.GetInProgressSnapshot()
}
