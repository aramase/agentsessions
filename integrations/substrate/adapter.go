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
	"fmt"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/runtime/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
var _ substrate.SnapshotCloner = (*Adapter)(nil)

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
// (the STATELESS_REPLAY realization); boot=false restores the RAM+disk snapshot (the
// REQUIRES_MEMORY_SNAPSHOT realization, and how a forked child comes up on its clone).
func (ad *Adapter) ResumeActor(ctx context.Context, actor substrate.ActorRef, boot bool) (substrate.ActorInfo, error) {
	resp, err := ad.ctl.ResumeActor(ctx, &atepb.ResumeActorRequest{Actor: objectRef(actor), Boot: boot})
	if err != nil {
		return substrate.ActorInfo{}, err
	}
	return ad.actorInfo(resp.GetActor()), nil
}

// SuspendActor snapshots RAM+disk to durable storage and frees the worker, returning the resulting
// ActorSnapshot's name (the handle Fork tags and clones from).
func (ad *Adapter) SuspendActor(ctx context.Context, actor substrate.ActorRef) (string, error) {
	resp, err := ad.ctl.SuspendActor(ctx, &atepb.SuspendActorRequest{Actor: objectRef(actor)})
	if err != nil {
		return "", err
	}
	return snapshotName(resp.GetActor()), nil
}

// DeleteActor deletes a suspended actor.
func (ad *Adapter) DeleteActor(ctx context.Context, actor substrate.ActorRef) error {
	_, err := ad.ctl.DeleteActor(ctx, &atepb.DeleteActorRequest{Actor: objectRef(actor)})
	return err
}

// GetActor reads the actor's current state. A missing actor is reported as substrate.ErrActorNotFound
// so the backend can tell "never placed" (create it) apart from "already placed" (attach to it)
// without inspecting gRPC codes itself.
func (ad *Adapter) GetActor(ctx context.Context, actor substrate.ActorRef) (substrate.ActorInfo, error) {
	a, err := ad.ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: objectRef(actor)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return substrate.ActorInfo{}, fmt.Errorf("%w: %s/%s", substrate.ErrActorNotFound, actor.Atespace, actor.Name)
		}
		return substrate.ActorInfo{}, err
	}
	return ad.actorInfo(a), nil
}

func snapshotObjectRef(s substrate.SnapshotID) *atepb.ObjectRef {
	return &atepb.ObjectRef{Atespace: s.Atespace, Name: s.Name}
}

// TagSnapshot gives an ActorSnapshot a stable, atespace-owned name, which is also the snapshot's
// retention pin. The tag is created with the default ATESPACE scope: it may initialize actors only in
// its owning atespace, which is all a fork needs because the child is created alongside its parent.
// Publishing (cross-atespace reuse) is deliberately not done here.
func (ad *Adapter) TagSnapshot(ctx context.Context, snapshot, tag substrate.SnapshotID) error {
	_, err := ad.ctl.TagActorSnapshot(ctx, &atepb.TagActorSnapshotRequest{
		Snapshot: &atepb.ActorSnapshotRef{
			Reference: &atepb.ActorSnapshotRef_Snapshot{Snapshot: snapshotObjectRef(snapshot)},
		},
		Tag: &atepb.ActorSnapshotTag{
			Metadata: &atepb.ResourceMetadata{Atespace: tag.Atespace, Name: tag.Name},
			Scope:    atepb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	return err
}

// CreateActorFromSnapshot creates an actor initialized from the snapshot behind tag. Substrate accepts
// a source snapshot only BY TAG — a canonical snapshot reference is rejected with FailedPrecondition —
// and requires the actor's template to be the snapshot's exact source ActorTemplate. The clone is
// created cold; the caller resumes it with boot=false to restore the cloned RAM.
func (ad *Adapter) CreateActorFromSnapshot(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef, tag substrate.SnapshotID) error {
	_, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:               &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplateNamespace: template.Namespace,
			ActorTemplateName:      template.Name,
		},
		SourceSnapshot: &atepb.ActorSnapshotRef{
			Reference: &atepb.ActorSnapshotRef_Tag{Tag: snapshotObjectRef(tag)},
		},
	})
	return err
}

// DeleteSnapshotTag removes a tag, releasing its retention pin on the snapshot.
func (ad *Adapter) DeleteSnapshotTag(ctx context.Context, tag substrate.SnapshotID) error {
	_, err := ad.ctl.DeleteActorSnapshotTag(ctx, &atepb.DeleteActorSnapshotTagRequest{Tag: snapshotObjectRef(tag)})
	return err
}

// actorInfo maps a substrate Actor onto the narrowed ActorInfo the backend needs. MeshDNS is the
// atenet router authority; it is reported for diagnostics only, because the router proxies HTTP/1.1
// to actors and cannot carry gRPC — the backend dials PodIP directly instead. ateom_pod_ip is the
// worker pod hosting the actor; substrate clears it whenever the actor holds no worker (SUSPENDED,
// PAUSED, CRASHED).
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

// snapshotName returns the durable ActorSnapshot created by a suspend. Substrate creates exactly one
// immutable ActorSnapshot per successful suspend and points Actor.latest_snapshot at it; the physical
// storage location is private, so the atespace-scoped NAME is the only durable handle a client gets.
//
// There is deliberately no fallback to Actor.in_progress_snapshot: that field holds a STORAGE URI
// ("<snapshotsConfig.location>/snapshots/<id>"), not a resource name, so it is neither a valid tag
// target nor interchangeable with a snapshot name. It is also only still set when the suspend did not
// register a snapshot at all. Returning "" instead lets the backend refuse the fork loudly rather than
// commit an unusable handle to the session's chain.
func snapshotName(a *atepb.Actor) string {
	return a.GetLatestSnapshot().GetName()
}
