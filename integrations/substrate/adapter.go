// Package ateadapter implements the frozen agentsessions runtime/substrate.ControlClient over the
// REAL agent-substrate ate-api Control gRPC (github.com/agent-substrate/substrate/pkg/proto/ateapipb).
//
// Neutrality (hard rule): this lives in its OWN Go module so the core agentsessions module imports
// zero substrate code and its go.mod stays clean. runtime/substrate.ControlClient is the seam —
// substrate is one backend that adapts to our interface, not a dependency of the neutral core.
//
// Mapping: Create->CreateActor; Resume->ResumeActor; Suspend->SuspendActor; Delete->DeleteActor;
// Status->GetActor; clone->CreateTag{source_actor}+CreateActor{source_tag}; release->DeleteTag. The
// interface is frozen our side; this is a pure translation of the generated client.
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

// Adapter implements substrate.ControlClient over the ate-api Control gRPC client.
type Adapter struct {
	ctl atepb.ControlClient
}

var _ substrate.ControlClient = (*Adapter)(nil)
var _ substrate.SnapshotCloner = (*Adapter)(nil)

// New wraps an ate-api Control client over conn. The caller owns dialing and authentication.
func New(conn grpc.ClientConnInterface) *Adapter {
	return &Adapter{ctl: atepb.NewControlClient(conn)}
}

// FromClient wraps an already-constructed ate-api Control client (used in tests with a double).
func FromClient(ctl atepb.ControlClient) *Adapter {
	return &Adapter{ctl: ctl}
}

func objectRef(a substrate.ActorRef) *atepb.ObjectRef {
	return &atepb.ObjectRef{Atespace: a.Atespace, Name: a.Name}
}

func templateRef(t substrate.ObjectRef) *atepb.ObjectRef {
	return &atepb.ObjectRef{Atespace: t.Atespace, Name: t.Name}
}

func tagRef(t substrate.SnapshotID) *atepb.ObjectRef {
	return &atepb.ObjectRef{Atespace: t.Atespace, Name: t.Name}
}

// CreateActor creates an actor from an ActorTemplate (atespace + name). With no source tag, substrate
// seeds it from the template's golden snapshot when one is built.
func (ad *Adapter) CreateActor(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef) error {
	_, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:      &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplate: templateRef(template),
		},
	})
	return err
}

// ResumeActor schedules the actor onto a worker. Substrate restores the actor's own external
// snapshot when it holds one (including a tag's, borrowed at creation) and cold-boots the template
// spec otherwise; the request carries no boot flag.
func (ad *Adapter) ResumeActor(ctx context.Context, actor substrate.ActorRef) (substrate.ActorInfo, error) {
	resp, err := ad.ctl.ResumeActor(ctx, &atepb.ResumeActorRequest{Actor: objectRef(actor)})
	if err != nil {
		return substrate.ActorInfo{}, err
	}
	return actorInfo(resp.GetActor()), nil
}

// SuspendActor snapshots the actor to durable storage and frees the worker, returning the URI of the
// external snapshot the actor now holds.
func (ad *Adapter) SuspendActor(ctx context.Context, actor substrate.ActorRef) (string, error) {
	resp, err := ad.ctl.SuspendActor(ctx, &atepb.SuspendActorRequest{Actor: objectRef(actor)})
	if err != nil {
		return "", err
	}
	return resp.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri(), nil
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
	return actorInfo(a), nil
}

// TagActor tags the external snapshot the suspended source actor holds. Substrate copies the snapshot
// into tag-owned storage inside the call, so the tag is usable as soon as it returns. The tag uses
// the ATESPACE scope: it may seed actors only in the source's atespace, which is all a fork needs
// because the child is created alongside its parent. Publishing (cross-atespace reuse) is
// deliberately not done here.
//
// A name already taken, whether by a finished tag or one a failed create left pending, is reported
// as substrate.ErrTagExists, so the backend does not delete a tag this call did not reserve.
func (ad *Adapter) TagActor(ctx context.Context, source substrate.ActorRef, tag substrate.SnapshotID) error {
	_, err := ad.ctl.CreateTag(ctx, &atepb.CreateTagRequest{
		Tag: &atepb.Tag{
			Metadata:    &atepb.ResourceMetadata{Atespace: tag.Atespace, Name: tag.Name},
			Scope:       atepb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: objectRef(source),
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf("%w: %s/%s: %w", substrate.ErrTagExists, tag.Atespace, tag.Name, err)
	}
	return err
}

// CreateActorFromTag creates an actor seeded from the snapshot behind tag. Substrate requires the
// actor's template to be the one the snapshot was taken under. The actor is created SUSPENDED and
// borrows the tag's snapshot; the caller resumes it to restore the cloned RAM.
func (ad *Adapter) CreateActorFromTag(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef, tag substrate.SnapshotID) error {
	_, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:      &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplate: templateRef(template),
			SourceTag:     tagRef(tag),
		},
	})
	return err
}

// DeleteTag removes a tag and collects the snapshot copy it owns.
func (ad *Adapter) DeleteTag(ctx context.Context, tag substrate.SnapshotID) error {
	_, err := ad.ctl.DeleteTag(ctx, &atepb.DeleteTagRequest{Tag: tagRef(tag)})
	return err
}

// actorInfo maps a substrate Actor onto the narrowed ActorInfo the backend needs. The worker is
// reported for diagnostics only: the backend reaches the harness through the atenet-router, which
// resolves the actor's worker on every request, so nothing here is dialed. Substrate clears the
// worker assignment whenever the actor holds no worker (SUSPENDED, PAUSED, CRASHED).
func actorInfo(a *atepb.Actor) substrate.ActorInfo {
	st := a.GetStatus()
	return substrate.ActorInfo{
		Status:   actorStatus(st.GetState()),
		Worker:   st.GetWorkerAssignment().GetWorkerPod(),
		Snapshot: st.GetExternalSnapshot().GetSnapshotUri(),
	}
}

// actorStatus narrows substrate's lifecycle onto the states the backend acts on. Transitional states
// (RESUMING, SUSPENDING, PAUSING, DELETING, REVERTING) map to StatusUnknown, so the backend refuses
// to place onto an actor that is mid-transition instead of guessing which side it will land on.
func actorStatus(s atepb.ActorState) substrate.ActorStatus {
	switch s {
	case atepb.ActorState_ACTOR_STATE_RUNNING:
		return substrate.StatusRunning
	case atepb.ActorState_ACTOR_STATE_SUSPENDED, atepb.ActorState_ACTOR_STATE_PAUSED:
		return substrate.StatusSuspended
	case atepb.ActorState_ACTOR_STATE_CRASHED:
		return substrate.StatusTerminated
	default:
		return substrate.StatusUnknown
	}
}
