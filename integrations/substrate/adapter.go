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

	"github.com/agent-primitives/sessions/runtime/substrate"
	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
// seeds it from the template's golden snapshot when one is built. A name already taken is reported as
// substrate.ErrActorExists, so the backend can attach to the actor another host just created.
func (ad *Adapter) CreateActor(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef) error {
	_, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:      &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplate: templateRef(template),
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf("%w: %s/%s: %w", substrate.ErrActorExists, actor.Atespace, actor.Name, err)
	}
	return err
}

// ResumeActor schedules the actor onto a worker. Substrate restores the actor's own external
// snapshot when it holds one (including a tag's, borrowed at creation) and cold-boots the template
// spec otherwise; the request carries no boot flag. Substrate refuses a resume while another
// operation holds the actor's lease with ABORTED, reported as substrate.ErrActorBusy, and a missing
// actor with NOT_FOUND (RPCService.ResumeActor in cmd/ateapi/internal/controlapi/actor.go), reported
// as substrate.ErrActorNotFound. Substrate can also answer NOT_FOUND for an actor that exists, when
// a volume is missing from its template (createActorVolumes in
// cmd/ateapi/internal/controlapi/volumes.go, called by ensureVolumesCreated in workflow_resume.go),
// so the backend checks the actor with GetActor before it reads either as the actor being gone.
func (ad *Adapter) ResumeActor(ctx context.Context, actor substrate.ActorRef) (substrate.ActorInfo, error) {
	resp, err := ad.ctl.ResumeActor(ctx, &atepb.ResumeActorRequest{Actor: objectRef(actor)})
	if status.Code(err) == codes.NotFound {
		return substrate.ActorInfo{}, fmt.Errorf("%w: %s/%s: %w", substrate.ErrActorNotFound, actor.Atespace, actor.Name, err)
	}
	if err != nil {
		return substrate.ActorInfo{}, busy(actor, err)
	}
	return actorInfo(resp.GetActor()), nil
}

// SuspendActor snapshots the actor to durable storage and frees the worker, returning the actor as
// the suspend left it: the external snapshot it now holds (see heldSnapshot) and its UID. Substrate
// refuses a suspend while another operation holds the actor's lease with ABORTED, reported as
// substrate.ErrActorBusy, and a missing actor with NOT_FOUND (RPCService.SuspendActor in
// cmd/ateapi/internal/controlapi/actor.go maps the store's ErrNotFound from
// ActorWorkflow.SuspendActor in cmd/ateapi/internal/controlapi/workflow_suspend.go), reported as
// substrate.ErrActorNotFound, which the ControlClient contract requires: a memory-tier Snapshot
// reads it as the session's in-RAM state being gone, not as a transport failure.
func (ad *Adapter) SuspendActor(ctx context.Context, actor substrate.ActorRef) (substrate.ActorInfo, error) {
	resp, err := ad.ctl.SuspendActor(ctx, &atepb.SuspendActorRequest{Actor: objectRef(actor)})
	if status.Code(err) == codes.NotFound {
		return substrate.ActorInfo{}, fmt.Errorf("%w: %s/%s: %w", substrate.ErrActorNotFound, actor.Atespace, actor.Name, err)
	}
	if err != nil {
		return substrate.ActorInfo{}, busy(actor, err)
	}
	return actorInfo(resp.GetActor()), nil
}

// busy wraps err in substrate.ErrActorBusy when substrate reported ABORTED, which it does when
// another operation holds the actor's lease (acquireLease in substrate's
// cmd/ateapi/internal/controlapi/workflow.go) or a concurrent update conflicted. Both settle on
// their own, so the caller can retry.
func busy(actor substrate.ActorRef, err error) error {
	if status.Code(err) == codes.Aborted {
		return fmt.Errorf("%w: %s/%s: %w", substrate.ErrActorBusy, actor.Atespace, actor.Name, err)
	}
	return err
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
// Substrate refuses a tag while another operation holds the source actor's lease with ABORTED
// (acquireLease in cmd/ateapi/internal/controlapi/workflow.go, called from TagActorSnapshot in
// cmd/ateapi/internal/controlapi/workflow_tag.go), reported as substrate.ErrActorBusy. A missing
// source actor is FAILED_PRECONDITION (RPCService.CreateTag in
// cmd/ateapi/internal/controlapi/tag.go), which the backend classifies by reading the actor.
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
	return busy(source, err)
}

// CreateActorFromTag creates an actor seeded from the snapshot behind tag. Substrate requires the
// actor's template to be the one the snapshot was taken under. The actor is created SUSPENDED and
// borrows the tag's snapshot; the caller resumes it to restore the cloned RAM. CreateActor returns
// the actor it stored, with the UID substrate assigned (RPCService.CreateActor in
// cmd/ateapi/internal/controlapi/actor.go), so the caller can tell that clone from a later actor
// under the same name.
func (ad *Adapter) CreateActorFromTag(ctx context.Context, actor substrate.ActorRef, template substrate.ObjectRef, tag substrate.SnapshotID) (substrate.ActorInfo, error) {
	a, err := ad.ctl.CreateActor(ctx, &atepb.CreateActorRequest{
		Actor: &atepb.Actor{
			Metadata:      &atepb.ResourceMetadata{Atespace: actor.Atespace, Name: actor.Name},
			ActorTemplate: templateRef(template),
			SourceTag:     tagRef(tag),
		},
	})
	if err != nil {
		return substrate.ActorInfo{}, err
	}
	return actorInfo(a), nil
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
//
// UID is ResourceMetadata.uid, which substrate assigns when the actor is created and keeps across
// every later update (suspend and resume included), so an actor created again under the same name
// reports a different one.
func actorInfo(a *atepb.Actor) substrate.ActorInfo {
	st := a.GetStatus()
	return substrate.ActorInfo{
		Status:   actorStatus(st.GetState()),
		Worker:   st.GetWorkerAssignment().GetWorkerPod(),
		Snapshot: heldSnapshot(st),
		UID:      a.GetMetadata().GetUid(),
	}
}

// heldSnapshot returns the object-storage URI of the external snapshot the actor holds: the
// highest-generation entry of ActorStatus.snapshots whose durable copy is COMPLETED, or "" when there
// is none. That is the snapshot substrate restores on ResumeActor and copies on CreateTag
// (findLatestDurableSnapshot in substrate's cmd/ateapi/internal/controlapi/snapshot_helpers.go), so
// comparing it before and after a tag proves which snapshot the tag took. The entry is picked by
// the same rule (on equal generations the later entry wins) before its URI is read: if that entry
// has no URI the result is "" and the caller refuses, rather than naming an older completed
// generation the tag would not copy.
//
// Entries that are not candidates: a PENDING durable copy is still being written (a suspend in
// flight), and an entry with no durable copy is a node-local pause checkpoint, which a tag cannot
// copy. An actor seeded from a tag holds the tag's snapshot as generation 1 until its own first
// suspend, so a fork child reports the borrowed URI, under the tag's prefix.
//
// Substrate f03327bd replaced the single ActorStatus.external_snapshot (field 4) with this list. A
// server older than that still sends field 4, which this client does not read, so it reports "" and
// a stateful fork refuses rather than tagging a snapshot it cannot name: through the Placer as
// unplaceable (no external snapshot handle), and a direct Backend.Fork with ErrNoSnapshotToClone.
func heldSnapshot(st *atepb.ActorStatus) string {
	var held *atepb.Snapshot
	for _, snap := range st.GetSnapshots() {
		if snap.GetDurableSnapshot().GetStatus() != atepb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
			continue
		}
		if held == nil || snap.GetGeneration() >= held.GetGeneration() {
			held = snap
		}
	}
	return held.GetDurableSnapshot().GetObject().GetSnapshotUri()
}

// actorStatus narrows substrate's lifecycle onto the states the backend acts on. RESUMING,
// SUSPENDING and PAUSING map to StatusTransitioning: they settle into RUNNING, SUSPENDED or PAUSED
// on their own (often another host's resume), so the backend reports them as retryable. DELETING,
// REVERTING and any state this client does not know map to StatusUnknown, which the backend refuses
// to place onto rather than guess which side it will land on.
func actorStatus(s atepb.ActorState) substrate.ActorStatus {
	switch s {
	case atepb.ActorState_ACTOR_STATE_RUNNING:
		return substrate.StatusRunning
	case atepb.ActorState_ACTOR_STATE_SUSPENDED, atepb.ActorState_ACTOR_STATE_PAUSED:
		return substrate.StatusSuspended
	case atepb.ActorState_ACTOR_STATE_CRASHED:
		return substrate.StatusTerminated
	case atepb.ActorState_ACTOR_STATE_RESUMING, atepb.ActorState_ACTOR_STATE_SUSPENDING, atepb.ActorState_ACTOR_STATE_PAUSING:
		return substrate.StatusTransitioning
	default:
		return substrate.StatusUnknown
	}
}
