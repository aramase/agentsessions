package session_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/wire"
)

// Keep the real Substrate backend's error wrapping; only the external control plane is replaced.
// An existing suspended actor makes Exec resume it (ResumeActor restores its snapshot), just as
// Resume does. Resume and suspend fail with err.
type conflictControl struct{ err error }

func (conflictControl) CreateActor(context.Context, substrate.ActorRef, substrate.ObjectRef) error {
	return nil
}

func (c conflictControl) ResumeActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, c.err
}

func (c conflictControl) SuspendActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, c.err
}

func (conflictControl) DeleteActor(context.Context, substrate.ActorRef) error { return nil }

func (conflictControl) GetActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{Status: substrate.StatusSuspended}, nil
}

// A transport status from the control plane does not leak into the public code: a raw ABORTED stays
// INTERNAL. Only the typed sentinel a ControlClient wraps for an actor another operation holds
// (substrate.ErrActorBusy, which the substrate adapter reports for substrate's ABORTED) is ABORTED.
// Neither changes the journal.
func TestBackendAbortedRemainsInternal(t *testing.T) {
	testBackendConflict(t, status.Error(codes.Aborted, "concurrent update conflict"), codes.Internal)
}

func TestBusyActorIsAborted(t *testing.T) {
	testBackendConflict(t, fmt.Errorf("%w: another operation is in progress for this actor", substrate.ErrActorBusy), codes.Aborted)
}

func testBackendConflict(t *testing.T, controlErr error, want codes.Code) {
	desc, err := (echoagent.Harness{}).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client := newClientWith(t, substrate.New(conflictControl{err: controlErr}, "space", substrate.ObjectRef{Name: "echo"}, desc))
	uid := mustCreate(t, client)
	for _, operation := range []string{"Suspend", "Resume", "Exec"} {
		t.Run(operation, func(t *testing.T) {
			var err error
			switch operation {
			case "Suspend":
				_, err = client.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
			case "Resume":
				_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			case "Exec":
				stream, execErr := client.Exec(t.Context(), &v1.ExecRequest{
					Session: uid,
					Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "conflict"))},
				})
				err = execErr
				if err == nil {
					err = drainExec(stream)
				}
			}
			if status.Code(err) != want {
				t.Errorf("backend conflict during %s = %v, want %v", operation, err, want)
			}
			sess, lookupErr := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if lookupErr != nil || sess.GetLastSeq() != 0 {
				t.Fatalf("failed %s changed journal: %v, %v", operation, sess, lookupErr)
			}
		})
	}
}
