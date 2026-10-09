package session_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/wire"
)

// Keep the real Substrate backend's error wrapping; only the external control plane is replaced.
// An existing suspended actor makes Exec resume it (ResumeActor restores its snapshot), just as
// Resume does.
type conflictControl struct{}

func (conflictControl) CreateActor(context.Context, substrate.ActorRef, substrate.ObjectRef) error {
	return nil
}

func (conflictControl) ResumeActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, status.Error(codes.Aborted, "concurrent update conflict")
}

func (conflictControl) SuspendActor(context.Context, substrate.ActorRef) (string, error) {
	return "", status.Error(codes.Aborted, "concurrent update conflict")
}

func (conflictControl) DeleteActor(context.Context, substrate.ActorRef) error { return nil }

func (conflictControl) GetActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{Status: substrate.StatusSuspended}, nil
}

func TestBackendAbortedRemainsInternal(t *testing.T) {
	desc, err := (echoagent.Harness{}).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client := newClientWith(t, substrate.New(conflictControl{}, "space", substrate.ObjectRef{Name: "echo"}, desc))
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
			if status.Code(err) != codes.Internal {
				t.Errorf("backend conflict during %s = %v, want Internal", operation, err)
			}
			sess, lookupErr := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if lookupErr != nil || sess.GetLastSeq() != 0 {
				t.Fatalf("failed %s changed journal: %v, %v", operation, sess, lookupErr)
			}
		})
	}
}
