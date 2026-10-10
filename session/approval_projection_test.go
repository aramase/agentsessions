package session

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// A later decision or malformed append must not alter a response whose row captured an
// earlier positive cursor. Zero must skip inspection, not mean "read the current head".
func TestPendingSessionCapturedPrefixAndZero(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const uid = "captured"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "gate"}); err != nil {
		t.Fatal(err)
	}
	empty, err := store.SessionInfo(uid)
	if err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "gate", InputCount: proto.Int64(0)}},
		{Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", Tool: "write", IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}},
		{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}},
	} {
		ev.ExecutionID = "execution"
		if _, err := log.Append(int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
	}
	captured, err := store.SessionInfo(uid)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "execution", ToolCallID: "call", RequestSeq: 3, Approved: false})
	if err != nil {
		t.Fatal(err)
	}
	// Invalid current evidence deliberately lies beyond the captured response boundary.
	if _, err := log.Append(decision.Seq, decision.Fence, api.Event{Kind: api.EventOutput, ExecutionID: "execution", Message: api.TextMessage("assistant", "illegal before receipt")}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, nil)
	pending, err := svc.pendingSession(captured)
	if err != nil || pending.LastSeq != 3 || pending.ExecState != v1.ExecState_EXEC_AWAITING || !proto.Equal(pending.PendingApproval, &v1.ApprovalRef{ExecutionId: "execution", ToolCallId: "call", RequestSeq: 3}) {
		t.Fatalf("captured prefix=%v %v", pending, err)
	}
	neverRun, err := svc.pendingSession(empty)
	if err != nil || neverRun.LastSeq != 0 || neverRun.PendingApproval != nil || neverRun.ExecState != v1.ExecState_EXEC_PENDING {
		t.Fatalf("zero prefix=%v %v", neverRun, err)
	}
}

func TestApprovalServiceCauseMapping(t *testing.T) {
	// Resume historically classifies ordinary recorded-prefix divergence as Internal.
	// Only the decision boundary maps invalid approval correlation to FailedPrecondition.
	if err := resumeError(fmt.Errorf("ordinary tool replay: %w", controller.ErrReplayDiverged)); status.Code(err) != codes.Internal {
		t.Errorf("ordinary Resume divergence changed status: %v", err)
	}
	for _, mapError := range []func(error) error{approvalError, execError, resumeError, suspendError} {
		for _, cause := range []error{eventlog.ErrConflict, eventlog.ErrFenced} {
			if err := mapError(fmt.Errorf("journal: %w", cause)); status.Code(err) != codes.Aborted {
				t.Errorf("CAS/fence=%v", err)
			}
		}
		// Backend Aborted is not a typed local busy/CAS cause; preserve the existing Internal boundary.
		if err := mapError(fmt.Errorf("backend: %w", status.Error(codes.Aborted, "backend conflict"))); status.Code(err) != codes.Internal {
			t.Errorf("backend conflict remapped=%v", err)
		}
	}
	for _, cause := range []error{controller.ErrInvalidApprovalDecision, controller.ErrInvalidExecutionLog, controller.ErrReplayDiverged, controller.ErrApprovalUnavailable} {
		if err := approvalError(fmt.Errorf("decision: %w", cause)); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("approval precondition=%v", err)
		}
	}
}

type failingApprovalStream struct {
	v1.Sessions_ExecServer
	ctx          context.Context
	fail         error
	afterInitial func()
	frames       []*v1.ExecUpdate
}

func (s *failingApprovalStream) Context() context.Context { return s.ctx }
func (s *failingApprovalStream) Send(frame *v1.ExecUpdate) error {
	s.frames = append(s.frames, frame)
	if len(s.frames) == 1 {
		if s.afterInitial != nil {
			s.afterInitial()
		}
		return nil
	}
	return s.fail
}

func TestApprovalRecoverySendFailureAfterDurableRepair(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.PutSession(sqlitelog.SessionMeta{UID: "s", Harness: "gate"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session("s")
	f, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "gate", InputCount: proto.Int64(0)}},
		{Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", Tool: "write", IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}},
	} {
		ev.ExecutionID = "e"
		if _, err := log.Append(int64(i), f, ev); err != nil {
			t.Fatal(err)
		}
	}
	// The registry's pending repair needs no Describe or compute; its real backend may be nil.
	registry, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, registry)
	cause := errors.New("client disconnected")
	// Advance after the initial captured frame: the recovery CAS must be checked under
	// the Registry guard, not only against the head read before sending that frame.
	casStream := &failingApprovalStream{ctx: t.Context(), fail: cause, afterInitial: func() {
		if _, err := log.Append(2, f, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleBaseline}}); err != nil {
			t.Error(err)
		}
	}}
	if err := svc.Exec(&v1.ExecRequest{Session: "s", ExpectedLastSeq: proto.Int64(2)}, casStream); status.Code(err) != codes.Aborted {
		t.Fatalf("guard-time CAS = %v", err)
	}
	if head, err := log.Head(); err != nil || head != 3 {
		t.Fatalf("stale recovery wrote a request: head=%d err=%v", head, err)
	}
	stream := &failingApprovalStream{ctx: t.Context(), fail: cause}
	if err := svc.Exec(&v1.ExecRequest{Session: "s"}, stream); err != cause {
		t.Fatalf("send failure=%v", err)
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil || state.Request == nil || state.Request.Seq != 4 || len(stream.frames) != 2 || stream.frames[1].GetRecord().GetEvent().GetKind() != v1.EventKind_EVENT_APPROVAL_REQUEST {
		t.Fatalf("unwound repair=%+v frames=%v err=%v", state, stream.frames, err)
	}
	got, err := svc.Resume(t.Context(), &v1.ResumeRequest{Session: "s"})
	if err != nil || got.PendingApproval.GetRequestSeq() != 4 || got.LastSeq != 4 {
		t.Fatalf("send failure retained guard: %v %v", got, err)
	}
}
