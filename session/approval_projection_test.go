package session

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
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

// Removing rendering-only tolerance must fail these real SQLite reads; allowing malformed
// evidence through command validation must fail the exact-cause and journal-invariance checks.
func TestPendingSessionCompatibilityReadOnlyAndStrictCommands(t *testing.T) {
	owned := func(id string) []api.Event {
		return []api.Event{
			{ExecutionID: id, Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "gate", InputCount: proto.Int64(0)}},
			{ExecutionID: id, Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", Tool: "write", IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}},
			{ExecutionID: id, Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}},
		}
	}
	changed := func(change func([]api.Event)) []api.Event {
		events := owned("execution")
		change(events)
		return events
	}
	invalid, diverged := controller.ErrInvalidExecutionLog, controller.ErrReplayDiverged
	for _, tc := range []struct {
		name    string
		events  []api.Event
		cause   error
		compute v1.ComputeState
	}{
		{"legacy idless request", []api.Event{{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}}}, invalid, v1.ComputeState_COMPUTE_NONE},
		{"orphan request", []api.Event{{ExecutionID: "execution", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}}}, invalid, v1.ComputeState_COMPUTE_NONE},
		{"missing request payload", changed(func(e []api.Event) { e[2].Approval = nil }), invalid, v1.ComputeState_COMPUTE_LIVE},
		{"missing call ID", changed(func(e []api.Event) { e[1].ToolCall.ID = "" }), invalid, v1.ComputeState_COMPUTE_LIVE},
		{"missing key", changed(func(e []api.Event) { e[1].ToolCall.IdempotencyKey = "" }), invalid, v1.ComputeState_COMPUTE_LIVE},
		{"wrong request tuple", changed(func(e []api.Event) { e[2].Approval.ToolCallID = "wrong" }), diverged, v1.ComputeState_COMPUTE_LIVE},
		{"missing decision tuple", append(owned("execution"), api.Event{ExecutionID: "execution", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "call"}}), diverged, v1.ComputeState_COMPUTE_LIVE},
		{"wrong decision call", append(owned("execution"), api.Event{ExecutionID: "execution", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "wrong", RequestSeq: 3}}), diverged, v1.ComputeState_COMPUTE_LIVE},
		{"wrong decision sequence", append(owned("execution"), api.Event{ExecutionID: "execution", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 2}}), diverged, v1.ComputeState_COMPUTE_LIVE},
		{"illegal continuation", append(owned("execution"), api.Event{ExecutionID: "execution", Kind: api.EventOutput, Message: api.TextMessage("assistant", "before receipt")}), invalid, v1.ComputeState_COMPUTE_LIVE},
		{"malformed old then valid current", append([]api.Event{{ExecutionID: "old", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "old-call"}}}, owned("execution")...), invalid, v1.ComputeState_COMPUTE_LIVE},
		{"cold orphan request and decision", []api.Event{
			{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}},
			{ExecutionID: "execution", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}},
			{ExecutionID: "execution", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 2}},
		}, invalid, v1.ComputeState_COMPUTE_COLD},
		{"valid owned request", owned("execution"), nil, v1.ComputeState_COMPUTE_LIVE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store, err := sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			const uid = "parent"
			if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Project: "compat", Name: "parent-name", Harness: "gate", Model: "model", Annotations: `{"note":"kept"}`, Identity: `{"principal":"parent"}`}); err != nil {
				t.Fatal(err)
			}
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range tc.events {
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			backend := local.New(echoagent.Harness{})
			defer backend.Close()
			registry, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(backend, nil)})
			if err != nil {
				t.Fatal(err)
			}
			svc := NewService(store, registry)
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			info, err := store.SessionInfo(uid)
			if err != nil {
				t.Fatal(err)
			}
			// Record equality alone cannot detect a fence minted without an append.
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			unchanged := func() {
				t.Helper()
				after, err := log.Read(1)
				if err != nil || !reflect.DeepEqual(after, before) {
					t.Fatalf("parent journal changed: %v %v", after, err)
				}
				var currentFence int64
				if err := raw.QueryRow(`SELECT fence FROM sessions WHERE session = ?`, uid).Scan(&currentFence); err != nil || currentFence != fence {
					t.Fatalf("parent fence=%d want=%d err=%v", currentFence, fence, err)
				}
				afterInfo, err := store.SessionInfo(uid)
				if err != nil || !reflect.DeepEqual(afterInfo, info) {
					t.Fatalf("parent metadata changed: %+v %v", afterInfo, err)
				}
			}
			if _, err := controller.InspectApproval(log, info.LastSeq); tc.cause != nil && !errors.Is(err, tc.cause) || tc.cause == nil && err != nil {
				t.Fatalf("InspectApproval=%v want=%v", err, tc.cause)
			}
			get, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil {
				t.Errorf("decodable history GetSession=%v", err)
			} else {
				wantState := v1.ExecState_EXEC_COMPLETED
				var wantRef *v1.ApprovalRef
				if tc.cause == nil {
					wantState = v1.ExecState_EXEC_AWAITING
					wantRef = &v1.ApprovalRef{ExecutionId: "execution", ToolCallId: "call", RequestSeq: 3}
				}
				if get.LastSeq != int64(len(tc.events)) || get.ExecState != wantState || !proto.Equal(get.PendingApproval, wantRef) || get.ComputeState != tc.compute || get.Metadata.Project != "compat" || get.Annotations["note"] != "kept" {
					t.Errorf("GetSession=%v want state=%v ref=%v compute=%v", get, wantState, wantRef, tc.compute)
				}
			}
			list, err := svc.ListSessions(t.Context(), &v1.ListSessionsRequest{Project: "compat"})
			if err != nil || len(list.GetSessions()) != 1 || !proto.Equal(list.Sessions[0], get) {
				t.Errorf("ListSessions=%v err=%v Get=%v", list, err, get)
			}
			unchanged()
			if tc.cause != nil {
				decision := api.ApprovalDecision{ExecutionID: "execution", ToolCallID: "call", RequestSeq: 3, Approved: false}
				for name, command := range map[string]func() error{
					"controller Approve": func() error { _, err := controller.Approve(log, decision); return err },
					"Registry Approve":   func() error { _, err := registry.Approve(t.Context(), log, uid, decision); return err },
					"Registry Resume":    func() error { return registry.Resume(t.Context(), log, uid, "gate") },
					"Registry Suspend":   func() error { _, err := registry.Suspend(t.Context(), log, uid, "gate"); return err },
				} {
					if err := command(); !errors.Is(err, tc.cause) {
						t.Errorf("%s=%v want exact cause %v", name, err, tc.cause)
					}
					unchanged()
				}
				code := codes.FailedPrecondition
				if tc.cause == diverged {
					code = codes.Internal // Preserve the original Exec/Resume divergence mapping.
				}
				for _, command := range []struct {
					name string
					call func() error
					code codes.Code
				}{
					{"Approve", func() error {
						_, err := svc.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: "execution", ToolCallId: "call", RequestSeq: 3, Approved: proto.Bool(false)})
						return err
					}, codes.FailedPrecondition},
					{"Exec new input", func() error {
						return svc.Exec(&v1.ExecRequest{Session: uid, Inputs: []*v1.Message{{Role: "user"}}}, &failingApprovalStream{ctx: t.Context()})
					}, code},
					{"Exec no input", func() error { return svc.Exec(&v1.ExecRequest{Session: uid}, &failingApprovalStream{ctx: t.Context()}) }, code},
					{"Resume", func() error { _, err := svc.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); return err }, code},
					{"Suspend", func() error { _, err := svc.Suspend(t.Context(), &v1.SuspendRequest{Session: uid}); return err }, codes.FailedPrecondition},
				} {
					if err := command.call(); status.Code(err) != command.code {
						t.Errorf("%s=%v want=%v", command.name, err, command.code)
					}
					unchanged()
				}
			}
			fork, err := svc.Fork(t.Context(), &v1.ForkRequest{Session: uid, AtSeq: info.LastSeq, Count: 1, ChildNames: []string{"child-name"}, Labels: map[string]string{"branch": "child"}})
			if err != nil || len(fork.GetChildren()) != 1 {
				t.Fatalf("Fork readable copied history=%v err=%v", fork, err)
			}
			child := fork.Children[0]
			if child.PendingApproval != nil || child.ExecState != v1.ExecState_EXEC_COMPLETED || child.ComputeState != v1.ComputeState_COMPUTE_COLD || child.ParentUid != uid || child.ForkSeq != info.LastSeq || child.LastSeq != info.LastSeq+1 || child.Metadata.Project != "compat" || child.Metadata.Name != "child-name" || child.Harness != "gate" || child.Model != "model" || child.Annotations["note"] != "kept" || child.Labels["branch"] != "child" || child.Identity != nil {
				t.Fatalf("child metadata/authority=%v", child)
			}
			childGet, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: child.Metadata.Uid})
			if err != nil || !proto.Equal(childGet, child) {
				t.Fatalf("child Get=%v %v", childGet, err)
			}
			if tc.cause == nil {
				state, err := controller.InspectApproval(store.Session(child.Metadata.Uid), child.LastSeq)
				if err != nil || state == nil || !state.Inherited {
					t.Fatalf("inherited request=%+v err=%v", state, err)
				}
				_, err = controller.Approve(store.Session(child.Metadata.Uid), api.ApprovalDecision{ExecutionID: "execution", ToolCallID: "call", RequestSeq: 3})
				if !errors.Is(err, controller.ErrInvalidApprovalDecision) {
					t.Fatalf("child approval=%v", err)
				}
			}
			unchanged()
		})
	}
}

// A future query renderer may already report AWAITING without gate authority. Invalid
// evidence must preserve that rendered base, not erase it or fabricate a pending tuple.
func TestWithPendingApprovalPreservesRenderedBase(t *testing.T) {
	for _, wrongTuple := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrong-tuple=%v", wrongTuple), func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("rendered")
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			events := []api.Event{{ExecutionID: "e", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "wrong"}}}
			cause := controller.ErrInvalidExecutionLog
			if wrongTuple {
				events = append([]api.Event{{ExecutionID: "e", Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}}}, events...)
				cause = controller.ErrReplayDiverged
			}
			for i, ev := range events {
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			info, err := store.SessionInfo("rendered")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controller.InspectApproval(log, info.LastSeq); !errors.Is(err, cause) {
				t.Fatalf("fixture cause=%v want=%v", err, cause)
			}
			base := &v1.Session{Metadata: &v1.ResourceMetadata{Uid: "rendered", Name: "preserved"}, LastSeq: info.LastSeq, ExecState: v1.ExecState_EXEC_AWAITING, ComputeState: v1.ComputeState_COMPUTE_COLD, Labels: map[string]string{"view": "base"}}
			want := proto.Clone(base)
			got, err := NewService(store, nil).withPendingApproval(info, base)
			if err != nil || got != base || !proto.Equal(got, want) || got.PendingApproval != nil {
				t.Fatalf("rendered base changed: got=%v err=%v want=%v", got, err, want)
			}
		})
	}
}

// Broad error swallowing would hide a broken database or undecodable records. A zero
// captured cursor must still avoid reading even when the store can no longer be used.
func TestPendingSessionOperationalAndDecodeErrors(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, nil)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	info := sqlitelog.SessionInfo{SessionMeta: sqlitelog.SessionMeta{UID: "closed"}, LastSeq: 1}
	if got, err := svc.pendingSession(info); got != nil || status.Code(err) != codes.Internal {
		t.Fatalf("closed positive cursor=%v %v", got, err)
	}
	base := &v1.Session{LastSeq: 1, ExecState: v1.ExecState_EXEC_AWAITING}
	if got, err := svc.withPendingApproval(info, base); got != nil || status.Code(err) != codes.Internal {
		t.Fatalf("closed direct decoration=%v %v", got, err)
	}
	info.LastSeq = 0
	if got, err := svc.pendingSession(info); err != nil || got.LastSeq != 0 || got.ExecState != v1.ExecState_EXEC_PENDING || got.PendingApproval != nil {
		t.Fatalf("closed zero cursor read the store: %v %v", got, err)
	}
	if got, err := svc.withPendingApproval(info, base); err != nil || got != base {
		t.Fatalf("closed zero decoration read the store: %v %v", got, err)
	}
	info.Labels = "{"
	if got, err := svc.pendingSession(info); got != nil || status.Code(err) != codes.Internal {
		t.Fatalf("invalid metadata decode=%v %v", got, err)
	}

	path := filepath.Join(t.TempDir(), "undecodable.db")
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("decode")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(0, fence, api.Event{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}}); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`UPDATE events SET event = x'ff' WHERE session = 'decode' AND seq = 1`); err != nil {
		t.Fatal(err)
	}
	svc = NewService(store, nil)
	if _, err := controller.InspectApproval(log, 1); err == nil || errors.Is(err, controller.ErrInvalidExecutionLog) || errors.Is(err, controller.ErrReplayDiverged) {
		t.Fatalf("decode must be operational, not typed invalid evidence: %v", err)
	}
	if got, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: "decode"}); got != nil || status.Code(err) != codes.Internal {
		t.Fatalf("undecodable Get=%v %v", got, err)
	}
	if got, err := svc.ListSessions(t.Context(), &v1.ListSessionsRequest{}); got != nil || status.Code(err) != codes.Internal {
		t.Fatalf("undecodable List=%v %v", got, err)
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
