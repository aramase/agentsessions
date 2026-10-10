package sqlitelog_test

import (
	"path/filepath"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Host repair/decision writes advance the journal without implying a live incarnation.
func TestHostApprovalEventsPreserveColdCompute(t *testing.T) {
	for _, kind := range []api.EventKind{api.EventApprovalRequest, api.EventApprovalResult} {
		t.Run(string(kind), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "approval.db")
			s := mustOpen(t, path)
			log := s.Session("session")
			if _, err := log.Append(0, 0, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}}); err != nil {
				t.Fatal(err)
			}
			ev := api.Event{ExecutionID: "execution", Kind: kind}
			if kind == api.EventApprovalRequest {
				ev.Approval = &api.ApprovalRequest{ToolCallID: "call"}
			} else {
				ev.ApprovalResult = &api.ApprovalResult{ToolCallID: "call", RequestSeq: 1, Approved: true}
			}
			if _, err := log.Append(1, 0, ev); err != nil {
				t.Fatal(err)
			}
			check := func(s *sqlitelog.Store) {
				t.Helper()
				info, err := s.SessionInfo("session")
				if err != nil {
					t.Fatal(err)
				}
				if info.ComputeState != api.ComputeCold || info.LastSeq != 2 {
					t.Errorf("after host %s: compute=%s cursor=%d, want COLD cursor=2", kind, info.ComputeState, info.LastSeq)
				}
			}
			check(s)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			check(mustOpen(t, path))
		})
	}
}

func TestOrdinaryRecordsStillMoveComputeLive(t *testing.T) {
	for _, ev := range []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}},
		{Kind: api.EventInput, Message: api.TextMessage("user", "hi")},
		{Kind: api.EventModelCall, ModelCall: &api.ModelCall{ID: "model"}},
		{Kind: api.EventOutput, Message: api.TextMessage("assistant", "hi")},
		{Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", Mediation: api.MediationControllerMediated}},
		{Kind: api.EventToolResult, Result: &api.ToolResult{ID: "call"}},
		{Kind: api.EventUsage, Usage: &api.Usage{}},
		{Kind: api.EventError, Err: &api.Error{Description: "error"}},
		{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	} {
		t.Run(string(ev.Kind), func(t *testing.T) {
			s := mustOpen(t, ":memory:")
			log := s.Session("session")
			if _, err := log.Append(0, 0, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}}); err != nil {
				t.Fatal(err)
			}
			ev.ExecutionID = "execution"
			if _, err := log.Append(1, 0, ev); err != nil {
				t.Fatal(err)
			}
			info, err := s.SessionInfo("session")
			if err != nil {
				t.Fatal(err)
			}
			if info.ComputeState != api.ComputeLive || info.LastSeq != 2 {
				t.Fatalf("compute=%s cursor=%d, want LIVE cursor=2", info.ComputeState, info.LastSeq)
			}
		})
	}
}
