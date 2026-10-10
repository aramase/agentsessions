package controller_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

func approvalLog(t *testing.T, backend string) eventlog.Store {
	t.Helper()
	if backend == "memory" {
		return eventlog.AsStore(eventlog.New())
	}
	s, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s.Session("session")
}

func approvalAppend(t *testing.T, log eventlog.Store, fence int64, events ...api.Event) eventlog.Record {
	t.Helper()
	var rec eventlog.Record
	for _, ev := range events {
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		rec, err = log.Append(head, fence, ev)
		if err != nil {
			t.Fatal(err)
		}
	}
	return rec
}

func approvalRecord(t *testing.T, log eventlog.Store, kind api.EventKind) eventlog.Record {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range recs {
		if rec.Event.Kind == kind {
			return rec
		}
	}
	t.Fatalf("missing %s", kind)
	return eventlog.Record{}
}

func approvalCall(id string) api.Event {
	return api.Event{ExecutionID: "e1", Kind: api.EventToolCall, ToolCall: &api.ToolCall{
		ID: id, Tool: "write", Args: map[string]any{"path": "report.txt"},
		Mediation: api.MediationRequiresApproval, IdempotencyKey: "key",
	}}
}

func approvalSeed(t *testing.T, log eventlog.Store, modern bool, cut api.EventKind) {
	t.Helper()
	if modern {
		count := int64(1)
		approvalAppend(t, log, 0,
			api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{
				InputCount: &count, Config: []byte("opaque"), ResumeFromSeq: 17, Harness: "recorded", HarnessVersion: "v1",
			}},
			api.Event{ExecutionID: "e1", Kind: api.EventInput, Message: api.TextMessage("user", "write")})
	}
	approvalAppend(t, log, 0, approvalCall("c1"))
	if cut == api.EventToolCall {
		return
	}
	req := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1", Reason: "destructive"}})
	if cut == api.EventApprovalRequest {
		return
	}
	dec := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", RequestSeq: req.Seq, Approved: true, Reason: "ok"}})
	if cut == api.EventApprovalResult {
		return
	}
	approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: req.Seq, ApprovalDecisionSeq: dec.Seq}})
	if cut == api.EventToolResult {
		return
	}
	approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
}

func TestInspectApprovalCuts(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, modern := range []bool{false, true} {
			for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult, api.EventToolResult, api.EventEnd} {
				t.Run(fmt.Sprintf("%s/modern=%t/%s", backend, modern, cut), func(t *testing.T) {
					log := approvalLog(t, backend)
					approvalSeed(t, log, modern, cut)
					head, _ := log.Head()
					got, err := controller.InspectApproval(log, 0)
					if err != nil {
						t.Fatal(err)
					}
					if got == nil || got.ExecutionID != "e1" || got.Call == nil || got.Call.ID != "c1" || got.Head != head || got.Inherited || got.Completed != (cut == api.EventEnd) {
						t.Fatalf("state=%+v", got)
					}
					if !reflect.DeepEqual(*got.Call, *approvalCall("c1").ToolCall) {
						t.Fatalf("changed original call: %+v", got.Call)
					}
					if modern {
						if got.Invocation == nil || string(got.Invocation.Config) != "opaque" || got.Invocation.ResumeFromSeq != 17 || got.Invocation.Harness != "recorded" || got.Invocation.HarnessVersion != "v1" || got.Invocation.InputCount == nil || *got.Invocation.InputCount != 1 {
							t.Fatalf("invocation=%+v", got.Invocation)
						}
					} else if got.Invocation != nil {
						t.Fatalf("invented legacy invocation: %+v", got.Invocation)
					}
					for _, check := range []struct {
						kind api.EventKind
						got  *eventlog.Record
						want bool
					}{
						{api.EventApprovalRequest, got.Request, cut != api.EventToolCall},
						{api.EventApprovalResult, got.Decision, cut == api.EventApprovalResult || cut == api.EventToolResult || cut == api.EventEnd},
						{api.EventToolResult, got.Receipt, cut == api.EventToolResult || cut == api.EventEnd},
					} {
						if (check.got != nil) != check.want {
							t.Fatalf("%s=%+v want present=%t", check.kind, check.got, check.want)
						}
						if check.want && !reflect.DeepEqual(*check.got, approvalRecord(t, log, check.kind)) {
							t.Fatalf("did not retain original %s record", check.kind)
						}
					}
				})
			}
		}
	}
}

func TestInspectApprovalCapturedPrefix(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			log := approvalLog(t, backend)
			approvalSeed(t, log, true, api.EventApprovalRequest)
			req := approvalRecord(t, log, api.EventApprovalRequest)
			approvalAppend(t, log, 0,
				api.Event{ExecutionID: "e1", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", RequestSeq: req.Seq, Approved: true}},
				api.Event{ExecutionID: "e2", Kind: api.EventInput, Message: api.TextMessage("user", "new")},
				api.Event{ExecutionID: "e2", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "bogus"}})
			got, err := controller.InspectApproval(log, req.Seq)
			if err != nil || got == nil || got.Head != req.Seq || got.Decision != nil || got.ExecutionID != "e1" {
				t.Fatalf("captured state=%+v err=%v", got, err)
			}
			if _, err := controller.InspectApproval(log, 0); !errors.Is(err, controller.ErrInvalidExecutionLog) {
				t.Fatalf("current err=%v want invalid log", err)
			}
		})
	}
}

func TestInspectApprovalFirstSeenSelectionAndOrdinaryEscape(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			log := approvalLog(t, backend)
			approvalSeed(t, log, true, api.EventApprovalRequest)
			approvalAppend(t, log, 0,
				api.Event{ExecutionID: "e2", Kind: api.EventExecutionStart}, // unrelated incomplete/malformed start must not be validated
				api.Event{ExecutionID: "e1", Kind: api.EventError, Err: &api.Error{Description: "late old error"}},
				api.Event{ExecutionID: "tail", Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}})
			got, err := controller.InspectApproval(log, 0)
			if err != nil || got != nil {
				t.Fatalf("nongated latest state=%+v err=%v", got, err)
			}
			oldReq := approvalRecord(t, log, api.EventApprovalRequest)
			if _, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: oldReq.Seq, Approved: true}); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
				t.Fatalf("superseded request err=%v", err)
			}
		})
	}
}

func TestInspectApprovalSequentialCalls(t *testing.T) {
	log := approvalLog(t, "sqlite")
	approvalSeed(t, log, false, api.EventToolResult)
	approvalAppend(t, log, 0, approvalCall("c2"))
	middleRequest := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c2"}})
	middleDecision := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c2", RequestSeq: middleRequest.Seq, Approved: true}})
	approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c2", ApprovalRequestSeq: middleRequest.Seq, ApprovalDecisionSeq: middleDecision.Seq}}, approvalCall("c3"))
	req := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c3"}})
	got, err := controller.InspectApproval(log, 0)
	if err != nil || got == nil || got.Call.ID != "c3" || got.Request.Seq != req.Seq || got.Decision != nil || got.Receipt != nil {
		t.Fatalf("later unresolved state=%+v err=%v", got, err)
	}
	// An identical older decision remains retryable within the same execution too.
	oldReq := approvalRecord(t, log, api.EventApprovalRequest)
	oldDec := approvalRecord(t, log, api.EventApprovalResult)
	gotDec, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: oldReq.Seq, Approved: true, Reason: "ok"})
	if err != nil || !reflect.DeepEqual(gotDec, oldDec) {
		t.Fatalf("historical retry=%+v err=%v", gotDec, err)
	}
}

// These literals deliberately do not use projection or production correlation helpers.
func approvalEvidence() []api.Event {
	return []api.Event{
		approvalCall("c1"),
		{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}},
		{ExecutionID: "e1", Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", RequestSeq: 2, Approved: true}},
		{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: 2, ApprovalDecisionSeq: 3}},
	}
}

func TestInspectApprovalRejectsBadEvidence(t *testing.T) {
	cases := []struct {
		name              string
		kind              api.EventKind
		mutate            func(*api.Event)
		remove, duplicate bool
		want              error
	}{
		{name: "idless gate", kind: api.EventToolCall, mutate: func(e *api.Event) { e.ExecutionID = "" }, want: controller.ErrInvalidExecutionLog},
		{name: "call missing id", kind: api.EventToolCall, mutate: func(e *api.Event) { e.ToolCall.ID = "" }, want: controller.ErrInvalidExecutionLog},
		{name: "call missing key", kind: api.EventToolCall, mutate: func(e *api.Event) { e.ToolCall.IdempotencyKey = "" }, want: controller.ErrMissingIdempotencyKey},
		{name: "not mediated", kind: api.EventToolCall, mutate: func(e *api.Event) { e.ToolCall.Mediation = api.MediationControllerMediated }, want: controller.ErrInvalidExecutionLog},
		{name: "call missing", kind: api.EventToolCall, remove: true, want: controller.ErrInvalidExecutionLog},
		{name: "call duplicate", kind: api.EventToolCall, duplicate: true, want: controller.ErrInvalidExecutionLog},
		{name: "request missing", kind: api.EventApprovalRequest, remove: true, want: controller.ErrInvalidExecutionLog},
		{name: "request payload", kind: api.EventApprovalRequest, mutate: func(e *api.Event) { e.Approval = nil }, want: controller.ErrInvalidExecutionLog},
		{name: "request wrong call", kind: api.EventApprovalRequest, mutate: func(e *api.Event) { e.Approval.ToolCallID = "wrong" }, want: controller.ErrReplayDiverged},
		{name: "request wrong execution", kind: api.EventApprovalRequest, mutate: func(e *api.Event) { e.ExecutionID = "other" }, want: controller.ErrInvalidExecutionLog},
		{name: "request duplicate", kind: api.EventApprovalRequest, duplicate: true, want: controller.ErrInvalidExecutionLog},
		{name: "decision missing", kind: api.EventApprovalResult, remove: true, want: controller.ErrInvalidExecutionLog},
		{name: "decision payload", kind: api.EventApprovalResult, mutate: func(e *api.Event) { e.ApprovalResult = nil }, want: controller.ErrInvalidExecutionLog},
		{name: "decision wrong call", kind: api.EventApprovalResult, mutate: func(e *api.Event) { e.ApprovalResult.ToolCallID = "wrong" }, want: controller.ErrReplayDiverged},
		{name: "decision zero seq", kind: api.EventApprovalResult, mutate: func(e *api.Event) { e.ApprovalResult.RequestSeq = 0 }, want: controller.ErrReplayDiverged},
		{name: "decision wrong seq", kind: api.EventApprovalResult, mutate: func(e *api.Event) { e.ApprovalResult.RequestSeq = 1 }, want: controller.ErrReplayDiverged},
		{name: "decision duplicate", kind: api.EventApprovalResult, duplicate: true, want: controller.ErrInvalidExecutionLog},
		{name: "decision conflict", kind: api.EventApprovalResult, duplicate: true, mutate: func(e *api.Event) { e.ApprovalResult.Approved = false }, want: controller.ErrInvalidExecutionLog},
		{name: "receipt payload", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result = nil }, want: controller.ErrInvalidExecutionLog},
		{name: "receipt wrong call", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.ID = "wrong" }, want: controller.ErrReplayDiverged},
		{name: "receipt zero request", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.ApprovalRequestSeq = 0 }, want: controller.ErrReplayDiverged},
		{name: "receipt wrong request", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.ApprovalRequestSeq = 1 }, want: controller.ErrReplayDiverged},
		{name: "receipt zero decision", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.ApprovalDecisionSeq = 0 }, want: controller.ErrReplayDiverged},
		{name: "receipt wrong decision", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.ApprovalDecisionSeq = 2 }, want: controller.ErrReplayDiverged},
		{name: "receipt duplicate", kind: api.EventToolResult, duplicate: true, want: controller.ErrInvalidExecutionLog},
		{name: "receipt contradicts approval", kind: api.EventToolResult, mutate: func(e *api.Event) { e.Result.Code = api.ToolResultCodeApprovalDenied; e.Result.IsError = true }, want: controller.ErrReplayDiverged},
	}
	for _, backend := range []string{"memory", "sqlite"} {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				log := approvalLog(t, backend)
				for _, ev := range approvalEvidence() {
					if ev.Kind == tc.kind {
						if tc.remove {
							continue
						}
						if tc.duplicate {
							approvalAppend(t, log, 0, ev)
						}
						if tc.mutate != nil {
							tc.mutate(&ev)
						}
					}
					approvalAppend(t, log, 0, ev)
				}
				if _, err := controller.InspectApproval(log, 0); !errors.Is(err, tc.want) {
					t.Fatalf("err=%v want %v", err, tc.want)
				}
			})
		}
	}
}

func TestInspectApprovalRejectsContinuation(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult} {
		for _, ev := range []api.Event{
			{ExecutionID: "e1", Kind: api.EventModelCall, ModelCall: &api.ModelCall{ID: "model"}},
			{ExecutionID: "e1", Kind: api.EventOutput, Message: api.TextMessage("assistant", "continued")},
			approvalCall("c2"),
			{ExecutionID: "e1", Kind: api.EventUsage, Usage: &api.Usage{}},
			{ExecutionID: "e1", Kind: api.EventInput, Message: api.TextMessage("user", "continued")},
			{ExecutionID: "e1", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		} {
			t.Run(string(cut)+"/"+string(ev.Kind), func(t *testing.T) {
				log := approvalLog(t, "memory")
				approvalSeed(t, log, false, cut)
				approvalAppend(t, log, 0, ev)
				if _, err := controller.InspectApproval(log, 0); !errors.Is(err, controller.ErrInvalidExecutionLog) {
					t.Fatalf("err=%v want invalid log", err)
				}
			})
		}
	}
}

func TestInspectApprovalErrorAndLifecycleDoNotResolve(t *testing.T) {
	log := approvalLog(t, "sqlite")
	approvalSeed(t, log, false, api.EventApprovalRequest)
	approvalAppend(t, log, 0,
		api.Event{ExecutionID: "e1", Kind: api.EventError, Err: &api.Error{Description: "interrupted"}},
		api.Event{ExecutionID: "unrelated", Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}})
	got, err := controller.InspectApproval(log, 0)
	if err != nil || got == nil || got.Decision != nil || got.Completed || got.ExecutionID != "e1" {
		t.Fatalf("state=%+v err=%v", got, err)
	}
}

func TestApproveDecisionAndHistoricRetryDoNotFence(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, approved := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/approved=%t", backend, approved), func(t *testing.T) {
				log := approvalLog(t, backend)
				approvalSeed(t, log, true, api.EventApprovalRequest)
				req := approvalRecord(t, log, api.EventApprovalRequest)
				decision := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: req.Seq, Approved: approved, Reason: "explicit", Identity: api.IdentityRef{Principal: "human", Issuer: "issuer", Subject: "subject"}}
				rec, err := controller.Approve(log, decision)
				if err != nil {
					t.Fatal(err)
				}
				if rec.Seq != req.Seq+1 || rec.Event.Kind != api.EventApprovalResult || rec.Event.ExecutionID != "e1" || rec.Event.Actor != decision.Identity || rec.Event.ApprovalResult == nil || *rec.Event.ApprovalResult != (api.ApprovalResult{ToolCallID: "c1", RequestSeq: req.Seq, Approved: approved, Reason: "explicit"}) {
					t.Fatalf("decision record=%+v", rec)
				}
				writer, err := log.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				immediate, err := controller.Approve(log, decision)
				if err != nil || !reflect.DeepEqual(immediate, rec) {
					t.Fatalf("decided-prefix exact retry=%+v err=%v", immediate, err)
				}
				result := &api.ToolResult{ID: "c1", ApprovalRequestSeq: req.Seq, ApprovalDecisionSeq: rec.Seq}
				if !approved {
					result.Code = api.ToolResultCodeApprovalDenied
					result.IsError = true
					result.Error = "explicit"
				}
				approvalAppend(t, log, writer,
					api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: result},
					api.Event{ExecutionID: "e1", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
					api.Event{ExecutionID: "e2", Kind: api.EventInput, Message: api.TextMessage("user", "next")})
				head, _ := log.Head()
				retry, err := controller.Approve(log, decision)
				if err != nil || !reflect.DeepEqual(retry, rec) {
					t.Fatalf("exact retry=%+v err=%v want %+v", retry, err, rec)
				}
				if after, _ := log.Head(); after != head {
					t.Fatalf("retry appended: %d -> %d", head, after)
				}
				approvalAppend(t, log, writer, api.Event{ExecutionID: "e2", Kind: api.EventError, Err: &api.Error{Description: "still owns fence"}})
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestApproveInvalidDoesNotFence(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"pending", "decided", "call-only"} {
			for _, field := range []string{"execution", "call", "seq", "missing execution", "missing call", "zero seq", "negative seq", "approved", "reason", "principal", "issuer", "subject"} {
				if mode != "decided" && (field == "approved" || field == "reason" || field == "principal" || field == "issuer" || field == "subject") {
					continue
				}
				t.Run(backend+"/"+mode+"/"+field, func(t *testing.T) {
					log := approvalLog(t, backend)
					cut := api.EventApprovalRequest
					if mode == "decided" {
						cut = api.EventApprovalResult
					} else if mode == "call-only" {
						cut = api.EventToolCall
					}
					approvalSeed(t, log, false, cut)
					decision := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 2, Approved: true, Reason: "ok"}
					switch field {
					case "execution":
						decision.ExecutionID = "wrong"
					case "call":
						decision.ToolCallID = "wrong"
					case "seq":
						decision.RequestSeq = 1
					case "missing execution":
						decision.ExecutionID = ""
					case "missing call":
						decision.ToolCallID = ""
					case "zero seq":
						decision.RequestSeq = 0
					case "negative seq":
						decision.RequestSeq = -1
					case "approved":
						decision.Approved = false
					case "reason":
						decision.Reason = "changed"
					case "principal":
						decision.Identity.Principal = "changed"
					case "issuer":
						decision.Identity.Issuer = "changed"
					case "subject":
						decision.Identity.Subject = "changed"
					}
					writer, err := log.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					head, _ := log.Head()
					if _, err := controller.Approve(log, decision); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
						t.Fatalf("err=%v want invalid decision", err)
					}
					if after, _ := log.Head(); after != head {
						t.Fatal("invalid decision appended")
					}
					approvalAppend(t, log, writer, api.Event{ExecutionID: "e1", Kind: api.EventError, Err: &api.Error{Description: "writer retained fence"}})
				})
			}
		}
	}
}

func TestInspectAndApproveForkOwnership(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult, api.EventToolResult} {
			t.Run(backend+"/"+string(cut), func(t *testing.T) {
				parent := approvalLog(t, backend)
				approvalSeed(t, parent, false, cut)
				child := approvalLog(t, backend)
				atSeq, err := parent.Head()
				if err != nil {
					t.Fatal(err)
				}
				if err := controller.Fork(parent, child, atSeq); err != nil {
					t.Fatal(err)
				}
				writer, err := child.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				got, err := controller.InspectApproval(child, 0)
				if err != nil || got == nil || !got.Inherited {
					t.Fatalf("fork state=%+v err=%v", got, err)
				}
				decision := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 2, Approved: true, Reason: "ok"}
				if _, err := controller.Approve(child, decision); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
					t.Fatalf("inherited decision err=%v", err)
				}
				if cut == api.EventApprovalResult {
					approvalAppend(t, child, writer, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: 2, ApprovalDecisionSeq: 3}})
					got, err = controller.InspectApproval(child, 0)
					if err != nil || got == nil || got.Receipt == nil || !got.Inherited {
						t.Fatalf("later inherited receipt=%+v err=%v", got, err)
					}
				}
				approvalAppend(t, child, writer, api.Event{ExecutionID: "e2", Kind: api.EventInput, Message: api.TextMessage("user", "new")})
				if got, err := controller.InspectApproval(child, 0); err != nil || got != nil {
					t.Fatalf("child new-turn escape state=%+v err=%v", got, err)
				}
			})
		}
	}
}

func TestInspectApprovalLatestGateWithLateOldRecords(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			log := approvalLog(t, backend)
			approvalSeed(t, log, false, api.EventApprovalResult)
			oldRequest := approvalRecord(t, log, api.EventApprovalRequest)
			oldDecision := approvalRecord(t, log, api.EventApprovalResult)
			call := approvalCall("c1") // IDs and keys may be reused by another execution.
			call.ExecutionID = "e2"
			approvalAppend(t, log, 0, call)
			request := approvalAppend(t, log, 0, api.Event{ExecutionID: "e2", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}})
			approvalAppend(t, log, 0,
				api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: oldRequest.Seq, ApprovalDecisionSeq: oldDecision.Seq}},
				api.Event{ExecutionID: "e1", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
				api.Event{ExecutionID: "e1", Kind: api.EventError, Err: &api.Error{Description: "late"}},
				api.Event{ExecutionID: "lifecycle", Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}})
			got, err := controller.InspectApproval(log, 0)
			if err != nil || got == nil || got.ExecutionID != "e2" || got.Request.Seq != request.Seq || got.Completed {
				t.Fatalf("latest state=%+v err=%v", got, err)
			}
			decision, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "e2", ToolCallID: "c1", RequestSeq: request.Seq, Approved: true})
			if err != nil || decision.Event.ExecutionID != "e2" {
				t.Fatalf("latest decision=%+v err=%v", decision, err)
			}
		})
	}
}

func TestInspectApprovalInvocationValidationIsScoped(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   *int64
		message *api.Message
		want    error
	}{
		{name: "missing count", message: api.TextMessage("user", "hi"), want: controller.ErrInvalidExecutionLog},
		{name: "negative count", count: approvalCount(-1), message: api.TextMessage("user", "hi"), want: controller.ErrInvalidExecutionLog},
		{name: "incomplete", count: approvalCount(2), message: api.TextMessage("user", "hi"), want: controller.ErrIncompleteInvocation},
		{name: "too many", count: approvalCount(0), message: api.TextMessage("user", "hi"), want: controller.ErrInvalidExecutionLog},
		{name: "missing message", count: approvalCount(1), want: controller.ErrInvalidExecutionLog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := approvalLog(t, "sqlite")
			approvalAppend(t, log, 0,
				api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: tc.count}},
				api.Event{ExecutionID: "e1", Kind: api.EventInput, Message: tc.message},
				approvalCall("c1"))
			request := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}})
			writer, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controller.InspectApproval(log, 0); !errors.Is(err, tc.want) {
				t.Fatalf("inspect err=%v want %v", err, tc.want)
			}
			if _, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: request.Seq}); !errors.Is(err, tc.want) {
				t.Fatalf("approve err=%v want %v", err, tc.want)
			}
			approvalAppend(t, log, writer, api.Event{ExecutionID: "e1", Kind: api.EventError, Err: &api.Error{Description: "still owns fence"}})
		})
	}
	for _, events := range [][]api.Event{
		nil,
		{{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}}},
		{{Kind: api.EventInput, Message: api.TextMessage("user", "ID-less legacy")}, {Kind: api.EventToolCall}},
		{{ExecutionID: "ordinary", Kind: api.EventExecutionStart}, {ExecutionID: "ordinary", Kind: api.EventToolCall}},
	} {
		log := approvalLog(t, "memory")
		approvalAppend(t, log, 0, events...)
		if got, err := controller.InspectApproval(log, 0); err != nil || got != nil {
			t.Fatalf("ordinary history state=%+v err=%v", got, err)
		}
	}
}

func approvalCount(n int64) *int64 { return &n }

func TestApproveColdReopenAndEmptyIdentityRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cold.db")
	s, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	log := s.Session("session")
	approvalSeed(t, log, true, api.EventToolCall)
	approvalAppend(t, log, 0, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}})
	request := approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}})
	input := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: request.Seq, Approved: true}
	decision, err := controller.Approve(log, input)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.SessionInfo("session")
	if err != nil {
		t.Fatal(err)
	}
	if info.ComputeState != api.ComputeCold || info.LastSeq != decision.Seq {
		t.Fatalf("host writes moved compute: %+v", info)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	input.Identity = api.IdentityRef{Principal: "", Issuer: "", Subject: ""}
	retry, err := controller.Approve(reopened.Session("session"), input)
	if err != nil || !reflect.DeepEqual(retry, decision) {
		t.Fatalf("reopened exact retry=%+v err=%v", retry, err)
	}
	info, err = reopened.SessionInfo("session")
	if err != nil {
		t.Fatal(err)
	}
	if info.ComputeState != api.ComputeCold || info.LastSeq != decision.Seq {
		t.Fatalf("reopened state=%+v", info)
	}
}

func TestApproveSessionScopedIDs(t *testing.T) {
	s, err := sqlitelog.Open(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, uid := range []string{"one", "two"} {
		approvalSeed(t, s.Session(uid), false, api.EventApprovalRequest)
	}
	for _, uid := range []string{"one", "two"} {
		rec, err := controller.Approve(s.Session(uid), api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 2, Approved: uid == "one"})
		if err != nil || rec.Event.ApprovalResult.Approved != (uid == "one") {
			t.Fatalf("%s decision=%+v err=%v", uid, rec, err)
		}
	}
}

// Fault injection decorates a real store; read/append/fence side effects remain real.
type approvalFaultStore struct {
	eventlog.Store
	readErr, fenceErr, appendErr error
	onFence                      func(int64)
}

func (s approvalFaultStore) Read(seq int64) ([]eventlog.Record, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.Store.Read(seq)
}
func (s approvalFaultStore) NewFence() (int64, error) {
	if s.fenceErr != nil {
		return 0, s.fenceErr
	}
	f, err := s.Store.NewFence()
	if err == nil && s.onFence != nil {
		s.onFence(f)
	}
	return f, err
}
func (s approvalFaultStore) Append(head, fence int64, ev api.Event) (eventlog.Record, error) {
	if s.appendErr != nil {
		return eventlog.Record{}, s.appendErr
	}
	return s.Store.Append(head, fence, ev)
}

func TestApproveOperationalCausesAndHeadCAS(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, op := range []string{"read", "fence", "append conflict", "append fenced", "head advance"} {
			t.Run(backend+"/"+op, func(t *testing.T) {
				log := approvalLog(t, backend)
				approvalSeed(t, log, false, api.EventApprovalRequest)
				fault := approvalFaultStore{Store: log}
				cause := errors.New("backend unavailable")
				switch op {
				case "read":
					fault.readErr = fmt.Errorf("read: %w", cause)
				case "fence":
					fault.fenceErr = fmt.Errorf("fence: %w", cause)
				case "append conflict":
					cause = eventlog.ErrConflict
					fault.appendErr = fmt.Errorf("append: %w", cause)
				case "append fenced":
					cause = eventlog.ErrFenced
					fault.appendErr = fmt.Errorf("append: %w", cause)
				case "head advance":
					cause = eventlog.ErrConflict
					fault.onFence = func(fence int64) {
						approvalAppend(t, log, fence, api.Event{ExecutionID: "e2", Kind: api.EventInput, Message: api.TextMessage("user", "raced")})
					}
				}
				_, err := controller.Approve(fault, api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 2, Approved: true})
				if !errors.Is(err, cause) || errors.Is(err, controller.ErrReplayDiverged) || errors.Is(err, controller.ErrInvalidApprovalDecision) {
					t.Fatalf("err=%v want operational cause %v", err, cause)
				}
				if op == "head advance" && err != eventlog.ErrConflict {
					t.Fatalf("head advance err=%v want exact conflict", err)
				}
				recs, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range recs {
					if rec.Event.Kind == api.EventApprovalResult {
						t.Fatal("failed operation wrote a decision")
					}
				}
			})
		}
	}
}
