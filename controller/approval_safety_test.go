package controller_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
)

// Pending and decided inherited requests must never cross into the child's effect namespace;
// a completed receipt remains replayable even when written after the fork marker.
func TestApprovalInheritedCutsRefuseOrServe(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult, api.EventToolResult, api.EventEnd} {
			t.Run(backend+"/"+string(cut), func(t *testing.T) {
				parent, child := approvalLog(t, backend), approvalLog(t, backend)
				approvalSeed(t, parent, true, cut)
				head, _ := parent.Head()
				if err := controller.Fork(parent, child, head); err != nil {
					t.Fatal(err)
				}
				before, _ := child.Read(1)
				c, err := controller.New(child, nil, controller.WithSessionUID("child"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					t.Fatal("inherited effect invoked")
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				if cut == api.EventToolCall || cut == api.EventApprovalRequest || cut == api.EventApprovalResult {
					if resumed, err := c.Resume(t.Context(), nil); resumed || !errors.Is(err, controller.ErrInheritedToolIntent) {
						t.Fatalf("inherited Resume=%t,%v", resumed, err)
					}
					after, _ := child.Read(1)
					if !reflect.DeepEqual(before, after) {
						t.Fatal("refusal wrote journal")
					}
					// Explicit child Exec is the safe escape, including an inputless new turn.
					childHead, _ := child.Head()
					if err := c.Exec(t.Context(), gateHarness(func(context.Context, *api.Start, api.EventSink) error { return nil }), nil, childHead); err != nil {
						t.Fatalf("child escape=%v", err)
					}
					return
				}
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					result, err := sink.ToolCall(ctx, gatedCall())
					if err != nil {
						return err
					}
					if result.Approval == nil || !result.Approval.Approved || result.ID != "c1" {
						t.Errorf("served inherited receipt=%+v", result)
					}
					return nil
				})
				if cut == api.EventToolResult {
					if resumed, err := c.Resume(t.Context(), h); !resumed || err != nil {
						t.Fatalf("completed intent Resume=%t,%v", resumed, err)
					}
				}
				if _, err := c.Replay(t.Context(), h); err != nil {
					t.Fatalf("inherited receipt Replay=%v", err)
				}
			})
		}
		t.Run(backend+"/receipt after fork", func(t *testing.T) {
			parent, child := approvalLog(t, backend), approvalLog(t, backend)
			approvalSeed(t, parent, true, api.EventApprovalResult)
			head, _ := parent.Head()
			if err := controller.Fork(parent, child, head); err != nil {
				t.Fatal(err)
			}
			req, dec := approvalRecord(t, child, api.EventApprovalRequest), approvalRecord(t, child, api.EventApprovalResult)
			fence, _ := child.NewFence()
			approvalAppend(t, child, fence, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: req.Seq, ApprovalDecisionSeq: dec.Seq}})
			c, _ := controller.New(child, nil, controller.WithSessionUID("child"))
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				result, err := sink.ToolCall(ctx, gatedCall())
				if err == nil && result.Approval == nil {
					t.Fatal("missing inherited decision")
				}
				return err
			})
			if resumed, err := c.Resume(t.Context(), h); !resumed || err != nil {
				t.Fatalf("later receipt Resume=%t,%v", resumed, err)
			}
		})
	}
}

func TestApprovalIncompleteReceiptEvidenceFailsClosed(t *testing.T) {
	for _, path := range []string{"resume", "replay"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "memory")
			approvalAppend(t, log, 0, approvalCall("c1"), api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1"}})
			if path == "replay" {
				approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
			}
			c, _ := controller.New(log, nil, controller.WithSessionUID("session"))
			head, _ := log.Head()
			var err error
			if path == "resume" {
				_, err = c.Resume(t.Context(), nil)
			} else {
				_, err = c.Replay(t.Context(), nil)
			}
			if !errors.Is(err, controller.ErrInvalidExecutionLog) {
				t.Fatalf("malformed approval receipt=%v", err)
			}
			after, _ := log.Head()
			if after != head {
				t.Fatal("invalid evidence appended")
			}
		})
	}
}

func TestApprovalRecordedIdentityDivergenceIsSticky(t *testing.T) {
	for _, path := range []string{"resume decided", "resume receipt", "replay"} {
		for _, change := range []struct {
			name   string
			mutate func(*api.ToolCall)
		}{
			{"id", func(c *api.ToolCall) { c.ID = "other" }},
			{"tool", func(c *api.ToolCall) { c.Tool = "other" }},
			{"args", func(c *api.ToolCall) { c.Args = map[string]any{"path": "other"} }},
			{"key", func(c *api.ToolCall) { c.IdempotencyKey = "other" }},
			{"mediation", func(c *api.ToolCall) { c.Mediation = api.MediationControllerMediated }},
		} {
			t.Run(path+"/"+change.name, func(t *testing.T) {
				log := approvalLog(t, "sqlite")
				cut := api.EventApprovalResult
				if path == "resume receipt" {
					cut = api.EventToolResult
				}
				if path == "replay" {
					cut = api.EventEnd
				}
				approvalSeed(t, log, true, cut)
				c, _ := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					t.Fatal("mismatch invoked executor")
					return api.ToolResult{}, nil
				}))
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					call := gatedCall()
					change.mutate(&call)
					_, err := sink.ToolCall(ctx, call)
					if !errors.Is(err, controller.ErrReplayDiverged) {
						t.Errorf("identity error=%v", err)
					}
					for _, got := range gateSinkCalls(ctx, sink) {
						if !errors.Is(got, controller.ErrReplayDiverged) {
							t.Errorf("handled mismatch=%v", got)
						}
					}
					return nil
				})
				var err error
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Fatalf("Run=%v", err)
				}
			})
		}
	}
}

func TestApprovalKeylessMediationMismatchCannotRetryGate(t *testing.T) {
	for _, path := range []string{"resume decided", "resume receipt", "replay"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "sqlite")
			cut := api.EventApprovalResult
			switch path {
			case "resume receipt":
				cut = api.EventToolResult
			case "replay":
				cut = api.EventEnd
			}
			approvalSeed(t, log, true, cut)
			head, _ := log.Head()
			effects := 0
			c, err := controller.New(log, echoModel, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				effects++
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				changed := gatedCall()
				changed.Mediation, changed.IdempotencyKey = api.MediationControllerMediated, ""
				_, err := sink.ToolCall(ctx, changed)
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Errorf("changed gate=%v, want ErrReplayDiverged", err)
				}
				_, err = sink.ToolCall(ctx, gatedCall())
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Errorf("retry original gate=%v, want sticky divergence", err)
				}
				for _, err := range gateSinkCalls(ctx, sink) {
					if !errors.Is(err, controller.ErrReplayDiverged) {
						t.Errorf("continuation=%v, want sticky divergence", err)
					}
				}
				return nil
			})
			if path == "replay" {
				_, err = c.Replay(t.Context(), h)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			if !errors.Is(err, controller.ErrReplayDiverged) || effects != 0 || c.ModelInvocations() != 0 {
				t.Errorf("run=%v effects=%d models=%d", err, effects, c.ModelInvocations())
			}
			records, err := log.Read(head + 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range records {
				if rec.Event.Kind != api.EventError {
					t.Errorf("divergence wrote continuation: %s", rec.Event.Kind)
				}
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.InspectApproval(log, 0); err != nil {
				t.Fatalf("divergence poisoned journal: %v", err)
			}
		})
	}
}

func TestApprovalHistoricalKeylessOrdinarySameIDFallback(t *testing.T) {
	for _, path := range []string{"resume", "replay"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "memory")
			call := recordedToolCall()
			appendToolEvidence(t, log, []api.Event{
				{Kind: api.EventToolCall, ToolCall: &call},
				{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}},
				{Kind: api.EventOutput, Message: api.TextMessage("assistant", "ordinary fallback")},
			}, path == "replay")
			c, err := controller.New(log, nil, controller.WithSessionUID("session"))
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				keyless := call
				keyless.IdempotencyKey = ""
				if _, err := sink.ToolCall(ctx, keyless); !errors.Is(err, controller.ErrMissingIdempotencyKey) || errors.Is(err, controller.ErrReplayDiverged) {
					t.Errorf("historical ordinary rejection=%v", err)
				}
				if result, err := sink.ToolCall(ctx, call); err != nil || result.ID != call.ID {
					t.Fatalf("same-ID ordinary fallback=%+v,%v", result, err)
				}
				return sink.Output(ctx, "ordinary fallback")
			})
			if path == "replay" {
				_, err = c.Replay(t.Context(), h)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			if err != nil || c.ToolInvocations() != 0 {
				t.Fatalf("historical ordinary continuation=%v effects=%d", err, c.ToolInvocations())
			}
		})
	}
}

func TestApprovalCorruptCorrelationPreflight(t *testing.T) {
	for _, path := range []string{"resume", "resume complete", "replay"} {
		for _, corruption := range []struct {
			name   string
			kind   api.EventKind
			mutate func(*api.Event)
		}{
			{"request call", api.EventApprovalRequest, func(e *api.Event) { e.Approval.ToolCallID = "wrong" }},
			{"decision call", api.EventApprovalResult, func(e *api.Event) { e.ApprovalResult.ToolCallID = "wrong" }},
			{"decision seq", api.EventApprovalResult, func(e *api.Event) { e.ApprovalResult.RequestSeq = 1 }},
			{"receipt call", api.EventToolResult, func(e *api.Event) { e.Result.ID = "wrong" }},
			{"receipt request", api.EventToolResult, func(e *api.Event) { e.Result.ApprovalRequestSeq = 1 }},
			{"receipt decision", api.EventToolResult, func(e *api.Event) { e.Result.ApprovalDecisionSeq = 2 }},
			{"receipt status", api.EventToolResult, func(e *api.Event) { e.Result.Code = api.ToolResultCodeApprovalDenied; e.Result.IsError = true }},
		} {
			t.Run(path+"/"+corruption.name, func(t *testing.T) {
				log := approvalLog(t, "memory")
				for _, ev := range approvalEvidence() {
					if ev.Kind == corruption.kind {
						corruption.mutate(&ev)
					}
					approvalAppend(t, log, 0, ev)
				}
				if path != "resume" {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
				}
				head, _ := log.Head()
				c, _ := controller.New(log, nil, controller.WithSessionUID("session"))
				var err error
				if path == "replay" {
					_, err = c.Replay(t.Context(), nil)
				} else {
					_, err = c.Resume(t.Context(), nil)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Fatalf("correlation=%v", err)
				}
				after, _ := log.Head()
				if after != head {
					t.Fatal("corrupt evidence appended")
				}
			})
		}
	}
}

func TestApprovalDescriptorReuseAndMissingUIDRecovery(t *testing.T) {
	for _, scenario := range []string{"memory decided", "empty UID decided", "single Describe live", "single Describe recovery"} {
		t.Run(scenario, func(t *testing.T) {
			log := approvalLog(t, "memory")
			if scenario != "single Describe live" {
				approvalSeed(t, log, true, api.EventApprovalResult)
			}
			uid := "session"
			if scenario == "empty UID decided" {
				uid = ""
			}
			c, _ := controller.New(log, nil, controller.WithSessionUID(uid))
			descriptions, runs := 0, 0
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				runs++
				_, err := sink.ToolCall(ctx, gatedCall())
				return err
			})
			desc := h.desc
			if scenario == "memory decided" {
				desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
			}
			h.describe = func(context.Context) (api.Descriptor, error) {
				descriptions++
				if descriptions > 1 {
					t.Error("descriptor described twice")
				}
				return desc, nil
			}
			var err error
			if scenario == "single Describe live" {
				err = c.Exec(t.Context(), h, nil, 0)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			want := controller.ErrMissingToolExecutor
			if scenario == "memory decided" {
				want = controller.ErrApprovalUnavailable
			}
			if scenario == "empty UID decided" {
				want = controller.ErrMissingSessionUID
			}
			if scenario == "single Describe live" {
				want = api.ErrApprovalParked
			}
			if !errors.Is(err, want) || descriptions != 1 {
				t.Fatalf("err=%v Describe=%d runs=%d", err, descriptions, runs)
			}
			assertNoGateContinuation(t, log)
		})
	}
}

func TestApprovalFailedHandoffPreservesAwaitingState(t *testing.T) {
	for _, path := range []string{"exec", "resume tail"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "sqlite")
			if path == "resume tail" {
				approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "recorded", HarnessVersion: "v1", InputCount: inputCount(0)}})
			}
			c, _ := controller.New(log, nil, controller.WithSessionUID("session"))
			cause := errors.New("handoff disconnected")
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				_, err := sink.ToolCall(ctx, gatedCall())
				if !errors.Is(err, api.ErrApprovalParked) {
					t.Errorf("park=%v", err)
				}
				return cause
			})
			var err error
			if path == "exec" {
				err = c.Exec(t.Context(), h, nil, 0)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			if !errors.Is(err, cause) || errors.Is(err, controller.ErrReplayDiverged) {
				t.Fatalf("handoff=%v", err)
			}
			assertNoGateContinuation(t, log)
			if resumed, err := c.Resume(t.Context(), nil); !resumed || !errors.Is(err, api.ErrApprovalParked) {
				t.Fatalf("pending=%t,%v", resumed, err)
			}
		})
	}
}

// Gate IDs cannot collide with an earlier ordinary intent or a completed gated receipt, on
// either direct Exec or the live tail of Resume; no normalization or second intent is permitted.
func TestApprovalCallIDCannotBeReused(t *testing.T) {
	for _, path := range []string{"exec ordinary", "resume ordinary", "resume approval", "resume approval to ordinary"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "memory")
			ordinary := gatedCall()
			ordinary.Mediation = api.MediationControllerMediated
			if path == "resume ordinary" {
				appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &ordinary}, {Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1"}}}, false)
			}
			if path == "resume approval" || path == "resume approval to ordinary" {
				approvalSeed(t, log, true, api.EventToolResult)
			}
			c, _ := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				return api.ToolResult{}, nil
			}))
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				first := ordinary
				if path == "resume approval" || path == "resume approval to ordinary" {
					first = gatedCall()
				}
				if _, err := sink.ToolCall(ctx, first); err != nil {
					return err
				}
				second := gatedCall()
				if path == "resume approval to ordinary" {
					second = ordinary
				}
				_, err := sink.ToolCall(ctx, second)
				return err
			})
			var err error
			if path == "exec ordinary" {
				err = c.Exec(t.Context(), h, nil, 0)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			if !errors.Is(err, controller.ErrInvalidApprovalCall) {
				t.Fatalf("duplicate=%v", err)
			}
			records, _ := log.Read(1)
			calls := 0
			for _, rec := range records {
				if rec.Event.Kind == api.EventToolCall {
					calls++
				}
			}
			if calls != 1 {
				t.Fatalf("duplicate intent count=%d", calls)
			}
		})
	}
}

func TestApprovalReportCannotReuseCompletedGateID(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventApprovalResult, api.EventToolResult} {
		t.Run(string(cut), func(t *testing.T) {
			log := approvalLog(t, "sqlite")
			approvalSeed(t, log, true, cut)
			effects := 0
			c, err := controller.New(log, echoModel, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				effects++
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
					return err
				}
				head, _ := log.Head()
				err := sink.Report(ctx, api.ToolResult{ID: "c1"})
				if !errors.Is(err, controller.ErrApprovalReceiptReport) {
					t.Errorf("colliding Report=%v, want ErrApprovalReceiptReport", err)
				}
				for _, err := range gateSinkCalls(ctx, sink) {
					if !errors.Is(err, controller.ErrApprovalReceiptReport) {
						t.Errorf("handled collision continuation=%v", err)
					}
				}
				after, _ := log.Head()
				if after != head {
					t.Errorf("collision/continuation appended: head=%d after=%d", head, after)
				}
				return nil
			})
			if resumed, err := c.Resume(t.Context(), h); !resumed || !errors.Is(err, controller.ErrApprovalReceiptReport) {
				t.Errorf("Resume=%t,%v", resumed, err)
			}
			wantEffects := 0
			if cut == api.EventApprovalResult {
				wantEffects = 1
			}
			if effects != wantEffects || c.ModelInvocations() != 0 {
				t.Errorf("effects after rejection=%d models=%d", effects, c.ModelInvocations())
			}
			state, err := controller.InspectApproval(log, 0)
			if err != nil || state == nil || state.Receipt == nil || state.Completed {
				t.Fatalf("collision poisoned journal or completed turn: %+v,%v", state, err)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
			continuation := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
					return err
				}
				if err := sink.Report(ctx, api.ToolResult{ID: "ordinary"}); err != nil {
					return err
				}
				return sink.Output(ctx, "recovered")
			})
			if resumed, err := c.Resume(t.Context(), continuation); !resumed || err != nil {
				t.Fatalf("next Resume=%t,%v", resumed, err)
			}
			if _, err := c.Replay(t.Context(), continuation); err != nil {
				t.Fatalf("recovered Replay=%v", err)
			}
			if effects != wantEffects {
				t.Fatal("completed gate redriven")
			}
			head, _ := log.Head()
			ordinary := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				for _, id := range []string{"c1", "c1", ""} {
					if err := sink.Report(ctx, api.ToolResult{ID: id}); err != nil {
						return err
					}
				}
				return nil
			})
			if err := c.Exec(t.Context(), ordinary, nil, head); err != nil {
				t.Fatalf("ordinary Report behavior broadened across invocation: %v", err)
			}
			if _, err := controller.InspectApproval(log, 0); err != nil {
				t.Fatalf("next Exec journal invalid: %v", err)
			}
		})
	}
}

func TestApprovalRecordedReportCollisionIsSticky(t *testing.T) {
	for _, path := range []string{"resume", "replay"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry=%t", path, retry), func(t *testing.T) {
				log := approvalLog(t, "sqlite")
				approvalSeed(t, log, true, api.EventToolResult)
				approvalAppend(t, log, 0,
					api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "ordinary-B"}},
					api.Event{ExecutionID: "e1", Kind: api.EventOutput, Message: api.TextMessage("assistant", "recorded continuation")})
				if path == "replay" {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
				}
				if _, err := controller.InspectApproval(log, 0); err != nil {
					t.Fatalf("fixture is not a valid journal: %v", err)
				}
				head, _ := log.Head()
				effects := 0
				c, err := controller.New(log, echoModel, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					effects++
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
						return err
					}
					collision := sink.Report(ctx, api.ToolResult{ID: "c1"})
					if !errors.Is(collision, controller.ErrReplayDiverged) || !errors.Is(collision, controller.ErrApprovalReceiptReport) {
						t.Errorf("recorded Report collision=%v, want sticky gated-ID divergence", collision)
					}
					if retry {
						if got := sink.Report(ctx, api.ToolResult{ID: "ordinary-B"}); got != collision {
							t.Errorf("retry B=%v, want original sticky error %v", got, collision)
						}
					}
					if got := sink.Output(ctx, "recorded continuation"); got != collision {
						t.Errorf("recorded continuation=%v, want original sticky error %v", got, collision)
					}
					if path == "resume" {
						for _, got := range gateSinkCalls(ctx, sink) {
							if got != collision {
								t.Errorf("live continuation=%v, want original sticky error %v", got, collision)
							}
						}
					}
					return nil // even a swallowed collision must fail the invocation
				})
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) || !errors.Is(err, controller.ErrApprovalReceiptReport) || effects != 0 || c.ModelInvocations() != 0 {
					t.Errorf("Run=%v effects=%d models=%d", err, effects, c.ModelInvocations())
				}
				records, err := log.Read(head + 1)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range records {
					if rec.Event.Kind != api.EventError {
						t.Errorf("handled recorded collision wrote %s", rec.Event.Kind)
					}
				}
				positive := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
						return err
					}
					if err := sink.Report(ctx, api.ToolResult{ID: "ordinary-B"}); err != nil {
						return err
					}
					return sink.Output(ctx, "recorded continuation")
				})
				if path == "resume" {
					if resumed, err := c.Resume(t.Context(), positive); !resumed || err != nil {
						t.Fatalf("original B continuation=%t,%v", resumed, err)
					}
				}
				if _, err := c.Replay(t.Context(), positive); err != nil {
					t.Fatalf("original B replay=%v", err)
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestApprovalRecordedReportsKeepOrdinaryKindOnlySemantics(t *testing.T) {
	for _, path := range []string{"resume", "replay"} {
		for _, marked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/marked=%t", path, marked), func(t *testing.T) {
				log := approvalLog(t, "sqlite")
				if marked {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "recorded", HarnessVersion: "v1", InputCount: inputCount(0)}})
				}
				// This ID belongs to a gate only later in the same invocation. Scanned approvals
				// must not reject a report before that gate has actually matched.
				approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1"}})
				approvalSeed(t, log, false, api.EventToolResult)
				approvalAppend(t, log, 0,
					api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "ordinary-B"}},
					api.Event{ExecutionID: "e1", Kind: api.EventOutput, Message: api.TextMessage("assistant", "ordinary continuation")})
				if path == "replay" {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
				}
				if _, err := controller.InspectApproval(log, 0); err != nil {
					t.Fatalf("invalid preservation fixture: %v", err)
				}
				c, err := controller.New(log, nil, controller.WithSessionUID("session"))
				if err != nil {
					t.Fatal(err)
				}
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					if err := sink.Report(ctx, api.ToolResult{ID: "c1"}); err != nil {
						return err
					}
					if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
						return err
					}
					// Ordinary report ID differences are historical kind-only behavior, not
					// gated receipt collisions, and must remain accepted in recorded prefixes.
					if err := sink.Report(ctx, api.ToolResult{ID: "ordinary-changed"}); err != nil {
						return err
					}
					return sink.Output(ctx, "ordinary continuation")
				})
				if path == "resume" {
					_, err = c.Resume(t.Context(), h)
				} else {
					_, err = c.Replay(t.Context(), h)
				}
				if err != nil || c.ToolInvocations() != 0 {
					t.Fatalf("ordinary recorded semantics changed: %v effects=%d", err, c.ToolInvocations())
				}
			})
		}
	}
}

func TestApprovalNextLiveTailCanGateAgain(t *testing.T) {
	log := approvalLog(t, "sqlite")
	approvalSeed(t, log, true, api.EventToolResult)
	c, _ := controller.New(log, nil, controller.WithSessionUID("session"))
	h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
			return err
		}
		call := gatedCall()
		call.ID, call.IdempotencyKey = "c2", "key2"
		_, err := sink.ToolCall(ctx, call)
		return err
	})
	if resumed, err := c.Resume(t.Context(), h); !resumed || !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("next gate=%t,%v", resumed, err)
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil || state == nil || state.Call.ID != "c2" || state.Request == nil || state.Receipt != nil {
		t.Fatalf("next state=%+v,%v", state, err)
	}
	head, _ := log.Head()
	if err := c.Exec(t.Context(), nil, nil, head); !errors.Is(err, controller.ErrApprovalBlocked) {
		t.Fatalf("second admission=%v", err)
	}
}

// Concurrent sink users are serialized even while live; no effect may occur after the gated
// intent regardless of scheduling. Concurrent use after Run closure exercises every method.
func TestApprovalConcurrentLiveAndClosedMethods(t *testing.T) {
	log := approvalLog(t, "memory")
	c, _ := controller.New(log, echoModel, controller.WithSessionUID("session"))
	var retained api.EventSink
	h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		retained = sink
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sink.Output(ctx, "before park")
				if err != nil && !errors.Is(err, api.ErrApprovalParked) {
					t.Errorf("concurrent Output=%v", err)
				}
			}()
		}
		_, err := sink.ToolCall(ctx, gatedCall())
		wg.Wait()
		return err
	})
	if err := c.Exec(t.Context(), h, nil, 0); !errors.Is(err, api.ErrApprovalParked) {
		t.Fatal(err)
	}
	records, _ := log.Read(1)
	open := false
	for _, rec := range records {
		if open && rec.Event.Kind != api.EventApprovalRequest {
			t.Fatalf("continuation after intent=%s", rec.Event.Kind)
		}
		if rec.Event.Kind == api.EventToolCall {
			open = true
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, err := range gateSinkCalls(t.Context(), retained) {
				if !errors.Is(err, api.ErrApprovalParked) {
					t.Errorf("late error=%v", err)
				}
			}
		}()
	}
	wg.Wait()
	if _, err := controller.InspectApproval(log, 0); err != nil {
		t.Fatal(err)
	}
}

// Old stateless journals can contain a handled rejection with no approval intent. Neither
// serving a distinct fallback nor an exhausted prefix may manufacture an approval prompt.
func TestApprovalHistoricalStatelessPreIntentFallback(t *testing.T) {
	for _, path := range []string{"replay", "resume"} {
		for _, fallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fallback=%t", path, fallback), func(t *testing.T) {
				log := approvalLog(t, "sqlite")
				call := recordedToolCall()
				var effects []api.Event
				if fallback {
					effects = append(effects, api.Event{Kind: api.EventToolCall, ToolCall: &call}, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}})
				}
				// Include recorded continuation, so Resume sees the rejected call in the recorded prefix.
				effects = append(effects, api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", "legacy continuation")})
				appendToolEvidence(t, log, effects, path == "replay")
				c, _ := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					t.Fatal("historical replay invoked executor")
					return api.ToolResult{}, nil
				}))
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					_, err := sink.ToolCall(ctx, gatedCall())
					if !errors.Is(err, controller.ErrApprovalUnavailable) {
						t.Errorf("historical rejection=%v", err)
					}
					if fallback {
						result, err := sink.ToolCall(ctx, call)
						if err != nil || result.ID != call.ID {
							t.Fatalf("fallback=%+v,%v", result, err)
						}
					}
					if err := sink.Output(ctx, "legacy continuation"); err != nil {
						return err
					}
					// Once consumed, Replay still reproduces an old rejection instead of prompting.
					if path == "replay" {
						_, err := sink.ToolCall(ctx, gatedCall())
						if !errors.Is(err, controller.ErrApprovalUnavailable) {
							t.Errorf("exhausted replay=%v", err)
						}
					}
					return nil
				})
				var err error
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if err != nil {
					t.Fatal(err)
				}
				records, _ := log.Read(1)
				for _, rec := range records {
					if rec.Event.Kind == api.EventApprovalRequest || rec.Event.Kind == api.EventApprovalResult {
						t.Fatal("historical rejection created approval evidence")
					}
				}
			})
		}
	}
}

// A handled recorded-prefix mismatch on any entry point must latch divergence; otherwise it
// could consume/skip evidence later and append END or activate live effects after a bad replay.
func TestApprovalRecordedPrefixMismatchAllMethodsLatches(t *testing.T) {
	for _, path := range []string{"resume", "replay"} {
		for _, method := range []string{"model", "output", "report", "usage"} {
			t.Run(path+"/"+method, func(t *testing.T) {
				log := approvalLog(t, "memory")
				approvalSeed(t, log, true, api.EventToolResult)
				approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventOutput, Message: api.TextMessage("assistant", "recorded output")})
				if path == "replay" {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
				}
				c, _ := controller.New(log, echoModel, controller.WithSessionUID("session"))
				h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					if _, err := sink.ToolCall(ctx, gatedCall()); err != nil {
						return err
					}
					var err error
					switch method {
					case "model":
						_, err = sink.Model(ctx, api.ModelRequest{Model: "unexpected"})
					case "output":
						err = sink.Output(ctx, "mismatch")
					case "report":
						err = sink.Report(ctx, api.ToolResult{ID: "unrelated"})
					case "usage":
						err = sink.Usage(ctx, api.Usage{})
					}
					if !errors.Is(err, controller.ErrReplayDiverged) {
						t.Errorf("%s mismatch=%v", method, err)
					}
					for _, got := range gateSinkCalls(ctx, sink) {
						if !errors.Is(got, controller.ErrReplayDiverged) {
							t.Errorf("handled prefix mismatch=%v", got)
						}
					}
					return nil
				})
				var err error
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Fatalf("Run=%v", err)
				}
			})
		}
	}
}
