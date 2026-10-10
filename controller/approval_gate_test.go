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
	"github.com/aramase/agentsessions/eventlog"
)

func gatedCall() api.ToolCall { return *approvalCall("c1").ToolCall }

func gateHarness(run func(context.Context, *api.Start, api.EventSink) error) identityHarness {
	return identityHarness{desc: api.Descriptor{ID: "recorded", Version: "v1", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, run: run}
}

func assertNoGateContinuation(t *testing.T, log eventlog.Store) {
	t.Helper()
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		switch rec.Event.Kind {
		case api.EventEnd, api.EventError, api.EventOutput, api.EventModelCall, api.EventToolResult, api.EventUsage:
			t.Fatalf("unexpected continuation %s at %d", rec.Event.Kind, rec.Seq)
		}
	}
}

// Removing the gate or writing a terminal event for a durable park must fail this live test.
func TestApprovalLiveStatelessKeyedCallParks(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			log := approvalLog(t, backend)
			c, err := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				t.Fatal("unapproved effect invoked")
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, start *api.Start, sink api.EventSink) error {
				if start.SessionUID != "session" {
					t.Errorf("SessionUID=%q", start.SessionUID)
				}
				_, err := sink.ToolCall(ctx, gatedCall())
				return err
			})
			err = c.Exec(t.Context(), h, []api.Message{msg("write")}, 0)
			var parked *api.ApprovalParkedError
			if !errors.Is(err, api.ErrApprovalParked) || !errors.As(err, &parked) {
				t.Fatalf("want typed park, got %v", err)
			}
			call := approvalRecord(t, log, api.EventToolCall)
			request := approvalRecord(t, log, api.EventApprovalRequest)
			if call.Event.ToolCall.ID != "c1" || call.Event.ToolCall.IdempotencyKey != "key" || request.Seq <= call.Seq || parked.Ref.ExecutionID != call.Event.ExecutionID || parked.Ref.ToolCallID != "c1" || parked.Ref.RequestSeq != request.Seq {
				t.Fatalf("park=%+v call=%+v request=%+v", parked.Ref, call, request)
			}
			assertNoGateContinuation(t, log)
		})
	}
}

// Every entry point must reject a stopped/closed invocation before invoking or appending.
func gateSinkCalls(ctx context.Context, sink api.EventSink) []error {
	_, modelErr := sink.Model(ctx, api.ModelRequest{Model: "test"})
	_, toolErr := sink.ToolCall(ctx, recordedToolCall())
	return []error{modelErr, toolErr, sink.Output(ctx, "masked"), sink.Report(ctx, api.ToolResult{ID: "report"}), sink.Usage(ctx, api.Usage{})}
}

func TestApprovalSwallowedParkSealsAllMethodsAndLateUse(t *testing.T) {
	log := approvalLog(t, "sqlite")
	c, err := controller.New(log, nil, controller.WithSessionUID("session"))
	if err != nil {
		t.Fatal(err)
	}
	var retained api.EventSink
	h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		retained = sink
		_, err := sink.ToolCall(ctx, gatedCall())
		if !errors.Is(err, api.ErrApprovalParked) {
			return err
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, got := range gateSinkCalls(ctx, sink) {
					if !errors.Is(got, api.ErrApprovalParked) {
						t.Errorf("stopped call=%v", got)
					}
				}
			}()
		}
		wg.Wait()
		return nil
	})
	if err := c.Exec(t.Context(), h, nil, 0); !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("swallowed park=%v", err)
	}
	for _, got := range gateSinkCalls(t.Context(), retained) {
		if !errors.Is(got, api.ErrApprovalParked) {
			t.Errorf("late call=%v", got)
		}
	}
	assertNoGateContinuation(t, log)
}

func TestInvocationNormalClosureSealsAllMethods(t *testing.T) {
	for _, path := range []string{"exec", "resume", "replay"} {
		t.Run(path, func(t *testing.T) {
			log := approvalLog(t, "memory")
			if path != "exec" {
				approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0)}})
				if path == "replay" {
					approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventEnd})
				}
			}
			c, err := controller.New(log, echoModel, controller.WithSessionUID("session"))
			if err != nil {
				t.Fatal(err)
			}
			var sink api.EventSink
			h := gateHarness(func(_ context.Context, _ *api.Start, got api.EventSink) error { sink = got; return nil })
			switch path {
			case "exec":
				err = c.Exec(t.Context(), h, nil, 0)
			case "resume":
				_, err = c.Resume(t.Context(), h)
			case "replay":
				_, err = c.Replay(t.Context(), h)
			}
			if err != nil {
				t.Fatal(err)
			}
			head, _ := log.Head()
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, got := range gateSinkCalls(t.Context(), sink) {
						if !errors.Is(got, controller.ErrSinkClosed) {
							t.Errorf("late call=%v", got)
						}
					}
				}()
			}
			wg.Wait()
			after, _ := log.Head()
			if after != head {
				t.Fatal("closed sink wrote events")
			}
		})
	}
}

func TestApprovalAdmissionAndPendingRepairWithoutHarness(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult} {
			t.Run(backend+"/"+string(cut), func(t *testing.T) {
				log := approvalLog(t, backend)
				approvalSeed(t, log, true, cut)
				c, err := controller.New(log, nil, controller.WithSessionUID("session"))
				if err != nil {
					t.Fatal(err)
				}
				head, _ := log.Head()
				for _, inputs := range [][]api.Message{nil, {msg("new")}} {
					if err := c.Exec(t.Context(), nil, inputs, head); !errors.Is(err, controller.ErrApprovalBlocked) {
						t.Fatalf("new Exec=%v", err)
					}
				}
				if cut == api.EventApprovalResult {
					return
				}
				for i := 0; i < 2; i++ {
					resumed, err := c.Resume(t.Context(), nil)
					var park *api.ApprovalParkedError
					if !resumed || !errors.As(err, &park) || park.Ref.RequestSeq <= 0 || park.Ref.ExecutionID != "e1" || park.Ref.ToolCallID != "c1" {
						t.Fatalf("pending Resume=%t,%v", resumed, err)
					}
				}
				records, _ := log.Read(1)
				requests := 0
				for _, rec := range records {
					if rec.Event.Kind == api.EventApprovalRequest {
						requests++
					}
				}
				if requests != 1 {
					t.Fatalf("requests=%d", requests)
				}
				want := head
				if cut == api.EventToolCall {
					want++
				}
				after, _ := log.Head()
				if after != want {
					t.Fatalf("head=%d want=%d", after, want)
				}
				assertNoGateContinuation(t, log)
			})
		}
	}
}

func TestApprovalDecidedRecoveryAndCompletedReplay(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, outcome := range []string{"approved", "denied", "executor error", "error result"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				log := approvalLog(t, backend)
				approvalSeed(t, log, true, api.EventApprovalRequest)
				request := approvalRecord(t, log, api.EventApprovalRequest)
				decision, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: request.Seq, Approved: outcome != "denied", Reason: "decision reason"})
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				opts := []controller.Option{controller.WithSessionUID("session"), controller.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
					calls++
					if scope.SessionUID != "session" || !reflect.DeepEqual(call, gatedCall()) {
						t.Fatalf("changed original scope/call=%+v/%+v", scope, call)
					}
					if outcome == "executor error" {
						return api.ToolResult{}, errToolInterrupted
					}
					return api.ToolResult{ID: "forged", IsError: outcome == "error result", Output: map[string]any{"receipt": "effect"}}, nil
				}), controller.WithObserver(controller.Observer{OnRecord: func(rec eventlog.Record) {
					if rec.Event.Kind == api.EventToolResult && rec.Event.Result.Approval != nil {
						t.Error("transient decision exposed before durable append")
					}
				}})}
				c, err := controller.New(log, nil, opts...)
				if err != nil {
					t.Fatal(err)
				}
				var served []api.ToolResult
				h := gateHarness(func(ctx context.Context, start *api.Start, sink api.EventSink) error {
					if start.SessionUID != "session" || string(start.Config) != "opaque" || start.ResumeFromSeq != 17 {
						t.Errorf("start=%+v", start)
					}
					result, err := sink.ToolCall(ctx, gatedCall())
					if err != nil {
						return err
					}
					served = append(served, result)
					return sink.Output(ctx, "continued")
				})
				if resumed, err := c.Resume(t.Context(), h); !resumed || err != nil {
					t.Fatalf("decided Resume=%t,%v", resumed, err)
				}
				receipt := approvalRecord(t, log, api.EventToolResult)
				wantCode := api.ToolResultCodeUnspecified
				if outcome == "denied" {
					wantCode = api.ToolResultCodeApprovalDenied
				} else if outcome != "approved" {
					wantCode = api.ToolResultCodeExecutorError
				}
				if receipt.Event.Result.ID != "c1" || receipt.Event.Result.ApprovalRequestSeq != request.Seq || receipt.Event.Result.ApprovalDecisionSeq != decision.Seq || receipt.Event.Result.Code != wantCode || receipt.Event.Result.IsError != (outcome != "approved") || receipt.Event.Result.Approval != nil {
					t.Fatalf("receipt=%+v", receipt.Event.Result)
				}
				wantCalls := 1
				if outcome == "denied" {
					wantCalls = 0
					if len(receipt.Event.Result.Content) == 0 {
						t.Fatal("denial has no model-facing content")
					}
				}
				if calls != wantCalls {
					t.Fatalf("executor calls=%d", calls)
				}
				head, _ := log.Head()
				if _, err := c.Replay(t.Context(), h); err != nil {
					t.Fatalf("completed Replay=%v", err)
				}
				if calls != wantCalls || len(served) != 2 || !reflect.DeepEqual(served[0], served[1]) || served[0].Approval == nil || !reflect.DeepEqual(*served[0].Approval, *decision.Event.ApprovalResult) {
					t.Fatalf("served=%+v calls=%d", served, calls)
				}
				if approvalRecord(t, log, api.EventToolResult).Event.Result.Approval != nil {
					t.Fatal("replay decorated journal receipt")
				}
				served[0].Approval.Reason = "caller mutation"
				served[1].Approval.Approved = !served[1].Approval.Approved
				if got := approvalRecord(t, log, api.EventApprovalResult).Event.ApprovalResult; got.Reason != "decision reason" || got.Approved != (outcome != "denied") {
					t.Fatalf("reply mutated decision: %+v", got)
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
				after, _ := log.Head()
				if after != head {
					t.Fatal("replay appended")
				}
				// Receipts, not decisions, unlock new input; direct inputless turns remain supported.
				if err := c.Exec(t.Context(), gateHarness(func(context.Context, *api.Start, api.EventSink) error { return nil }), nil, head); err != nil {
					t.Fatalf("receipt admission=%v", err)
				}
			})
		}
	}
}

type failGateAppend struct {
	eventlog.Store
	kind  api.EventKind
	cause error
}

func (s failGateAppend) Append(head, fence int64, ev api.Event) (eventlog.Record, error) {
	if ev.Kind == s.kind {
		return eventlog.Record{}, s.cause
	}
	return s.Store.Append(head, fence, ev)
}

func TestApprovalCrashGapFailuresStopHandledContinuation(t *testing.T) {
	for _, scenario := range []string{"request append", "receipt append", "missing executor"} {
		t.Run(scenario, func(t *testing.T) {
			log := approvalLog(t, "sqlite")
			cause := errors.New("disk unavailable")
			store := eventlog.Store(log)
			if scenario == "request append" {
				store = failGateAppend{log, api.EventApprovalRequest, cause}
			} else {
				approvalSeed(t, log, true, api.EventApprovalResult)
				if scenario == "receipt append" {
					store = failGateAppend{log, api.EventToolResult, cause}
				} else {
					cause = controller.ErrMissingToolExecutor
				}
			}
			opts := []controller.Option{controller.WithSessionUID("session")}
			if scenario != "missing executor" {
				opts = append(opts, controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					return api.ToolResult{}, nil
				}))
			}
			c, err := controller.New(store, nil, opts...)
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				_, err := sink.ToolCall(ctx, gatedCall())
				if !errors.Is(err, cause) {
					t.Errorf("first cause=%v", err)
				}
				for _, got := range gateSinkCalls(ctx, sink) {
					if !errors.Is(got, cause) || errors.Is(got, controller.ErrReplayDiverged) {
						t.Errorf("handled continuation cause=%v", got)
					}
				}
				return nil
			})
			if scenario == "request append" {
				err = c.Exec(t.Context(), h, nil, 0)
			} else {
				_, err = c.Resume(t.Context(), h)
			}
			if !errors.Is(err, cause) || errors.Is(err, controller.ErrReplayDiverged) {
				t.Fatalf("Run cause=%v", err)
			}
			assertNoGateContinuation(t, log)
			state, err := controller.InspectApproval(log, 0)
			if err != nil || state == nil || state.Receipt != nil {
				t.Fatalf("recoverability state=%+v err=%v", state, err)
			}
		})
	}
}

func TestApprovalValidationBeforeIntent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*api.ToolCall)
		uid    string
		memory bool
		want   error
	}{
		{"memory", nil, "session", true, controller.ErrApprovalUnavailable},
		{"empty session", nil, "", false, controller.ErrMissingSessionUID},
		{"empty id", func(c *api.ToolCall) { c.ID = "" }, "session", false, controller.ErrInvalidApprovalCall},
		{"empty key", func(c *api.ToolCall) { c.IdempotencyKey = "" }, "session", false, controller.ErrMissingIdempotencyKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := approvalLog(t, "memory")
			c, err := controller.New(log, nil, controller.WithSessionUID(tc.uid))
			if err != nil {
				t.Fatal(err)
			}
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				call := gatedCall()
				if tc.change != nil {
					tc.change(&call)
				}
				_, err := sink.ToolCall(ctx, call)
				return err
			})
			if tc.memory {
				h.desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
			}
			if err := c.Exec(t.Context(), h, nil, 0); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			records, _ := log.Read(1)
			for _, rec := range records {
				if rec.Event.Kind == api.EventToolCall || rec.Event.Kind == api.EventApprovalRequest {
					t.Fatal("invalid gate wrote intent")
				}
			}
		})
	}
}

func TestApprovalRejectsHostReceiptReports(t *testing.T) {
	for _, result := range []api.ToolResult{{Approval: &api.ApprovalResult{}}, {ApprovalRequestSeq: 3}, {ApprovalDecisionSeq: 4}, {Code: api.ToolResultCodeApprovalDenied}} {
		t.Run(fmt.Sprintf("%+v", result), func(t *testing.T) {
			log := approvalLog(t, "memory")
			c, _ := controller.New(log, nil)
			h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error { return sink.Report(ctx, result) })
			if err := c.Exec(t.Context(), h, nil, 0); !errors.Is(err, controller.ErrApprovalReceiptReport) {
				t.Fatalf("forged Report=%v", err)
			}
			records, _ := log.Read(1)
			for _, rec := range records {
				if rec.Event.Kind == api.EventToolResult {
					t.Fatal("forged receipt committed")
				}
			}
		})
	}
}
