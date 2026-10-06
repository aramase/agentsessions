package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

var errToolInterrupted = errors.New("test: interrupted turn")

type callHarness struct {
	calls     []api.ToolCall
	interrupt bool
	results   []api.ToolResult
}

func (*callHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "tools", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h *callHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	for _, call := range h.calls {
		result, err := sink.ToolCall(ctx, call)
		if err != nil {
			return err
		}
		h.results = append(h.results, result)
	}
	if h.interrupt {
		return errToolInterrupted
	}
	return nil
}

func recordedToolCall() api.ToolCall {
	return api.ToolCall{
		ID: "call-1", Tool: "read", Mediation: api.MediationControllerMediated, IdempotencyKey: "key-1",
		Args: map[string]any{"count": 2, "nested": map[string]any{"resource": "fixture"}, "order": []any{1, 2}},
	}
}

// Removing any identity field from the comparison would let an unrelated call receive a receipt
// (or re-drive a recorded effect). Run all three paths through the public controller.
func TestToolCallDivergenceFailsBeforeResultsOrEffects(t *testing.T) {
	changes := []struct {
		name   string
		change func(*api.ToolCall)
	}{
		{"id", func(c *api.ToolCall) { c.ID = "different" }},
		{"name", func(c *api.ToolCall) { c.Tool = "submit" }},
		{"args", func(c *api.ToolCall) { c.Args["count"] = 3 }},
		{"nested args", func(c *api.ToolCall) { c.Args["nested"] = map[string]any{"resource": "other"} }},
		{"list order", func(c *api.ToolCall) { c.Args["order"] = []any{2, 1} }},
		{"key", func(c *api.ToolCall) { c.IdempotencyKey = "different" }},
		{"mediation", func(c *api.ToolCall) { c.Mediation = api.MediationRequiresApproval }},
	}
	for _, path := range []string{"replay", "resume result", "resume intent"} {
		for _, change := range changes {
			t.Run(path+"/"+change.name, func(t *testing.T) {
				log := memStore(t)
				c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					if path == "resume intent" {
						return api.ToolResult{}, errToolInterrupted
					}
					return api.ToolResult{Output: map[string]any{"receipt": "receipt-1"}}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{recordedToolCall()}, interrupt: path != "replay"}, []api.Message{msg("read")}, 0)
				if path == "replay" && err != nil || path != "replay" && !errors.Is(err, errToolInterrupted) {
					t.Fatalf("fixture execution: %v", err)
				}
				changed := recordedToolCall()
				change.change(&changed)
				h := &callHarness{calls: []api.ToolCall{changed}}
				attempts := 0
				fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					attempts++
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				if path == "replay" {
					_, err = fresh.Replay(t.Context(), h)
				} else {
					_, err = fresh.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) {
					t.Fatalf("want divergence, got %v", err)
				}
				if attempts != 0 || len(h.results) != 0 || fresh.ModelInvocations() != 0 {
					t.Fatalf("divergence served results/effects: attempts=%d results=%d", attempts, len(h.results))
				}
			})
		}
	}
}

func TestToolCallNormalizedIdentityServesRecordedResult(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay", true: "resume"}[recovery], func(t *testing.T) {
			log := memStore(t)
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				return api.ToolResult{ID: "executor-cannot-pick-id", Output: map[string]any{"receipt": "receipt-1"}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{recordedToolCall()}, interrupt: recovery}, []api.Message{msg("read")}, 0)
			if recovery && !errors.Is(err, errToolInterrupted) || !recovery && err != nil {
				t.Fatal(err)
			}
			fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				t.Error("recorded result invoked executor")
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			call := recordedToolCall()
			call.Args = map[string]any{"order": []any{float64(1), json.Number("2.0")}, "nested": map[string]any{"resource": "fixture"}, "count": int64(2)}
			h := &callHarness{calls: []api.ToolCall{call}}
			if recovery {
				_, err = fresh.Resume(t.Context(), h)
			} else {
				_, err = fresh.Replay(t.Context(), h)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(h.results) != 1 || h.results[0].ID != "call-1" || h.results[0].Output["receipt"] != "receipt-1" {
				t.Fatalf("wrong recorded receipt: %+v", h.results)
			}
			if fresh.ModelInvocations() != 0 || fresh.ToolInvocations() != 0 {
				t.Fatal("recorded result invoked live effects")
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Hand-built evidence fixtures isolate malformed prefixes; no corruption of the hash chain is
// needed. The controller must reject them even when the harness repeats the same malformed call.
func TestMalformedToolEvidenceFailsClosed(t *testing.T) {
	base := recordedToolCall()
	result := api.ToolResult{ID: "call-1", Output: map[string]any{"receipt": "receipt-1"}}
	cases := []struct {
		name   string
		events []api.Event
		call   api.ToolCall
		replay bool
	}{
		{"missing call", []api.Event{{Kind: api.EventToolCall}, {Kind: api.EventToolResult, Result: &result}}, base, true},
		{"missing result", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventToolResult}}, base, true},
		{"wrong result id", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventToolResult, Result: &api.ToolResult{ID: "wrong"}}}, base, true},
		{"no recorded result", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}}, base, true},
		{"nonterminal intent output", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventOutput, Message: api.TextMessage("assistant", "unexpected")}}, base, false},
		{"nonterminal intent call", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventToolCall, ToolCall: &base}}, base, false},
		{"nonterminal intent usage", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventUsage, Usage: &api.Usage{}}}, base, false},
		{"nonterminal intent model", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventModelCall, ModelCall: &api.ModelCall{ID: "model-1"}}}, base, false},
	}
	for _, test := range cases {
		for _, recovery := range []bool{false, true} {
			if !recovery && !test.replay {
				continue
			}
			t.Run(test.name+map[bool]string{false: "/replay", true: "/resume"}[recovery], func(t *testing.T) {
				log := memStore(t)
				appendToolEvidence(t, log, test.events, !recovery)
				attempts := 0
				c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					attempts++
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				h := &callHarness{calls: []api.ToolCall{test.call}}
				if recovery {
					_, err = c.Resume(t.Context(), h)
				} else {
					_, err = c.Replay(t.Context(), h)
				}
				// A terminal intent without result is the one valid recovery window.
				if recovery && test.name == "no recorded result" {
					if err != nil || attempts != 1 {
						t.Fatalf("terminal recovery: attempts=%d err=%v", attempts, err)
					}
					return
				}
				if err == nil || attempts != 0 || len(h.results) != 0 {
					t.Fatalf("malformed evidence accepted: attempts=%d results=%d err=%v", attempts, len(h.results), err)
				}
			})
		}
	}
}

func appendToolEvidence(t *testing.T, log eventlog.Store, effects []api.Event, completed bool) {
	t.Helper()
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	events := append([]api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "read")}}, effects...)
	if completed {
		events = append(events, api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	}
	var seq int64
	for _, ev := range events {
		ev.ExecutionID = "recorded-execution"
		record, err := log.Append(seq, fence, ev)
		if err != nil {
			t.Fatal(err)
		}
		seq = record.Seq
	}
}

func TestRecoveryRejectsInvalidRecordedToolIntent(t *testing.T) {
	for _, change := range []struct {
		name   string
		change func(*api.ToolCall)
	}{
		{"empty key", func(c *api.ToolCall) { c.IdempotencyKey = "" }},
		{"unmediated", func(c *api.ToolCall) { c.Mediation = api.MediationInHarnessReported }},
		{"approval", func(c *api.ToolCall) { c.Mediation = api.MediationRequiresApproval }},
	} {
		t.Run(change.name, func(t *testing.T) {
			log := memStore(t)
			call := recordedToolCall()
			change.change(&call)
			appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}}, false)
			attempts := 0
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Resume(t.Context(), &callHarness{calls: []api.ToolCall{call}})
			if err == nil || attempts != 0 {
				t.Fatalf("invalid intent executed: attempts=%d err=%v", attempts, err)
			}
		})
	}
}

type handledToolErrorHarness struct{ call api.ToolCall }

func (handledToolErrorHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "handles-errors"}, nil
}

func (h handledToolErrorHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	// A harness may handle a tool error, but cannot turn a rejected recorded prefix into a new
	// live turn or a verified replay by swallowing the controller's fidelity error.
	_, _ = sink.ToolCall(ctx, h.call)
	fresh := recordedToolCall()
	fresh.ID, fresh.IdempotencyKey = "new-call", "new-key"
	_, _ = sink.ToolCall(ctx, fresh)
	_, _ = sink.Model(ctx, api.ModelRequest{Model: "test"})
	_ = sink.Output(ctx, "masked")
	_ = sink.Report(ctx, api.ToolResult{ID: "reported"})
	_ = sink.Usage(ctx, api.Usage{})
	return nil
}

func TestHandledToolDivergenceCannotActivateLiveEffectsOrVerify(t *testing.T) {
	for _, test := range []struct {
		name              string
		badKey, badResult bool
	}{
		{name: "changed call"}, {name: "invalid terminal key", badKey: true}, {name: "missing result payload", badResult: true},
	} {
		for _, replay := range []bool{false, true} {
			if replay && test.badKey {
				continue
			}
			t.Run(test.name+map[bool]string{false: "/resume", true: "/replay"}[replay], func(t *testing.T) {
				log := memStore(t)
				call := recordedToolCall()
				if test.badKey {
					call.IdempotencyKey = ""
				}
				events := []api.Event{{Kind: api.EventToolCall, ToolCall: &call}}
				if test.badResult {
					events = append(events, api.Event{Kind: api.EventToolResult})
				} else if replay {
					events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}})
				}
				appendToolEvidence(t, log, events, replay)
				if !test.badKey && !test.badResult {
					call.ID = "different"
				}
				tools, models := 0, 0
				c, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
					models++
					return api.ModelResponse{Message: *api.TextMessage("assistant", "unexpected")}, nil
				}, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { tools++; return api.ToolResult{}, nil }))
				if err != nil {
					t.Fatal(err)
				}
				before, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				if replay {
					_, err = c.Replay(t.Context(), handledToolErrorHarness{call: call})
				} else {
					_, err = c.Resume(t.Context(), handledToolErrorHarness{call: call})
				}
				if err == nil || tools != 0 || models != 0 {
					t.Fatalf("handled divergence escaped: tools=%d models=%d err=%v", tools, models, err)
				}
				recs, err := log.Read(before + 1)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range recs {
					if rec.Event.Kind != api.EventError {
						t.Fatalf("handled divergence appended %s", rec.Event.Kind)
					}
				}
			})
		}
	}
}

func TestRecoveryRejectsUnconsumedToolEffects(t *testing.T) {
	log := memStore(t)
	call := recordedToolCall()
	result := api.ToolResult{ID: call.ID}
	appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}, {Kind: api.EventToolResult, Result: &result}}, false)
	c, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Resume(t.Context(), &callHarness{}); !errors.Is(err, controller.ErrReplayDiverged) {
		t.Fatalf("want underconsumption divergence, got %v", err)
	}
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range recs {
		if rec.Event.Kind == api.EventEnd {
			t.Fatal("recovery certified an unconsumed prefix")
		}
	}
}

func TestRecoveryExecutesOnlyNewCallsAfterRecordedPrefix(t *testing.T) {
	log := memStore(t)
	call := recordedToolCall()
	result := api.ToolResult{ID: call.ID, Output: map[string]any{"receipt": "recorded"}}
	appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}, {Kind: api.EventToolResult, Result: &result}}, false)
	newCall := recordedToolCall()
	newCall.ID = "call-2"
	newCall.IdempotencyKey = "key-2"
	attempts := 0
	c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(_ context.Context, got api.ToolCall) (api.ToolResult, error) {
		attempts++
		if got.ID != "call-2" || got.IdempotencyKey != "key-2" {
			t.Fatalf("wrong live call: %+v", got)
		}
		return api.ToolResult{Output: map[string]any{"receipt": "new"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	h := &callHarness{calls: []api.ToolCall{call, newCall}}
	if _, err = c.Resume(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || len(h.results) != 2 || h.results[0].Output["receipt"] != "recorded" || h.results[1].ID != "call-2" {
		t.Fatalf("wrong continuation: attempts=%d results=%+v", attempts, h.results)
	}
}

func TestToolArgumentPresenceRemainsDistinct(t *testing.T) {
	for _, test := range []struct {
		name              string
		recorded, emitted map[string]any
	}{
		{"absent vs empty", nil, map[string]any{}},
		{"missing vs null", map[string]any{}, map[string]any{"x": nil}},
		{"object vs list", map[string]any{"x": map[string]any{}}, map[string]any{"x": []any{}}},
		{"invalid cannot equal absent", nil, map[string]any{"x": make(chan int)}},
	} {
		for _, path := range []string{"replay", "resume result", "resume intent"} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				log := memStore(t)
				call := recordedToolCall()
				call.Args = test.recorded
				events := []api.Event{{Kind: api.EventToolCall, ToolCall: &call}}
				if path != "resume intent" {
					events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}})
				}
				appendToolEvidence(t, log, events, path == "replay")
				attempts := 0
				c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
				if err != nil {
					t.Fatal(err)
				}
				call.Args = test.emitted
				h := &callHarness{calls: []api.ToolCall{call}}
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 || len(h.results) != 0 {
					t.Fatalf("different/invalid args accepted: attempts=%d results=%d err=%v", attempts, len(h.results), err)
				}
			})
		}
	}
}

type legacyToolNumber int

func TestToolReplayPreservesLegacyFallbackRequestIdentity(t *testing.T) {
	shapes := []map[string]any{
		{"count": 2, "optional": map[string]any(nil)},
		{"count": legacyToolNumber(2), "optional": map[string]any(nil)},
	}
	for shapeIndex, args := range shapes {
		for _, path := range []string{"replay", "resume result", "resume intent"} {
			for _, crossShape := range []bool{false, true} {
				t.Run(fmt.Sprintf("shape=%d/%s/cross=%v", shapeIndex, path, crossShape), func(t *testing.T) {
					log := memStore(t)
					call := recordedToolCall()
					call.Args = args
					// Append directly, without the new live normalizer, to model a legacy journal.
					events := []api.Event{{Kind: api.EventToolCall, ToolCall: &call}}
					if path != "resume intent" {
						events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}})
					}
					appendToolEvidence(t, log, events, path == "replay")
					attempts := 0
					c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
					if err != nil {
						t.Fatal(err)
					}
					if crossShape {
						call.Args = shapes[1-shapeIndex]
					}
					h := &callHarness{calls: []api.ToolCall{call}}
					if path == "replay" {
						_, err = c.Replay(t.Context(), h)
					} else {
						_, err = c.Resume(t.Context(), h)
					}
					if crossShape {
						if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 || len(h.results) != 0 {
							t.Fatalf("different legacy wire args accepted: attempts=%d results=%d err=%v", attempts, len(h.results), err)
						}
					} else {
						wantAttempts := 0
						if path == "resume intent" {
							wantAttempts = 1
						}
						if err != nil || attempts != wantAttempts || len(h.results) != 1 {
							t.Fatalf("original legacy request rejected: attempts=%d results=%d err=%v", attempts, len(h.results), err)
						}
					}
				})
			}
		}
	}
}

func TestToolReplayPreservesExistingEmptyCallIDs(t *testing.T) {
	log := memStore(t)
	call := recordedToolCall()
	call.ID = ""
	c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
		return api.ToolResult{Output: map[string]any{"receipt": "recorded"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{call}}, []api.Message{msg("read")}, 0); err != nil {
		t.Fatal(err)
	}
	fresh, err := controller.New(log, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	h := &callHarness{calls: []api.ToolCall{call}}
	if _, err = fresh.Replay(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if len(h.results) != 1 || h.results[0].ID != "" || h.results[0].Output["receipt"] != "recorded" {
		t.Fatalf("legacy empty ID did not replay: %+v", h.results)
	}
}

// Direct Go harnesses must execute the same Struct-domain values that are journaled, rather than
// giving an executor raw json.Number/typed-container values it would not receive after recovery.
func TestLiveToolExecutorReceivesRecordedArgumentRepresentation(t *testing.T) {
	log := memStore(t)
	call := recordedToolCall()
	call.Args = map[string]any{"number": json.Number("2.00000000000000000001"), "items": []int{1, 2}}
	c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(_ context.Context, got api.ToolCall) (api.ToolResult, error) {
		if number, ok := got.Args["number"].(float64); !ok || number != 2 {
			return api.ToolResult{}, errors.New("executor received unnormalized number")
		}
		if items, ok := got.Args["items"].([]any); !ok || len(items) != 2 || items[0] != float64(1) || items[1] != float64(2) {
			return api.ToolResult{}, errors.New("executor received unnormalized list")
		}
		recs, err := log.Read(1)
		if err != nil {
			return api.ToolResult{}, err
		}
		if len(recs) == 0 {
			return api.ToolResult{}, errors.New("executor invoked before durable intent")
		}
		intent := recs[len(recs)-1].Event
		if intent.Kind != api.EventToolCall || intent.ToolCall == nil || intent.ToolCall.Args["number"] != got.Args["number"] {
			return api.ToolResult{}, errors.New("executor args differ from durable intent")
		}
		return api.ToolResult{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{call}}, []api.Message{msg("read")}, 0); err != nil {
		t.Fatal(err)
	}
}

func TestLiveToolCallRejectsInvalidArgumentsBeforeIntent(t *testing.T) {
	for _, test := range []struct {
		name string
		args map[string]any
	}{
		{"channel", map[string]any{"secret": make(chan int)}},
		{"nonfinite", map[string]any{"secret": math.NaN()}},
		{"unsafe integer", map[string]any{"secret": int64(9007199254740993)}},
		{"struct", map[string]any{"secret": struct{ X string }{"private"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := memStore(t)
			attempts := 0
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
			if err != nil {
				t.Fatal(err)
			}
			call := recordedToolCall()
			call.Args = test.args
			err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{call}}, []api.Message{msg("read")}, 0)
			if err == nil || attempts != 0 {
				t.Fatalf("invalid args executed: attempts=%d err=%v", attempts, err)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
				t.Fatalf("error leaked argument content: %v", err)
			}
			recs, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range recs {
				if rec.Event.Kind == api.EventToolCall || rec.Event.Kind == api.EventToolResult {
					t.Fatal("invalid args produced a tool intent/result")
				}
			}
		})
	}
}
