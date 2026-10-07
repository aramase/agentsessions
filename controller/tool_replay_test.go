package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
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
		{"no recorded call", []api.Event{{Kind: api.EventToolResult, Result: &result}}, base, true},
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
				if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 || len(h.results) != 0 {
					t.Fatalf("want malformed-evidence divergence: attempts=%d results=%d err=%v", attempts, len(h.results), err)
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
		cause  error
	}{
		{"empty key", func(c *api.ToolCall) { c.IdempotencyKey = "" }, controller.ErrMissingIdempotencyKey},
		{"unmediated", func(c *api.ToolCall) { c.Mediation = api.MediationInHarnessReported }, controller.ErrUnmediatedToolCall},
		{"unspecified", func(c *api.ToolCall) { c.Mediation = "" }, controller.ErrUnmediatedToolCall},
		// Approval has no live sentinel on main. Invalid recorded approval evidence is classified
		// as divergence, without inventing an error-string match or a new public approval API.
		{"approval", func(c *api.ToolCall) { c.Mediation = api.MediationRequiresApproval }, controller.ErrReplayDiverged},
	} {
		for _, path := range []string{"replay", "resume result", "resume intent"} {
			t.Run(path+"/"+change.name, func(t *testing.T) {
				log := memStore(t)
				call := recordedToolCall()
				change.change(&call)
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
				h := &callHarness{calls: []api.ToolCall{call}}
				if path == "replay" {
					_, err = c.Replay(t.Context(), h)
				} else {
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) || !errors.Is(err, change.cause) || attempts != 0 || len(h.results) != 0 {
					t.Fatalf("want divergence and %v before results/effects: attempts=%d results=%d err=%v", change.cause, attempts, len(h.results), err)
				}
			})
		}
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
				if !errors.Is(err, controller.ErrReplayDiverged) || tools != 0 || models != 0 {
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
					t.Fatalf("different args accepted: attempts=%d results=%d err=%v", attempts, len(h.results), err)
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
					// The existing whole-map fallback records native nil containers as empty, but
					// typed siblings can turn them into null. Keep that legacy wire identity.
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

// Comparison must not normalize the arguments a direct Go executor receives on main's live
// path, including new calls after a recorded prefix. Removing this distinction changes ToolFunc.
func TestLiveToolExecutorReceivesOriginalArguments(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "exec", true: "resume live tail"}[recovery], func(t *testing.T) {
			log := memStore(t)
			call := recordedToolCall()
			if recovery {
				appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}, {Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}}}, false)
			}
			live := recordedToolCall()
			live.ID, live.IdempotencyKey = "call-2", "key-2"
			live.Args = map[string]any{"number": json.Number("2.00000000000000000001"), "items": []int{1, 2}}
			attempts := 0
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(_ context.Context, got api.ToolCall) (api.ToolResult, error) {
				attempts++
				want := map[string]any{"number": json.Number("2.00000000000000000001"), "items": []int{1, 2}}
				if !reflect.DeepEqual(got.Args, want) {
					t.Fatalf("executor arguments changed: got %#v want %#v", got.Args, want)
				}
				recs, err := log.Read(1)
				if err != nil {
					return api.ToolResult{}, err
				}
				intent := recs[len(recs)-1].Event
				if intent.Kind != api.EventToolCall || intent.ToolCall == nil || intent.ToolCall.ID != "call-2" {
					t.Fatal("executor invoked before durable intent")
				}
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if recovery {
				_, err = c.Resume(t.Context(), &callHarness{calls: []api.ToolCall{call, live}})
			} else {
				err = c.Exec(t.Context(), &callHarness{calls: []api.ToolCall{live}}, []api.Message{msg("read")}, 0)
			}
			if err != nil || attempts != 1 {
				t.Fatalf("live execution: attempts=%d err=%v", attempts, err)
			}
		})
	}
}

func TestForkCutAtToolCallChecksIdentityBeforeRedrive(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*api.ToolCall)
	}{
		{"same identity", func(*api.ToolCall) {}},
		{"different id", func(c *api.ToolCall) { c.ID = "changed" }},
		{"different name", func(c *api.ToolCall) { c.Tool = "write" }},
		{"different args", func(c *api.ToolCall) { c.Args["count"] = 3 }},
		{"different mediation", func(c *api.ToolCall) { c.Mediation = api.MediationRequiresApproval }},
		{"different key", func(c *api.ToolCall) { c.IdempotencyKey = "changed" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			parent, child := store.Session("parent"), store.Session("child")
			original, err := controller.New(parent, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				return api.ToolResult{Output: map[string]any{"receipt": "parent"}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if err := original.Exec(t.Context(), &callHarness{calls: []api.ToolCall{recordedToolCall()}}, []api.Message{msg("read")}, 0); err != nil {
				t.Fatal(err)
			}
			before, err := parent.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			var atSeq int64
			for _, rec := range before {
				if rec.Event.Kind == api.EventToolCall {
					atSeq = rec.Seq
				}
			}
			if atSeq == 0 {
				t.Fatal("parent has no tool intent")
			}
			if err := controller.Fork(parent, child, atSeq); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			fresh, err := controller.New(child, echoModel, controller.WithSessionUID("child"), controller.WithToolExecutor(func(_ context.Context, got api.ToolCall) (api.ToolResult, error) {
				attempts++
				if got.ID != "call-1" || got.Tool != "read" || got.Mediation != api.MediationControllerMediated || got.IdempotencyKey != "key-1" || got.Args["count"] != float64(2) {
					t.Fatalf("child re-drove wrong recorded call: %+v", got)
				}
				return api.ToolResult{Output: map[string]any{"receipt": "child"}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			call := recordedToolCall()
			test.change(&call)
			h := &callHarness{calls: []api.ToolCall{call}}
			resumed, err := fresh.Resume(t.Context(), h)
			if !resumed {
				t.Fatal("fork cut was not resumed")
			}
			if test.name == "same identity" {
				if err != nil || attempts != 1 || len(h.results) != 1 || h.results[0].ID != "call-1" || h.results[0].Output["receipt"] != "child" {
					t.Fatalf("child recovery: attempts=%d results=%+v err=%v", attempts, h.results, err)
				}
			} else if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 || len(h.results) != 0 {
				t.Fatalf("fork divergence served results/effects: attempts=%d results=%d err=%v", attempts, len(h.results), err)
			}
			after, err := parent.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("child recovery changed parent journal")
			}
			recs, err := child.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			wantTail := []api.EventKind{api.EventLifecycle, api.EventError}
			if test.name == "same identity" {
				wantTail = []api.EventKind{api.EventLifecycle, api.EventToolResult, api.EventEnd}
			}
			if len(recs) != int(atSeq)+len(wantTail) {
				t.Fatalf("child journal has %d records, want prefix plus %v", len(recs), wantTail)
			}
			if recs[atSeq-1].Hash != before[atSeq-1].Hash {
				t.Fatal("fork changed shared prefix hash")
			}
			for i, kind := range wantTail {
				if recs[int(atSeq)+i].Event.Kind != kind {
					t.Fatalf("child tail event %d = %s, want %s", i, recs[int(atSeq)+i].Event.Kind, kind)
				}
			}
			for _, rec := range recs {
				if rec.Seq <= atSeq || rec.Event.Kind == api.EventLifecycle {
					continue
				}
				if test.name != "same identity" && rec.Event.Kind != api.EventError {
					t.Fatalf("mismatched child appended %s", rec.Event.Kind)
				}
				if rec.Event.Kind == api.EventToolResult && (rec.Event.Result.ID != "call-1" || rec.Event.Result.Output["receipt"] != "child") {
					t.Fatal("child did not journal its own correlated result")
				}
			}
			if err := child.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type continuesAfterToolFailureHarness struct{ call api.ToolCall }

func (continuesAfterToolFailureHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (&callHarness{}).Describe(ctx)
}

func (h continuesAfterToolFailureHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, _ = sink.ToolCall(ctx, h.call)
	if err := sink.Output(ctx, "handled tool failure"); err != nil {
		return err
	}
	return errToolInterrupted
}

// Main does not journal executor failures as TOOL_RESULT. A harness can handle that failure and
// append another effect before interruption. Recovery cannot safely correlate or re-drive it.
func TestRecoveryFailsClosedAfterHandledExecutorFailure(t *testing.T) {
	log := memStore(t)
	h := continuesAfterToolFailureHarness{call: recordedToolCall()}
	original, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
		return api.ToolResult{}, errors.New("executor failed")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Exec(t.Context(), h, []api.Message{msg("read")}, 0); !errors.Is(err, errToolInterrupted) {
		t.Fatalf("fixture: %v", err)
	}
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var intent, output int64
	for _, rec := range recs {
		switch rec.Event.Kind {
		case api.EventToolCall:
			intent = rec.Seq
		case api.EventOutput:
			output = rec.Seq
		case api.EventToolResult:
			t.Fatal("fixture unexpectedly journaled an executor failure receipt")
		}
	}
	if intent == 0 || output != intent+1 {
		t.Fatal("fixture lacks an intent followed by a handled-failure output")
	}
	attempts := 0
	fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Resume(t.Context(), h); !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 {
		t.Fatalf("uncertain intent re-driven: attempts=%d err=%v", attempts, err)
	}
}

func TestRecoveryLiveTailToolErrorsAreNotDivergence(t *testing.T) {
	executorErr := errors.New("executor failed")
	for _, test := range []struct {
		name     string
		change   func(*api.ToolCall)
		want     error
		attempts int
	}{
		{"executor", func(*api.ToolCall) {}, executorErr, 1},
		{"key", func(c *api.ToolCall) { c.IdempotencyKey = "" }, controller.ErrMissingIdempotencyKey, 0},
		{"mediation", func(c *api.ToolCall) { c.Mediation = api.MediationInHarnessReported }, controller.ErrUnmediatedToolCall, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := memStore(t)
			call := recordedToolCall()
			appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}, {Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}}}, false)
			live := recordedToolCall()
			live.ID, live.IdempotencyKey = "call-2", "key-2"
			test.change(&live)
			attempts := 0
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				attempts++
				return api.ToolResult{}, executorErr
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := &callHarness{calls: []api.ToolCall{call, live}}
			_, err = c.Resume(t.Context(), h)
			if !errors.Is(err, test.want) || errors.Is(err, controller.ErrReplayDiverged) || attempts != test.attempts || len(h.results) != 1 {
				t.Fatalf("live error classification: attempts=%d results=%d err=%v", attempts, len(h.results), err)
			}
		})
	}
}
