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
		{"in-harness mediation", func(c *api.ToolCall) { c.Mediation = api.MediationInHarnessReported }},
		{"unspecified mediation", func(c *api.ToolCall) { c.Mediation = "" }},
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
	}{
		{"no recorded call", []api.Event{{Kind: api.EventToolResult, Result: &result}}, base},
		{"missing call", []api.Event{{Kind: api.EventToolCall}, {Kind: api.EventToolResult, Result: &result}}, base},
		{"missing result payload", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventToolResult}}, base},
		{"wrong result id", []api.Event{{Kind: api.EventToolCall, ToolCall: &base}, {Kind: api.EventToolResult, Result: &api.ToolResult{ID: "wrong"}}}, base},
	}
	for _, test := range cases {
		for _, recovery := range []bool{false, true} {
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
				// A keyless emitted call is rejected before intent on the live path. Emit a valid
				// key here so recovery must inspect (and reject) the invalid recorded intent.
				if call.IdempotencyKey == "" {
					call.IdempotencyKey = "key-1"
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
		name                           string
		badKey, badResult, wrongResult bool
	}{
		{name: "changed call"}, {name: "invalid terminal key", badKey: true},
		{name: "missing result payload", badResult: true}, {name: "wrong result correlation", wrongResult: true},
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
				} else if test.wrongResult {
					events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: "different"}})
				} else if replay {
					events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: call.ID}})
				}
				appendToolEvidence(t, log, events, replay)
				if test.badKey {
					call.IdempotencyKey = "key-1"
				} else if !test.badResult && !test.wrongResult {
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
		{"unconvertible emitted args", nil, map[string]any{"unsupported": make(chan int)}},
		{"unconvertible recorded args", map[string]any{"unsupported": make(chan int)}, nil},
		{"both unconvertible", map[string]any{"unsupported": make(chan int)}, map[string]any{"unsupported": make(chan int)}},
		{"missing vs null", map[string]any{}, map[string]any{"x": nil}},
		{"object vs list", map[string]any{"x": map[string]any{}}, map[string]any{"x": []any{}}},
	} {
		for _, path := range []string{"replay", "resume result", "resume intent"} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				// The memory store preserves raw Go args; SQLite has already converted them.
				log := eventlog.AsStore(eventlog.New())
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
				emitted := call
				emitted.Args = test.emitted
				h := &callHarness{calls: []api.ToolCall{emitted}}
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

// This harness handles the first tool error, then emits further effects. Recovery must return an
// error without consuming those effects or stealing the later call's receipt.
type continuesAfterToolFailureHarness struct {
	calls     []api.ToolCall
	interrupt bool
	toolError error
	results   []api.ToolResult
}

func (*continuesAfterToolFailureHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (&callHarness{}).Describe(ctx)
}

func (h *continuesAfterToolFailureHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, h.toolError = sink.ToolCall(ctx, h.calls[0])
	for _, call := range h.calls[1:] {
		result, err := sink.ToolCall(ctx, call)
		if err != nil {
			return err
		}
		h.results = append(h.results, result)
	}
	if err := sink.Output(ctx, "handled tool failure"); err != nil {
		return err
	}
	if err := sink.Usage(ctx, api.Usage{InputTokens: 1}); err != nil {
		return err
	}
	if _, err := sink.Model(ctx, api.ModelRequest{Model: "test"}); err != nil {
		return err
	}
	if h.interrupt {
		return errToolInterrupted
	}
	return nil
}

func TestCompletedTurnReplaysHandledToolFailures(t *testing.T) {
	for _, keyless := range []bool{false, true} {
		for _, laterCall := range []bool{false, true} {
			t.Run(fmt.Sprintf("keyless=%v/later-call=%v", keyless, laterCall), func(t *testing.T) {
				log := memStore(t)
				failed := recordedToolCall()
				if keyless {
					failed.IdempotencyKey = ""
				}
				calls := []api.ToolCall{failed}
				if laterCall {
					next := recordedToolCall()
					next.ID, next.IdempotencyKey = "call-2", "key-2"
					calls = append(calls, next)
				}
				original, err := controller.New(log, echoModel, controller.WithToolExecutor(func(_ context.Context, call api.ToolCall) (api.ToolResult, error) {
					if call.ID == "call-1" {
						return api.ToolResult{}, errToolInterrupted
					}
					return api.ToolResult{Output: map[string]any{"receipt": "second"}}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				h := &continuesAfterToolFailureHarness{calls: calls}
				if err := original.Exec(t.Context(), h, []api.Message{msg("read")}, 0); err != nil {
					t.Fatal(err)
				}
				want := errToolInterrupted
				if keyless {
					want = controller.ErrMissingIdempotencyKey
				}
				if !errors.Is(h.toolError, want) {
					t.Fatalf("live tool error = %v, want %v", h.toolError, want)
				}
				before, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					t.Fatal("replay invoked executor")
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				replayed := &continuesAfterToolFailureHarness{calls: calls}
				outputs, err := fresh.Replay(t.Context(), replayed)
				if err != nil {
					t.Fatalf("handled tool error prevented replay: %v", err)
				}
				assertRecordedToolFailure(t, replayed.toolError, keyless)
				if !reflect.DeepEqual(outputs, []string{"handled tool failure", "echo:"}) || fresh.ModelInvocations() != 0 || fresh.ToolInvocations() != 0 {
					t.Fatalf("wrong replay outputs/effects: %v", outputs)
				}
				if laterCall && (len(replayed.results) != 1 || replayed.results[0].ID != "call-2" || replayed.results[0].Output["receipt"] != "second") {
					t.Fatalf("later receipt misassociated: %+v", replayed.results)
				}
				after, err := log.Read(1)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("replay changed journal: %v", err)
				}
			})
		}
	}
}

func assertRecordedToolFailure(t *testing.T, err error, keyless bool) {
	t.Helper()
	if keyless {
		if !errors.Is(err, controller.ErrMissingIdempotencyKey) {
			t.Fatalf("keyless error = %v", err)
		}
		return
	}
	// The legacy journal has no failure payload to recover. Return only a bounded generic
	// diagnostic, not fabricated success or a fidelity error that makes recovery impossible.
	if err == nil || errors.Is(err, controller.ErrReplayDiverged) || len(err.Error()) > 128 {
		t.Fatalf("want bounded non-divergence recorded failure, got %v", err)
	}
}

func TestRecoveryContinuesAfterHandledExecutorFailure(t *testing.T) {
	for _, keyless := range []bool{false, true} {
		for _, laterCall := range []bool{false, true} {
			t.Run(fmt.Sprintf("keyless=%v/later-call=%v", keyless, laterCall), func(t *testing.T) {
				log := memStore(t)
				failed := recordedToolCall()
				if keyless {
					failed.IdempotencyKey = ""
				}
				calls := []api.ToolCall{failed}
				if laterCall {
					next := recordedToolCall()
					next.ID, next.IdempotencyKey = "call-2", "key-2"
					calls = append(calls, next)
				}
				original, err := controller.New(log, echoModel, controller.WithToolExecutor(func(_ context.Context, call api.ToolCall) (api.ToolResult, error) {
					if call.ID == "call-1" {
						return api.ToolResult{}, errToolInterrupted
					}
					return api.ToolResult{Output: map[string]any{"receipt": "second"}}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				if err := original.Exec(t.Context(), &continuesAfterToolFailureHarness{calls: calls, interrupt: true}, []api.Message{msg("read")}, 0); !errors.Is(err, errToolInterrupted) {
					t.Fatalf("fixture: %v", err)
				}
				before, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					t.Fatal("handled failure repeated an effect")
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				h := &continuesAfterToolFailureHarness{calls: calls}
				if resumed, err := fresh.Resume(t.Context(), h); !resumed || err != nil {
					t.Fatalf("handled failure prevented completion: resumed=%v err=%v", resumed, err)
				}
				assertRecordedToolFailure(t, h.toolError, keyless)
				if laterCall && (len(h.results) != 1 || h.results[0].ID != "call-2" || h.results[0].Output["receipt"] != "second") {
					t.Fatalf("later receipt misassociated: %+v", h.results)
				}
				if fresh.ModelInvocations() != 0 || fresh.ToolInvocations() != 0 {
					t.Fatal("recorded prefix invoked live effects")
				}
				tail, err := log.Read(before + 1)
				if err != nil || len(tail) != 1 || tail[0].Event.Kind != api.EventEnd {
					t.Fatalf("resume should append only END: tail=%+v err=%v", tail, err)
				}
				if resumed, err := fresh.Resume(t.Context(), h); resumed || err != nil {
					t.Fatalf("completed turn resumed again: resumed=%v err=%v", resumed, err)
				}
			})
		}
	}
}

type handlesMediationRejectionHarness struct {
	call       api.ToolCall
	emitOutput bool
	interrupt  bool
	toolError  error
}

func (*handlesMediationRejectionHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (&callHarness{}).Describe(ctx)
}

func (h *handlesMediationRejectionHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, h.toolError = sink.ToolCall(ctx, h.call)
	if h.emitOutput {
		if err := sink.Output(ctx, "handled"); err != nil {
			return err
		}
	}
	if h.interrupt {
		return errToolInterrupted
	}
	return nil
}

// Latching a pre-intent mediation rejection or consuming its OUTPUT would prevent the same
// harness that handled the live rejection from successfully replaying or completing recovery.
func TestHandledMediationRejectionPreservesContinuation(t *testing.T) {
	for _, mediation := range []api.Mediation{
		api.MediationRequiresApproval, api.MediationInHarnessReported, "",
	} {
		for _, path := range []string{"replay output", "replay exhausted", "resume output", "resume live tail"} {
			t.Run(fmt.Sprintf("%s/%s", mediation, path), func(t *testing.T) {
				log := memStore(t)
				call := recordedToolCall()
				call.Mediation = mediation
				attempts := 0
				executor := func(context.Context, api.ToolCall) (api.ToolResult, error) {
					attempts++
					return api.ToolResult{}, errors.New("unexpected tool execution")
				}
				original, err := controller.New(log, echoModel, controller.WithToolExecutor(executor))
				if err != nil {
					t.Fatal(err)
				}
				recovery := path == "resume output" || path == "resume live tail"
				h := &handlesMediationRejectionHarness{
					call: call, emitOutput: path == "replay output" || path == "resume output", interrupt: recovery,
				}
				err = original.Exec(t.Context(), h, []api.Message{msg("read")}, 0)
				if recovery && !errors.Is(err, errToolInterrupted) || !recovery && err != nil {
					t.Fatalf("fixture execution: %v", err)
				}
				assertMediationRejection(t, h.toolError, mediation)
				before, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range before {
					if rec.Event.Kind == api.EventToolCall || rec.Event.Kind == api.EventToolResult {
						t.Fatalf("live rejection recorded a tool effect: %s", rec.Event.Kind)
					}
				}
				fresh, err := controller.New(log, echoModel, controller.WithToolExecutor(executor))
				if err != nil {
					t.Fatal(err)
				}
				h = &handlesMediationRejectionHarness{call: call, emitOutput: path != "replay exhausted"}
				if recovery {
					if resumed, err := fresh.Resume(t.Context(), h); !resumed || err != nil {
						t.Fatalf("handled mediation rejection prevented recovery: resumed=%v err=%v", resumed, err)
					}
				} else {
					outputs, err := fresh.Replay(t.Context(), h)
					if err != nil {
						t.Fatalf("handled mediation rejection prevented replay: %v", err)
					}
					if path == "replay output" && !reflect.DeepEqual(outputs, []string{"handled"}) || path == "replay exhausted" && len(outputs) != 0 {
						t.Fatalf("wrong replay output: %v", outputs)
					}
				}
				assertMediationRejection(t, h.toolError, mediation)
				if attempts != 0 || original.ModelInvocations() != 0 || fresh.ModelInvocations() != 0 {
					t.Fatalf("mediation rejection invoked live effects: tools=%d", attempts)
				}
				after, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				if !recovery {
					if !reflect.DeepEqual(before, after) {
						t.Fatal("replay changed journal")
					}
				} else {
					wantTail := []api.EventKind{api.EventEnd}
					if path == "resume live tail" {
						wantTail = []api.EventKind{api.EventOutput, api.EventEnd}
					}
					tail := after[len(before):]
					if len(tail) != len(wantTail) {
						t.Fatalf("resume appended unexpected events: %+v", tail)
					}
					for i, kind := range wantTail {
						if tail[i].Event.Kind != kind {
							t.Fatalf("resume event %d = %s, want %s", i, tail[i].Event.Kind, kind)
						}
					}
					outputs, err := fresh.Replay(t.Context(), &handlesMediationRejectionHarness{call: call, emitOutput: true})
					if err != nil || !reflect.DeepEqual(outputs, []string{"handled"}) {
						t.Fatalf("recovered turn did not replay: outputs=%v err=%v", outputs, err)
					}
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func assertMediationRejection(t *testing.T, err error, mediation api.Mediation) {
	t.Helper()
	if err == nil || errors.Is(err, controller.ErrReplayDiverged) {
		t.Fatalf("want handleable mediation rejection, got %v", err)
	}
	// Approval has no live sentinel; preserve that generic rejection without matching its text.
	if mediation != api.MediationRequiresApproval && !errors.Is(err, controller.ErrUnmediatedToolCall) {
		t.Fatalf("want unmediated tool rejection, got %v", err)
	}
}

func TestRecoveryHandlesTerminalToolRedriveErrors(t *testing.T) {
	// An executor may itself return ErrReplayDiverged: provenance, not errors.Is, determines
	// whether to latch. The executor error must remain handleable just like any live error.
	for _, executorErr := range []error{nil, errToolInterrupted, controller.ErrReplayDiverged} {
		t.Run(fmt.Sprint(executorErr), func(t *testing.T) {
			log := memStore(t)
			call := recordedToolCall()
			appendToolEvidence(t, log, []api.Event{{Kind: api.EventToolCall, ToolCall: &call}}, false)
			attempts := 0
			c, err := controller.New(log, echoModel, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				attempts++
				return api.ToolResult{}, executorErr
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := &continuesAfterToolFailureHarness{calls: []api.ToolCall{call}}
			if resumed, err := c.Resume(t.Context(), h); !resumed || err != nil {
				t.Fatalf("redrive prevented completion: resumed=%v err=%v", resumed, err)
			}
			if !errors.Is(h.toolError, executorErr) || attempts != 1 || c.ModelInvocations() != 1 {
				t.Fatalf("redrive error/effects: toolErr=%v attempts=%d models=%d", h.toolError, attempts, c.ModelInvocations())
			}
			// A failed re-drive followed by handled output is now nonterminal evidence. Replay
			// must not retry it, and must still correlate the recorded model/output suffix.
			h = &continuesAfterToolFailureHarness{calls: []api.ToolCall{call}}
			if _, err := c.Replay(t.Context(), h); err != nil || attempts != 1 || c.ModelInvocations() != 1 {
				t.Fatalf("redrive completion did not replay: attempts=%d err=%v", attempts, err)
			}
		})
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
