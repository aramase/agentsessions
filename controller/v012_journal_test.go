package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/sqlitelog"
)

func v012Records(t *testing.T, name string) []eventlog.Record {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "v0.1.2", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var records []eventlog.Record
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatal(err)
	}
	if err := eventlog.NewFrom(records).Verify(); err != nil {
		t.Fatalf("tagged fixture chain: %v", err)
	}
	return records
}

// Copy the actual old records into SQLite, checking every old hash, and reopen it before use.
func v012Log(t *testing.T, name string) (eventlog.Store, []eventlog.Record) {
	t.Helper()
	records := v012Records(t, name)
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	log := store.Session("legacy")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		for fence < record.Fence {
			fence, err = log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
		}
		got, err := log.Append(record.Seq-1, fence, record.Event)
		if err != nil || !reflect.DeepEqual(got, record) {
			t.Fatalf("copy old record %d: %#v, %v", record.Seq, got, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store.Session("legacy"), records
}

// Scope only these historical fixture controllers; general controller tests stay unscoped.
func v012Controller(log eventlog.Store, model controller.ModelFunc, opts ...controller.Option) (*controller.Controller, error) {
	return controller.New(log, model, append([]controller.Option{controller.WithSessionUID("v012-session")}, opts...)...)
}

type observedLegacyHarness struct{ starts *[]api.Start }

func (h observedLegacyHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (echoagent.Harness{}).Describe(ctx)
}
func (h observedLegacyHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	*h.starts = append(*h.starts, *start)
	return (echoagent.Harness{}).Run(ctx, start, sink)
}

func TestV012JournalReplayAndCompletedResume(t *testing.T) {
	log, original := v012Log(t, "completed")
	var starts []api.Start
	c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		t.Fatal("legacy replay/completed resume invoked model")
		return api.ModelResponse{}, nil
	}, controller.WithStart([]byte("not the recorded config"), 99))
	if err != nil {
		t.Fatal(err)
	}
	har := observedLegacyHarness{&starts}
	if resumed, err := c.Resume(t.Context(), har); resumed || err != nil {
		t.Fatalf("completed Resume = %v, %v", resumed, err)
	}
	out, err := c.Replay(t.Context(), har)
	if err != nil || !reflect.DeepEqual(out, []string{"echo:hello"}) {
		t.Fatalf("Replay = %v, %v", out, err)
	}
	if len(starts) != 1 {
		t.Fatalf("harness runs = %d", len(starts))
	}
	start := starts[0]
	var history []api.Event
	for _, record := range original {
		history = append(history, record.Event)
	}
	if !legacyExecutionIDPattern.MatchString(start.ExecutionID) || start.Config != nil || start.ResumeFromSeq != 0 || !reflect.DeepEqual(start.History, history) || !reflect.DeepEqual(messageTexts(start.Inputs), []string{"hello"}) {
		t.Fatalf("v0.1.2 whole-log Start = %#v", start)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(after, original) {
		t.Fatalf("read-only Replay changed journal: %v", err)
	}
}

func TestV012CrashedJournalUpgradeResumeThenExec(t *testing.T) {
	for _, name := range []string{"crashed-input", "crashed-output"} {
		t.Run(name, func(t *testing.T) {
			log, original := v012Log(t, name)
			modelCalls := 0
			model := func(ctx context.Context, request api.ModelRequest) (api.ModelResponse, error) {
				modelCalls++
				return echoagent.Model(ctx, request)
			}
			var starts []api.Start
			c, err := v012Controller(log, model, controller.WithStart([]byte("replacement"), 99))
			if err != nil {
				t.Fatal(err)
			}
			har := observedLegacyHarness{&starts}
			if resumed, err := c.Resume(t.Context(), har); !resumed || err != nil {
				t.Fatalf("upgraded Resume = %v, %v", resumed, err)
			}
			wantCalls := 0
			if name == "crashed-input" {
				wantCalls = 1
			}
			if modelCalls != wantCalls {
				t.Fatalf("recovery model calls = %d, want %d", modelCalls, wantCalls)
			}
			if len(starts) != 1 || !legacyExecutionIDPattern.MatchString(starts[0].ExecutionID) || len(starts[0].History) != 0 || starts[0].Config != nil || starts[0].ResumeFromSeq != 0 {
				t.Fatalf("legacy recovery Start = %#v", starts)
			}
			recovered, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(recovered[:len(original)], original) {
				t.Fatal("recovery rewrote old records")
			}
			for _, record := range recovered {
				if record.Event.ExecutionID != "" {
					t.Fatalf("legacy recovery invented ID: %#v", record)
				}
			}
			if resumed, err := c.Resume(t.Context(), har); resumed || err != nil {
				t.Fatalf("second Resume = %v, %v", resumed, err)
			}
			out, err := c.Replay(t.Context(), har)
			if err != nil || !reflect.DeepEqual(out, []string{"echo:hello"}) {
				t.Fatalf("recovered legacy Replay = %v, %v", out, err)
			}
			// The crashed v0.1.2 turn was recovered on this binary; a new Exec now makes a mixed log.
			c, err = v012Controller(log, model)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Exec(t.Context(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "new")}, int64(len(recovered))); err != nil {
				t.Fatal(err)
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			modelCalls = 0
			starts = nil
			out, err = c.Replay(t.Context(), har)
			if err != nil || !reflect.DeepEqual(out, []string{"echo:hello", "echo:new"}) {
				t.Fatalf("mixed Replay = %v, %v", out, err)
			}
			if modelCalls != 0 || len(starts) != 2 {
				t.Fatalf("mixed model calls/runs = %d/%d", modelCalls, len(starts))
			}
			var expectedHistory []api.Event
			for _, record := range recovered {
				expectedHistory = append(expectedHistory, record.Event)
			}
			if !reflect.DeepEqual(starts[1].History, expectedHistory) {
				t.Fatal("modern turn lost original legacy history")
			}
			after, err := log.Read(1)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("mixed Replay changed journal: %v", err)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV012MultiTurnReplayKeepsHistoricalLimitation(t *testing.T) {
	log, _ := v012Log(t, "multi-turn")
	c, err := v012Controller(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Replay(t.Context(), echoagent.Harness{}); err == nil || errors.Is(err, controller.ErrInvalidExecutionLog) || !strings.Contains(err.Error(), "model input hash mismatch") {
		t.Fatalf("want historical whole-log hash mismatch, got %v", err)
	}
	if c.ModelInvocations() != 0 {
		t.Fatal("legacy replay invoked model")
	}
	if resumed, err := c.Resume(t.Context(), echoagent.Harness{}); resumed || err != nil {
		t.Fatalf("completed multi-turn Resume = %v, %v", resumed, err)
	}
}

func TestV012ResumeUsesOnlyLastInput(t *testing.T) {
	log, original := v012Log(t, "multi-turn-crashed-output")
	var starts []api.Start
	c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		t.Fatal("recorded completion re-invoked model")
		return api.ModelResponse{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(t.Context(), observedLegacyHarness{&starts}); !resumed || err != nil {
		t.Fatalf("Resume = %v, %v", resumed, err)
	}
	var expectedHistory []api.Event
	for _, record := range original[:4] {
		expectedHistory = append(expectedHistory, record.Event)
	}
	if len(starts) != 1 || !reflect.DeepEqual(messageTexts(starts[0].Inputs), []string{"second"}) || !reflect.DeepEqual(starts[0].History, expectedHistory) {
		t.Fatalf("last-input Start = %#v", starts)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestV012NewExecRequiresCompletedLegacyBoundary(t *testing.T) {
	for _, name := range []string{"crashed-input", "crashed-output", "multi-turn-crashed-output"} {
		t.Run(name, func(t *testing.T) {
			log, original := v012Log(t, name)
			runs, modelCalls, observed := 0, 0, 0
			c, err := v012Controller(log, func(ctx context.Context, request api.ModelRequest) (api.ModelResponse, error) {
				modelCalls++
				return echoagent.Model(ctx, request)
			}, controller.WithObserver(controller.Observer{OnRecord: func(eventlog.Record) { observed++ }}))
			if err != nil {
				t.Fatal(err)
			}
			var starts []api.Start
			err = c.Exec(t.Context(), observedLegacyHarness{&starts}, []api.Message{*api.TextMessage("user", "new")}, int64(len(original)))
			runs = len(starts)
			if !errors.Is(err, controller.ErrInvalidExecutionLog) || !strings.Contains(err.Error(), "resume the legacy turn") {
				t.Fatalf("Exec must reject unfinished legacy boundary: %v", err)
			}
			if runs != 0 || modelCalls != 0 || observed != 0 {
				t.Fatalf("rejected Exec ran harness/model/appends: %d/%d/%d", runs, modelCalls, observed)
			}
			after, err := log.Read(1)
			if err != nil || !reflect.DeepEqual(original, after) {
				t.Fatalf("rejected Exec changed legacy journal: %v", err)
			}
		})
	}
}

func TestLifecycleOnlyJournalDoesNotInventLegacyInvocation(t *testing.T) {
	parent, child := memStore(t), memStore(t)
	if err := controller.Fork(parent, child, 0); err != nil {
		t.Fatal(err)
	}
	before, err := child.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	c, err := v012Controller(child, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.Replay(t.Context(), observedLegacyHarness{&starts}); err != nil || len(out) != 0 {
		t.Fatalf("lifecycle-only Replay = %v, %v", out, err)
	}
	if resumed, err := c.Resume(t.Context(), observedLegacyHarness{&starts}); resumed || err != nil {
		t.Fatalf("lifecycle-only Resume = %v, %v", resumed, err)
	}
	if len(starts) != 0 {
		t.Fatal("lifecycle-only journal invented a harness invocation")
	}
	after, err := child.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("lifecycle-only reconstruction changed log: %v", err)
	}
}

func TestV012ReplayUnfinishedLogAndMissingModelCompletion(t *testing.T) {
	for _, name := range []string{"crashed-output", "crashed-model"} {
		t.Run(name, func(t *testing.T) {
			log, original := v012Log(t, name)
			c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				t.Fatal("legacy replay repeated model")
				return api.ModelResponse{}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			out, err := c.Replay(t.Context(), echoagent.Harness{})
			if name == "crashed-output" {
				if err != nil || !reflect.DeepEqual(out, []string{"echo:hello"}) {
					t.Fatalf("unfinished Replay = %v, %v", out, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "expected a recorded completion") {
				t.Fatalf("missing completion Replay = %v", err)
			}
			after, readErr := log.Read(1)
			if readErr != nil || !reflect.DeepEqual(original, after) {
				t.Fatalf("unfinished Replay changed log: %v", readErr)
			}
			if name == "crashed-model" {
				resumed, err := c.Resume(t.Context(), echoagent.Harness{})
				if !resumed || err == nil || !strings.Contains(err.Error(), "recorded completion missing") {
					t.Fatalf("missing completion Resume = %v, %v", resumed, err)
				}
				if c.ModelInvocations() != 0 {
					t.Fatal("missing completion recovery repeated model")
				}
			}
		})
	}
}

func TestV012CommittedUpgradeFixtures(t *testing.T) {
	for _, name := range []string{"upgraded-resume", "mixed-completed", "mixed-interrupted"} {
		t.Run(name, func(t *testing.T) {
			log, original := v012Log(t, name)
			old := v012Records(t, "crashed-output")
			if !reflect.DeepEqual(original[:len(old)], old) {
				t.Fatal("mixed fixture lost the original crashed v0.1.2 prefix")
			}
			var starts []api.Start
			c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				t.Fatal("fixture recovery/replay invoked model")
				return api.ModelResponse{}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			har := observedLegacyHarness{&starts}
			resumed, err := c.Resume(t.Context(), har)
			if err != nil || resumed != (name == "mixed-interrupted") {
				t.Fatalf("Resume = %v, %v", resumed, err)
			}
			if resumed && (len(starts) != 1 || starts[0].ExecutionID == "" || len(starts[0].History) != 4) {
				t.Fatalf("modern recovery Start = %#v", starts)
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before[:len(original)], original) {
				t.Fatal("Resume rewrote fixture records")
			}
			want := []string{"echo:hello", "echo:new"}
			if name == "upgraded-resume" {
				want = want[:1]
			}
			if out, err := c.Replay(t.Context(), har); err != nil || !reflect.DeepEqual(out, want) {
				t.Fatalf("Replay = %v, %v", out, err)
			}
			after, err := log.Read(1)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("Replay changed fixture: %v", err)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV012LegacyShapeAndUpgradeBoundaryFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
		detail string
	}{
		{"idless start", []api.Event{{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}}}, "no execution_id"},
		{"nonlegacy approval", []api.Event{{Kind: api.EventApprovalRequest}}, "not a legacy event"},
		{"unfinished legacy then modern", []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "old")}, {Kind: api.EventInput, ExecutionID: "modern", Message: api.TextMessage("user", "new")}}, "unfinished legacy prefix"},
		{"idless after modern", []api.Event{{Kind: api.EventInput, ExecutionID: "modern", Message: api.TextMessage("user", "new")}, {Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}}}, "no execution_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := eventlog.AsStore(eventlog.New())
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, event := range tc.events {
				if _, err := log.Append(int64(i), fence, event); err != nil {
					t.Fatal(err)
				}
			}
			var starts []api.Start
			c, err := v012Controller(log, echoagent.Model)
			if err != nil {
				t.Fatal(err)
			}
			har := observedLegacyHarness{&starts}
			if _, err := c.Replay(t.Context(), har); !errors.Is(err, controller.ErrInvalidExecutionLog) || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("Replay = %v", err)
			}
			if resumed, err := c.Resume(t.Context(), har); resumed || !errors.Is(err, controller.ErrInvalidExecutionLog) || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("Resume = %v, %v", resumed, err)
			}
			if len(starts) != 0 {
				t.Fatal("invalid log ran harness")
			}
			if head, err := log.Head(); err != nil || head != int64(len(tc.events)) {
				t.Fatalf("invalid log appended: %d, %v", head, err)
			}
		})
	}
}

type legacyUsageHarness struct{ echoagent.Harness }

func (legacyUsageHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if err := sink.Usage(ctx, api.Usage{Model: "echo", InputTokens: 1}); err != nil {
		return err
	}
	if err := (echoagent.Harness{}).Run(ctx, start, sink); err != nil {
		return err
	}
	return sink.Usage(ctx, api.Usage{Model: "echo", OutputTokens: 2})
}

func TestV012UsageKeepsHistoricalServingAndLiveBoundary(t *testing.T) {
	log, original := v012Log(t, "crashed-output")
	c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		t.Fatal("legacy accounting repeated model")
		return api.ModelResponse{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.Replay(t.Context(), legacyUsageHarness{}); err != nil || !reflect.DeepEqual(out, []string{"echo:hello"}) {
		t.Fatalf("Usage Replay = %v, %v", out, err)
	}
	if resumed, err := c.Resume(t.Context(), legacyUsageHarness{}); !resumed || err != nil {
		t.Fatalf("Usage Resume = %v, %v", resumed, err)
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(original)+2 || after[len(original)].Event.Kind != api.EventUsage || after[len(original)+1].Event.Kind != api.EventEnd {
		t.Fatalf("legacy continuation kinds = %v", after)
	}
	if usage := after[len(original)].Event.Usage; usage == nil || usage.OutputTokens != 2 {
		t.Fatalf("live accounting = %#v", usage)
	}
	if _, err := c.Replay(t.Context(), legacyUsageHarness{}); err != nil {
		t.Fatalf("Replay including recorded legacy Usage = %v", err)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestV012ToolIntentRedriveRetainsScope(t *testing.T) {
	log := memStore(t)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	call := sessionToolCall()
	for i, event := range []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "charge")}, {Kind: api.EventToolCall, ToolCall: &call}} {
		if _, err := log.Append(int64(i), fence, event); err != nil {
			t.Fatal(err)
		}
	}
	toolCalls := 0
	c, err := controller.New(log, echoagent.Model, controller.WithSessionUID("legacy-session"), controller.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, recovered api.ToolCall) (api.ToolResult, error) {
		toolCalls++
		if scope.SessionUID != "legacy-session" || !reflect.DeepEqual(call, recovered) {
			t.Fatalf("tool scope/call changed: %#v, %#v", scope, recovered)
		}
		return api.ToolResult{Output: map[string]any{"receipt": "original"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(t.Context(), sessionToolHarness{}); !resumed || err != nil {
		t.Fatalf("tool Resume = %v, %v", resumed, err)
	}
	if toolCalls != 1 {
		t.Fatalf("tool invocations = %d", toolCalls)
	}
	if out, err := c.Replay(t.Context(), sessionToolHarness{}); err != nil || !reflect.DeepEqual(out, []string{"original"}) {
		t.Fatalf("tool Replay = %v, %v", out, err)
	}
	if toolCalls != 1 {
		t.Fatal("Replay repeated legacy effect")
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestV012ForkedToolIntentSafety(t *testing.T) {
	for _, resultLocation := range []string{"none", "before fork", "after fork"} {
		t.Run(resultLocation, func(t *testing.T) {
			parent := memStore(t)
			fence, err := parent.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			call := sessionToolCall()
			result := api.ToolResult{ID: call.ID, Output: map[string]any{"receipt": "original"}}
			events := []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "charge")}, {Kind: api.EventToolCall, ToolCall: &call}}
			if resultLocation == "before fork" {
				events = append(events, api.Event{Kind: api.EventToolResult, Result: &result})
			}
			for i, event := range events {
				if _, err := parent.Append(int64(i), fence, event); err != nil {
					t.Fatal(err)
				}
			}
			child := memStore(t)
			if err := controller.Fork(parent, child, int64(len(events))); err != nil {
				t.Fatal(err)
			}
			if resultLocation == "after fork" {
				fence, err := child.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := child.Append(int64(len(events))+1, fence, api.Event{Kind: api.EventToolResult, Result: &result}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := child.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			c, err := controller.New(child, echoagent.Model, controller.WithSessionUID("child"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				t.Fatal("legacy child repeated parent's effect")
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := c.Resume(t.Context(), sessionToolHarness{})
			if resultLocation == "none" {
				if resumed || !errors.Is(err, controller.ErrInheritedToolIntent) {
					t.Fatalf("Resume = %v, %v", resumed, err)
				}
				if err := c.Exec(t.Context(), sessionToolHarness{}, []api.Message{*api.TextMessage("user", "new charge")}, int64(len(before))); !errors.Is(err, controller.ErrInvalidExecutionLog) {
					t.Fatalf("Exec after unresolved legacy fork = %v", err)
				}
				after, err := child.Read(1)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected recovery changed journal: %v", err)
				}
			} else {
				if !resumed || err != nil {
					t.Fatalf("Resume = %v, %v", resumed, err)
				}
				if out, err := c.Replay(t.Context(), sessionToolHarness{}); err != nil || !reflect.DeepEqual(out, []string{"original"}) {
					t.Fatalf("Replay = %v, %v", out, err)
				}
			}
			if err := child.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestModernJournalMissingExecutionIDStillFails(t *testing.T) {
	for _, withMarker := range []bool{false, true} {
		for _, missingKind := range []api.EventKind{api.EventInput, api.EventOutput, api.EventEnd} {
			t.Run(string(missingKind)+"/marker="+map[bool]string{false: "no", true: "yes"}[withMarker], func(t *testing.T) {
				log := eventlog.AsStore(eventlog.New())
				fence, err := log.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				events := []api.Event{
					{ExecutionID: "modern", Kind: api.EventInput, Message: api.TextMessage("user", "hello")},
					{ExecutionID: "modern", Kind: api.EventOutput, Message: api.TextMessage("assistant", "hello")},
					{ExecutionID: "modern", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
				}
				for i := range events {
					if events[i].Kind == missingKind {
						events[i].ExecutionID = ""
					}
				}
				if withMarker {
					count := int64(1)
					events = append([]api.Event{{ExecutionID: "modern", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: &count}}}, events...)
				}
				for i, event := range events {
					if _, err := log.Append(int64(i), fence, event); err != nil {
						t.Fatal(err)
					}
				}
				var starts []api.Start
				c, err := v012Controller(log, echoagent.Model)
				if err != nil {
					t.Fatal(err)
				}
				har := observedLegacyHarness{&starts}
				if _, err := c.Replay(t.Context(), har); !errors.Is(err, controller.ErrInvalidExecutionLog) {
					t.Fatalf("Replay accepted malformed modern log: %v", err)
				}
				if resumed, err := c.Resume(t.Context(), har); resumed || !errors.Is(err, controller.ErrInvalidExecutionLog) {
					t.Fatalf("Resume accepted malformed modern log: %v, %v", resumed, err)
				}
				if len(starts) != 0 {
					t.Fatal("malformed modern log ran harness")
				}
			})
		}
	}
}
