package chatagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/chatagent"
	"github.com/aramase/agentsessions/sqlitelog"
)

func TestControllerReplay(t *testing.T) {
	question := *api.TextMessage("user", "first question")
	followUp := *api.TextMessage("user", "follow-up")
	reply := *api.TextMessage("assistant", "first reply")
	for _, tt := range []struct {
		name      string
		inputs    [][]api.Message
		contexts  [][]api.Message
		failFirst bool
	}{
		{
			name:     "single turn",
			inputs:   [][]api.Message{{question}},
			contexts: [][]api.Message{{question}},
		},
		{
			name:     "multiple turns",
			inputs:   [][]api.Message{{question}, {followUp}},
			contexts: [][]api.Message{{question}, {question, reply, followUp}},
		},
		{
			name:     "grouped and repeated inputs",
			inputs:   [][]api.Message{{question, question}, {question, followUp}},
			contexts: [][]api.Message{{question, question}, {question, question, reply, question, followUp}},
		},
		{
			name:      "failed input remains in subsequent context",
			inputs:    [][]api.Message{{question}, {followUp}},
			contexts:  [][]api.Message{{question}, {question, followUp}},
			failFirst: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			log := store.Session("chat")
			modelErr := errors.New("model unavailable")
			var requests []api.ModelRequest
			var wantOutputs []string
			live, err := controller.New(log, func(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
				requests = append(requests, req)
				if tt.failFirst && len(requests) == 1 {
					return api.ModelResponse{}, modelErr
				}
				text := "first reply"
				if len(requests) > 1 {
					text = "second reply"
				}
				wantOutputs = append(wantOutputs, text)
				return api.ModelResponse{Message: *api.TextMessage("assistant", text)}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			harness := chatagent.Harness{Model: "test-model"}
			for i, inputs := range tt.inputs {
				head, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				err = live.Exec(t.Context(), harness, inputs, head)
				if tt.failFirst && i == 0 {
					if !errors.Is(err, modelErr) {
						t.Fatalf("failed turn error = %v, want %v", err, modelErr)
					}
					records, err := log.Read(1)
					if err != nil {
						t.Fatal(err)
					}
					var kinds []api.EventKind
					for _, record := range records {
						kinds = append(kinds, record.Event.Kind)
					}
					if !reflect.DeepEqual(kinds, []api.EventKind{api.EventExecutionStart, api.EventInput, api.EventModelCall, api.EventError}) {
						t.Fatalf("failed turn records = %v", kinds)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if len(requests) != len(tt.contexts) {
				t.Fatalf("model calls = %d, want %d", len(requests), len(tt.contexts))
			}
			for i, want := range tt.contexts {
				if requests[i].Model != "test-model" || !reflect.DeepEqual(requests[i].Messages, want) {
					t.Fatalf("turn %d request = %+v, want configured model and %+v", i+1, requests[i], want)
				}
			}
			liveOutputs, err := live.Outputs()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(liveOutputs, wantOutputs) {
				t.Fatalf("live outputs = %v, want %v", liveOutputs, wantOutputs)
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			replayCalls := 0
			replay, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				replayCalls++
				return api.ModelResponse{}, errors.New("unexpected live model call during replay")
			})
			if err != nil {
				t.Fatal(err)
			}
			outputs, err := replay.Replay(t.Context(), harness)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(outputs, liveOutputs) {
				t.Fatalf("replay outputs = %v, want %v", outputs, liveOutputs)
			}
			if replayCalls != 0 || replay.ModelInvocations() != 0 {
				t.Fatal("replay invoked the live model")
			}
			after, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatal("replay changed the journal")
			}
			if err := log.Verify(); err != nil {
				t.Fatalf("journal integrity: %v", err)
			}
		})
	}
}

func TestControllerReplayWithSystemPrompt(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	log := store.Session("chat-config")
	harness := chatagent.Harness{Model: "test-model"}
	question := *api.TextMessage("user", "question")
	reply := *api.TextMessage("assistant", "reply")
	firstPrompt := *api.TextMessage("system", "first instruction")
	secondPrompt := *api.TextMessage("system", "second instruction")
	for i, tt := range []struct {
		config []byte
		want   []api.Message
	}{
		{
			config: []byte(`{"system_prompt":"first instruction"}`),
			want:   []api.Message{firstPrompt, question},
		},
		{
			config: []byte(`{"system_prompt":"first instruction"}`),
			want:   []api.Message{firstPrompt, question, reply, question},
		},
		{
			config: []byte(`{"system_prompt":"second instruction"}`),
			want:   []api.Message{secondPrompt, question, reply, question, reply, question},
		},
		{want: []api.Message{question, reply, question, reply, question, reply, question}},
	} {
		calls := 0
		live, err := controller.New(log, func(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
			calls++
			want := api.ModelRequest{Model: "test-model", Messages: tt.want}
			if !reflect.DeepEqual(req, want) {
				t.Errorf("turn %d model request = %+v, want %+v", i+1, req, want)
			}
			return api.ModelResponse{Message: reply}, nil
		}, controller.WithStart(tt.config, 0))
		if err != nil {
			t.Fatal(err)
		}
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		if err := live.Exec(t.Context(), harness, []api.Message{question}, head); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("turn %d model calls = %d, want 1", i+1, calls)
		}
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	replayCalls := 0
	replay, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		replayCalls++
		return api.ModelResponse{}, errors.New("unexpected live model call during replay")
	}, controller.WithStart([]byte(`{"system_prompt":"not a replay default"}`), 0))
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := replay.Replay(t.Context(), harness)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outputs, []string{"reply", "reply", "reply", "reply"}) {
		t.Fatalf("replay outputs = %v", outputs)
	}
	if replayCalls != 0 || replay.ModelInvocations() != 0 {
		t.Fatal("replay invoked the live model")
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatal("replay changed the journal")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("journal integrity: %v", err)
	}
}

func TestDescribe(t *testing.T) {
	got, err := (chatagent.Harness{Model: "test-model"}).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := api.Descriptor{
		ID:     "chat",
		Models: []string{"test-model"},
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("descriptor = %+v, want %+v", got, want)
	}
}

func TestRunContext(t *testing.T) {
	question := *api.TextMessage("user", "first question")
	reply := *api.TextMessage("assistant", "first reply")
	followUp := *api.TextMessage("user", "follow-up")
	history := []api.Event{
		{Kind: api.EventInput, Message: &question},
		{Kind: api.EventModelCall},
		{Kind: api.EventOutput, Message: &reply},
		{Kind: api.EventEnd},
	}
	tests := []struct {
		name  string
		start api.Start
		want  []api.Message
	}{
		{
			name:  "single turn",
			start: api.Start{Inputs: []api.Message{question}},
			want:  []api.Message{question},
		},
		{
			name:  "second turn",
			start: api.Start{History: history, Inputs: []api.Message{followUp}},
			want:  []api.Message{question, reply, followUp},
		},
		{
			name:  "multiple current inputs including repeated text",
			start: api.Start{History: history, Inputs: []api.Message{followUp, question, followUp}},
			want:  []api.Message{question, reply, followUp, question, followUp},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &modelSink{}
			ctx := t.Context()
			if err := (chatagent.Harness{Model: "test-model"}).Run(ctx, &tt.start, sink); err != nil {
				t.Fatal(err)
			}
			if sink.calls != 1 {
				t.Fatalf("model calls = %d, want 1", sink.calls)
			}
			if sink.ctx != ctx {
				t.Fatal("model did not receive the turn context")
			}
			want := api.ModelRequest{Model: "test-model", Messages: tt.want}
			if !reflect.DeepEqual(sink.request, want) {
				t.Fatalf("model request = %+v, want %+v", sink.request, want)
			}
		})
	}
}

func TestRunConfig(t *testing.T) {
	question := *api.TextMessage("user", "first question")
	reply := *api.TextMessage("assistant", "first reply")
	followUp := *api.TextMessage("user", "follow-up")
	for _, tt := range []struct {
		name   string
		config []byte
		want   []api.Message
	}{
		{name: "nil", want: []api.Message{question, reply, followUp}},
		{name: "empty", config: []byte{}, want: []api.Message{question, reply, followUp}},
		{name: "empty object", config: []byte(`{}`), want: []api.Message{question, reply, followUp}},
		{name: "empty prompt", config: []byte(`{"system_prompt":""}`), want: []api.Message{question, reply, followUp}},
		{
			name:   "prompt",
			config: []byte(`{"system_prompt":"Be concise."}`),
			want:   []api.Message{*api.TextMessage("system", "Be concise."), question, reply, followUp},
		},
		{
			name:   "duplicate system_prompt keys resolve last-wins",
			config: []byte(`{"system_prompt":"a","system_prompt":"b"}`),
			want:   []api.Message{*api.TextMessage("system", "b"), question, reply, followUp},
		},
		{
			name:   "whitespace and newlines preserved",
			config: []byte(" {\"system_prompt\":\"  First line.\\nSecond line.\\n\\t \"} \n"),
			want:   []api.Message{*api.TextMessage("system", "  First line.\nSecond line.\n\t "), question, reply, followUp},
		},
		{
			name:   "whitespace-only prompt is not empty",
			config: []byte(`{"system_prompt":" \n\t"}`),
			want:   []api.Message{*api.TextMessage("system", " \n\t"), question, reply, followUp},
		},
		{
			name:   "unknown fields ignored",
			config: []byte(`{"future":{"nested":[null,true,1e400]},"System_Prompt":"not the field"}`),
			want:   []api.Message{question, reply, followUp},
		},
		{
			name:   "unknown fields with prompt",
			config: []byte(`{"system_prompt":"Be concise.","future":null}`),
			want:   []api.Message{*api.TextMessage("system", "Be concise."), question, reply, followUp},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			start := api.Start{
				Config: tt.config,
				History: []api.Event{
					{Kind: api.EventInput, Message: &question},
					{Kind: api.EventModelCall},
					{Kind: api.EventOutput, Message: &reply},
					{Kind: api.EventEnd},
				},
				Inputs: []api.Message{followUp},
			}
			sink := &modelSink{}
			if err := (chatagent.Harness{Model: "test-model"}).Run(t.Context(), &start, sink); err != nil {
				t.Fatal(err)
			}
			want := api.ModelRequest{Model: "test-model", Messages: tt.want}
			if sink.calls != 1 || !reflect.DeepEqual(sink.request, want) {
				t.Fatalf("model calls = %d, request = %+v, want %+v", sink.calls, sink.request, want)
			}
		})
	}
}

func TestRunRejectsInvalidConfigBeforeModel(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config string
		want   string
	}{
		{name: "whitespace only", config: " \n\t", want: "chatagent: config must be a JSON object"},
		{name: "malformed", config: `{"system_prompt":"private prompt"`, want: "chatagent: config must be a JSON object"},
		{name: "trailing token", config: `{"system_prompt":"private prompt"} private`, want: "chatagent: config must be a JSON object"},
		{name: "trailing object", config: `{"system_prompt":"private prompt"} {}`, want: "chatagent: config must be a JSON object"},
		{name: "null object", config: `null`, want: "chatagent: config must be a JSON object"},
		{name: "array", config: `[{"system_prompt":"private prompt"}]`, want: "chatagent: config must be a JSON object"},
		{name: "string", config: `"private prompt"`, want: "chatagent: config must be a JSON object"},
		{name: "number", config: `123`, want: "chatagent: config must be a JSON object"},
		{name: "boolean", config: `true`, want: "chatagent: config must be a JSON object"},
		{name: "null prompt", config: `{"system_prompt":null}`, want: "chatagent: system_prompt must be a string"},
		{name: "number prompt", config: `{"system_prompt":123}`, want: "chatagent: system_prompt must be a string"},
		{name: "boolean prompt", config: `{"system_prompt":true}`, want: "chatagent: system_prompt must be a string"},
		{name: "array prompt", config: `{"system_prompt":["private prompt"]}`, want: "chatagent: system_prompt must be a string"},
		{name: "object prompt", config: `{"system_prompt":{"private":"prompt"}}`, want: "chatagent: system_prompt must be a string"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := &modelSink{}
			err := (chatagent.Harness{Model: "test-model"}).Run(t.Context(), &api.Start{
				Config: []byte(tt.config),
				Inputs: []api.Message{*api.TextMessage("user", "hello")},
			}, sink)
			if err == nil {
				t.Fatal("invalid config was accepted")
			}
			// Errors may be journaled; only fixed diagnostics, never config values, are safe.
			if err.Error() != tt.want {
				t.Fatalf("error = %q, want safe diagnostic %q", err, tt.want)
			}
			if sink.calls != 0 {
				t.Fatalf("model calls = %d, want 0", sink.calls)
			}
		})
	}
}

func TestRunPreservesConversationWithPrompt(t *testing.T) {
	priorSystem := *api.TextMessage("system", "prior input instruction")
	question := *api.TextMessage("user", "first question")
	reply := api.Message{Role: "assistant", Parts: []api.Part{
		{Text: &api.TextPart{Text: "first reply"}},
		{Reasoning: &api.ReasoningPart{Provider: "test", Opaque: []byte("opaque reasoning")}},
	}}
	failedInput := *api.TextMessage("user", "failed question")
	currentSystem := *api.TextMessage("system", "current input instruction")
	followUp := api.Message{Role: "user", Parts: []api.Part{
		{Text: &api.TextPart{Text: "follow-up"}},
		{Data: map[string]any{"detail": "keep"}},
	}}
	start := api.Start{
		Config: []byte(`{"system_prompt":"config instruction"}`),
		History: []api.Event{
			{Kind: api.EventInput, Message: &priorSystem},
			{Kind: api.EventInput, Message: &question},
			{Kind: api.EventOutput, Message: &reply},
			{Kind: api.EventEnd},
			{Kind: api.EventInput, Message: &failedInput},
			{Kind: api.EventError},
		},
		Inputs: []api.Message{currentSystem, followUp, followUp},
	}
	before, err := json.Marshal([]any{start.Config, start.History, start.Inputs})
	if err != nil {
		t.Fatal(err)
	}
	sink := &modelSink{}
	if err := (chatagent.Harness{Model: "test-model"}).Run(t.Context(), &start, sink); err != nil {
		t.Fatal(err)
	}
	want := []api.Message{
		*api.TextMessage("system", "config instruction"),
		priorSystem, question, reply, failedInput, currentSystem, followUp, followUp,
	}
	if sink.calls != 1 || !reflect.DeepEqual(sink.request.Messages, want) {
		t.Fatalf("model calls = %d, messages = %+v, want %+v", sink.calls, sink.request.Messages, want)
	}
	after, err := json.Marshal([]any{start.Config, start.History, start.Inputs})
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("Run changed config, history, or inputs")
	}
}

func TestRunConfigIsPerExecution(t *testing.T) {
	harness := chatagent.Harness{Model: "test-model"}
	question := *api.TextMessage("user", "first question")
	reply := *api.TextMessage("assistant", "first reply")
	followUp := *api.TextMessage("user", "follow-up")
	for _, tt := range []struct {
		config []byte
		want   []api.Message
	}{
		{
			config: []byte(`{"system_prompt":"first instruction"}`),
			want:   []api.Message{*api.TextMessage("system", "first instruction"), question, reply, followUp},
		},
		{
			config: []byte(`{"system_prompt":"second instruction"}`),
			want:   []api.Message{*api.TextMessage("system", "second instruction"), question, reply, followUp},
		},
		{want: []api.Message{question, reply, followUp}},
	} {
		sink := &modelSink{}
		if err := harness.Run(t.Context(), &api.Start{
			Config: tt.config,
			History: []api.Event{
				{Kind: api.EventInput, Message: &question},
				{Kind: api.EventOutput, Message: &reply},
			},
			Inputs: []api.Message{followUp},
		}, sink); err != nil {
			t.Fatal(err)
		}
		if sink.calls != 1 || !reflect.DeepEqual(sink.request.Messages, tt.want) {
			t.Fatalf("model calls = %d, messages = %+v, want %+v", sink.calls, sink.request.Messages, tt.want)
		}
	}
}

func TestRunExcludesNonConversationEvents(t *testing.T) {
	question := api.TextMessage("user", "first question")
	reply := api.TextMessage("assistant", "first reply")
	history := []api.Event{{Kind: api.EventInput, Message: question}}
	for _, kind := range []api.EventKind{
		api.EventModelCall, api.EventEnd, api.EventError, api.EventLifecycle,
		api.EventToolCall, api.EventToolResult, api.EventUsage,
		api.EventApprovalRequest, api.EventApprovalResult, api.EventKind("UNKNOWN"),
	} {
		// Even an audit event carrying a message must not enter the conversation.
		history = append(history, api.Event{Kind: kind, Message: api.TextMessage("assistant", "not context")})
	}
	history = append(history,
		api.Event{Kind: api.EventInput},
		api.Event{Kind: api.EventOutput},
		api.Event{Kind: api.EventOutput, Message: reply},
	)
	followUp := *api.TextMessage("user", "follow-up")
	sink := &modelSink{}
	err := (chatagent.Harness{Model: "test-model"}).Run(t.Context(), &api.Start{
		History: history,
		Inputs:  []api.Message{followUp},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	want := []api.Message{*question, *reply, followUp}
	if sink.calls != 1 || !reflect.DeepEqual(sink.request.Messages, want) {
		t.Fatalf("model calls = %d, messages = %+v, want %+v", sink.calls, sink.request.Messages, want)
	}
}

func TestRunPropagatesModelError(t *testing.T) {
	modelErr := errors.New("model unavailable")
	sink := &modelSink{err: modelErr}
	err := (chatagent.Harness{Model: "test-model"}).Run(t.Context(), &api.Start{
		Inputs: []api.Message{*api.TextMessage("user", "hello")},
	}, sink)
	if !errors.Is(err, modelErr) {
		t.Fatalf("error = %v, want %v", err, modelErr)
	}
	if sink.calls != 1 {
		t.Fatalf("model calls = %d, want 1", sink.calls)
	}
}

// Any call other than Model fails through the nil embedded sink, including duplicate Output.
type modelSink struct {
	api.EventSink
	ctx     context.Context
	request api.ModelRequest
	calls   int
	err     error
}

func (s *modelSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	s.ctx, s.request = ctx, req
	s.calls++
	return api.ModelResponse{Message: *api.TextMessage("assistant", "reply")}, s.err
}
