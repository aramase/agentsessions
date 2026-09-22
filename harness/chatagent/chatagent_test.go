package chatagent_test

import (
	"context"
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
					if !reflect.DeepEqual(kinds, []api.EventKind{api.EventInput, api.EventModelCall, api.EventError}) {
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
