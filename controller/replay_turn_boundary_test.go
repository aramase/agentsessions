package controller_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/sqlitelog"
)

// historyAwareHarness reconstructs model context from completed prior turns, then appends the
// current turn's inputs. This is the Start.History/Start.Inputs contract a conversational harness
// relies on.
type historyAwareHarness struct {
	starts *[]api.Start
}

func (historyAwareHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID: "history-aware",
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

func (h historyAwareHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.starts != nil {
		*h.starts = append(*h.starts, api.Start{
			ExecutionID: start.ExecutionID,
			History:     append([]api.Event(nil), start.History...),
			Inputs:      append([]api.Message(nil), start.Inputs...),
		})
	}

	messages := make([]api.Message, 0, len(start.History)+len(start.Inputs))
	for _, event := range start.History {
		if event.Message == nil {
			continue
		}
		switch event.Kind {
		case api.EventInput, api.EventOutput:
			messages = append(messages, *event.Message)
		}
	}
	messages = append(messages, start.Inputs...)

	_, err := sink.Model(ctx, api.ModelRequest{
		Model:    "test-model",
		Messages: messages,
	})
	return err
}

func TestReplayUsesExecutionIDsForTurnBoundaries(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("session")

	replies := []string{"first reply", "second reply"}
	liveCalls := 0
	live, err := controller.New(
		log,
		func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			reply := replies[liveCalls]
			liveCalls++
			return api.ModelResponse{Message: *api.TextMessage("assistant", reply)}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	same := *api.TextMessage("user", "same")
	var liveStarts []api.Start
	liveHarness := historyAwareHarness{starts: &liveStarts}
	if err := live.Exec(context.Background(), liveHarness, []api.Message{same, same}, 0); err != nil {
		t.Fatal(err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(context.Background(), liveHarness, []api.Message{same}, head); err != nil {
		t.Fatal(err)
	}

	recordsBeforeReplay, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	firstID := recordsBeforeReplay[0].Event.ExecutionID
	secondID := recordsBeforeReplay[5].Event.ExecutionID
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatalf("execution IDs = %q and %q, want distinct nonempty IDs", firstID, secondID)
	}
	if len(liveStarts) != 2 {
		t.Fatalf("live harness runs = %d, want 2", len(liveStarts))
	}
	if liveStarts[0].ExecutionID != firstID || liveStarts[1].ExecutionID != secondID {
		t.Fatalf("live harness execution IDs = %q, %q; want %q, %q",
			liveStarts[0].ExecutionID, liveStarts[1].ExecutionID, firstID, secondID)
	}
	for i, record := range recordsBeforeReplay {
		want := firstID
		if i >= 5 {
			want = secondID
		}
		if record.Event.ExecutionID != want {
			t.Fatalf("event %d execution ID = %q, want %q", i+1, record.Event.ExecutionID, want)
		}
	}

	var replayStarts []api.Start
	replay, err := controller.New(
		log,
		func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			t.Fatal("replay invoked the live model")
			return api.ModelResponse{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := replay.Replay(context.Background(), historyAwareHarness{starts: &replayStarts})
	if err != nil {
		t.Fatalf("replay history-aware harness: %v", err)
	}
	if !reflect.DeepEqual(outputs, replies) {
		t.Fatalf("replay outputs = %v, want %v", outputs, replies)
	}
	if len(replayStarts) != 2 {
		t.Fatalf("replay harness runs = %d, want 2", len(replayStarts))
	}
	if replayStarts[0].ExecutionID != firstID || replayStarts[1].ExecutionID != secondID {
		t.Fatalf("replay execution IDs = %q, %q; want %q, %q",
			replayStarts[0].ExecutionID, replayStarts[1].ExecutionID, firstID, secondID)
	}
	if got := messageTexts(replayStarts[0].Inputs); !reflect.DeepEqual(got, []string{"same", "same"}) {
		t.Fatalf("first turn inputs = %v, want [same same]", got)
	}
	if len(replayStarts[0].History) != 0 {
		t.Fatalf("first turn history has %d events, want 0", len(replayStarts[0].History))
	}
	if got := messageTexts(replayStarts[1].Inputs); !reflect.DeepEqual(got, []string{"same"}) {
		t.Fatalf("second turn inputs = %v, want [same]", got)
	}
	if got := eventKinds(replayStarts[1].History); !reflect.DeepEqual(got, []api.EventKind{
		api.EventInput,
		api.EventInput,
		api.EventModelCall,
		api.EventOutput,
		api.EventEnd,
	}) {
		t.Fatalf("second turn history kinds = %v", got)
	}

	recordsAfterReplay, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recordsAfterReplay, recordsBeforeReplay) {
		t.Fatal("replay changed the journal")
	}
}

type failingHarness struct{}

func (failingHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "failing"}, nil
}

func (failingHarness) Run(context.Context, *api.Start, api.EventSink) error {
	return errors.New("failed attempt")
}

func TestReplayIncludesFailedExecutionInLaterHistory(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("session")

	live, err := controller.New(
		log,
		func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			return api.ModelResponse{Message: *api.TextMessage("assistant", "success")}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(context.Background(), failingHarness{}, []api.Message{*api.TextMessage("user", "failed")}, 0); err == nil {
		t.Fatal("failed execution returned nil")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(context.Background(), historyAwareHarness{}, []api.Message{*api.TextMessage("user", "success")}, head); err != nil {
		t.Fatal(err)
	}

	var starts []api.Start
	replay, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.Replay(context.Background(), historyAwareHarness{starts: &starts}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 1 {
		t.Fatalf("replayed executions = %d, want 1 completed execution", len(starts))
	}
	if got := eventKinds(starts[0].History); !reflect.DeepEqual(got, []api.EventKind{
		api.EventInput,
		api.EventError,
	}) {
		t.Fatalf("successful execution history = %v, want [INPUT ERROR]", got)
	}
	if got := messageTexts(starts[0].Inputs); !reflect.DeepEqual(got, []string{"success"}) {
		t.Fatalf("successful execution inputs = %v, want [success]", got)
	}
}

func TestReplayRejectsMissingExecutionID(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("session")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(0, fence, api.Event{
		Kind:    api.EventInput,
		Message: api.TextMessage("user", "unstamped"),
	}); err != nil {
		t.Fatal(err)
	}

	replay, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.Replay(context.Background(), historyAwareHarness{}); !errors.Is(err, controller.ErrInvalidExecutionLog) {
		t.Fatalf("replay error = %v, want ErrInvalidExecutionLog", err)
	}
}

func messageTexts(messages []api.Message) []string {
	texts := make([]string, 0, len(messages))
	for _, message := range messages {
		texts = append(texts, message.Text())
	}
	return texts
}

func eventKinds(events []api.Event) []api.EventKind {
	kinds := make([]api.EventKind, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

func TestReplayPreservesTurnHistoryBoundary(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("session")

	live, err := controller.New(
		log,
		func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			return api.ModelResponse{
				Message: *api.TextMessage("assistant", "recorded reply"),
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(
		context.Background(),
		historyAwareHarness{},
		[]api.Message{*api.TextMessage("user", "hello")},
		0,
	); err != nil {
		t.Fatal(err)
	}
	headBeforeReplay, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	replay, err := controller.New(
		log,
		func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			t.Fatal("replay invoked the live model")
			return api.ModelResponse{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := replay.Replay(context.Background(), historyAwareHarness{})
	if err != nil {
		t.Fatalf("replay history-aware harness: %v", err)
	}
	if !reflect.DeepEqual(outputs, []string{"recorded reply"}) {
		t.Fatalf("replay outputs = %v, want [recorded reply]", outputs)
	}
	if replay.ModelInvocations() != 0 {
		t.Fatalf("replay invoked the model %d times, want 0", replay.ModelInvocations())
	}
	headAfterReplay, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if headAfterReplay != headBeforeReplay {
		t.Fatalf("journal head = %d after replay, want unchanged at %d", headAfterReplay, headBeforeReplay)
	}
}
