package controller_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/sqlitelog"
)

// historyAwareHarness reconstructs model context from completed prior turns, then appends the
// current turn's inputs. This is the Start.History/Start.Inputs contract a conversational harness
// relies on.
type historyAwareHarness struct{}

func (historyAwareHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID: "history-aware",
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

func (historyAwareHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
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
