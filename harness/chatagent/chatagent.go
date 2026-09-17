// Package chatagent provides a text-only conversational reference harness. The host supplies
// the model implementation; the harness only builds context and calls EventSink.Model.
package chatagent

import (
	"context"

	"github.com/aramase/agentsessions/api"
)

// Harness sends the full recorded conversation to a configured model on every turn.
// It holds no durable in-memory state beyond the event log.
type Harness struct {
	Model string
}

var _ api.Harness = Harness{}

// Describe declares the configured model and stateless replay contract.
func (h Harness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID:     "chat",
		Models: []string{h.Model},
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

// Run builds one turn's context in journal order and lets the host record the model completion.
func (h Harness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
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
	// History precedes this turn; current inputs belong in the request exactly once.
	messages = append(messages, start.Inputs...)
	_, err := sink.Model(ctx, api.ModelRequest{
		Model:    h.Model,
		Messages: messages,
	})
	return err
}
