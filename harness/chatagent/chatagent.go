// Package chatagent provides a text-only conversational reference harness. The host supplies
// the model implementation; the harness only builds context and calls EventSink.Model.
package chatagent

import (
	"context"
	"encoding/json"
	"errors"

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
		ID:      "chat",
		Version: "1", // bump when execution or config interpretation changes, not for host releases
		Models:  []string{h.Model},
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

// Run builds one turn's context in journal order and lets the host record the model completion.
func (h Harness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	prompt, err := parseSystemPrompt(start.Config)
	if err != nil {
		return err
	}
	messages := make([]api.Message, 0, 1+len(start.History)+len(start.Inputs))
	if prompt != "" {
		messages = append(messages, *api.TextMessage("system", prompt))
	}
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
	_, err = sink.Model(ctx, api.ModelRequest{
		Model:    h.Model,
		Messages: messages,
	})
	return err
}

func parseSystemPrompt(config []byte) (string, error) {
	if len(config) == 0 {
		return "", nil
	}
	// Retain unknown values as raw JSON so future fields are ignored, not interpreted.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(config, &fields); err != nil || fields == nil {
		// Decoder errors can include config content; only fixed diagnostics may be journaled.
		return "", errors.New("chatagent: config must be a JSON object")
	}
	raw, ok := fields["system_prompt"]
	if !ok {
		return "", nil
	}
	var prompt *string
	if err := json.Unmarshal(raw, &prompt); err != nil || prompt == nil {
		return "", errors.New("chatagent: system_prompt must be a string")
	}
	return *prompt, nil
}
