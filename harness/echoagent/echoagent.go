// Package echoagent is a trivial STATELESS_REPLAY api.Harness: it feeds the last input to the
// model through the host-mediated sink and lets the recorded completion be the output. It exists
// to close the agentsessions loop end-to-end (controller → harness → journal → replay) without a
// real model or tools, and is the harness used by the pod resume/fork demo.
package echoagent

import (
	"context"

	"github.com/aramase/agentsessions/api"
)

// Harness is the echo BYOH. It holds no durable in-memory state beyond the event log, so it runs
// on any runtime (including a plain pod) and is reconstructed on resume/fork purely by replay.
type Harness struct{}

// Describe returns the static contract: an echo harness that resumes by stateless replay.
func (Harness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID:     "echo",
		Models: []string{"echo"},
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

// Run performs one execution: it sends the last input to the model via the sink (the only
// nondeterministic op), which the host records live and serves from the journal on replay.
func (Harness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	text := ""
	if n := len(s.Inputs); n > 0 {
		text = s.Inputs[n-1].Text()
	}
	_, err := sink.Model(ctx, api.ModelRequest{
		Model:    "echo",
		Messages: []api.Message{*api.TextMessage("user", text)},
	})
	return err
}

// Model is the echo "model": it returns the last message text prefixed with "echo:". It stands in
// for a real model provider; on replay the host serves the recorded completion instead of calling
// this.
func Model(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	last := ""
	if n := len(req.Messages); n > 0 {
		last = req.Messages[n-1].Text()
	}
	return api.ModelResponse{Message: *api.TextMessage("assistant", "echo:"+last)}, nil
}
