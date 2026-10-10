package approvalfixture

import (
	"context"
	"encoding/json"

	"github.com/aramase/agentsessions/api"
)

// Harness requests one host-owned effect. All deterministic inputs come from Start so a cold
// restart must reconstruct the original call, not a caller's replacement recovery parameters.
type Harness struct{ Memory bool }

func (h Harness) Describe(context.Context) (api.Descriptor, error) {
	resumability := api.ResumabilityStatelessReplay
	if h.Memory {
		resumability = api.ResumabilityRequiresMemorySnapshot
	}
	return api.Descriptor{ID: "approval-fixture", Version: "1", Capabilities: api.Capabilities{Resumability: resumability, ForkSafe: true}, Tools: []api.ToolSpec{{Name: "fixture-effect", Mediation: api.MediationRequiresApproval}}}, nil
}

func (Harness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	input := ""
	if len(start.Inputs) > 0 {
		input = start.Inputs[len(start.Inputs)-1].Text()
	}
	args := map[string]any{"input": input, "config": string(start.Config), "cursor": float64(start.ResumeFromSeq)}
	result, err := sink.ToolCall(ctx, api.ToolCall{ID: "fixture-call", Tool: "fixture-effect", IdempotencyKey: "shared-key", Mediation: api.MediationRequiresApproval, Args: args})
	if err != nil {
		return err
	}
	// Only durable receipt fields are needed; no Go-only transient Approval pointer is observed.
	output, err := json.Marshal(map[string]any{"input": input, "config": string(start.Config), "cursor": start.ResumeFromSeq, "code": result.Code, "request_seq": result.ApprovalRequestSeq, "decision_seq": result.ApprovalDecisionSeq, "receipt": result.Output})
	if err != nil {
		return err
	}
	return sink.Output(ctx, string(output))
}
