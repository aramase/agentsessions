package controller

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/wire"
)

// validateLiveToolCall returns the same argument representation the journal records, so direct
// Go harnesses cannot give an executor values that differ from a later recovered call.
func validateLiveToolCall(call api.ToolCall) (api.ToolCall, error) {
	switch call.Mediation {
	case api.MediationControllerMediated:
	case api.MediationRequiresApproval:
		return api.ToolCall{}, errors.New("controller: REQUIRES_APPROVAL mediation is not yet implemented")
	default:
		return api.ToolCall{}, fmt.Errorf("%w (got %q)", ErrUnmediatedToolCall, call.Mediation)
	}
	if call.IdempotencyKey == "" {
		return api.ToolCall{}, ErrMissingIdempotencyKey
	}
	args, err := wire.NormalizeToolArgs(call.Args)
	if err != nil {
		return api.ToolCall{}, err
	}
	call.Args = args
	return call, nil
}

// matchToolCall compares only the observable request, not raw Go numeric types or map ordering.
// Mismatch errors name the field without exposing arguments or idempotency keys to the journal.
func matchToolCall(emitted api.ToolCall, recorded *api.ToolCall) error {
	if recorded == nil {
		return fmt.Errorf("%w: recorded tool call missing its payload", ErrReplayDiverged)
	}
	switch {
	case emitted.ID != recorded.ID:
		return fmt.Errorf("%w: tool call ID mismatch", ErrReplayDiverged)
	case emitted.Tool != recorded.Tool:
		return fmt.Errorf("%w: tool name mismatch", ErrReplayDiverged)
	case emitted.Mediation != recorded.Mediation:
		return fmt.Errorf("%w: tool mediation mismatch", ErrReplayDiverged)
	case emitted.IdempotencyKey != recorded.IdempotencyKey:
		return fmt.Errorf("%w: tool idempotency key mismatch", ErrReplayDiverged)
	}
	want, err := wire.NormalizeToolArgs(recorded.Args)
	if err != nil {
		return fmt.Errorf("%w: invalid recorded tool arguments", ErrReplayDiverged)
	}
	got, err := wire.NormalizeToolArgs(emitted.Args)
	if err != nil {
		return fmt.Errorf("%w: invalid emitted tool arguments", ErrReplayDiverged)
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%w: tool arguments mismatch", ErrReplayDiverged)
	}
	return nil
}

func recordedToolResult(call *api.ToolCall, result *api.ToolResult) (api.ToolResult, error) {
	if result == nil {
		return api.ToolResult{}, fmt.Errorf("%w: recorded tool result missing its payload", ErrReplayDiverged)
	}
	if result.ID != call.ID {
		return api.ToolResult{}, fmt.Errorf("%w: recorded tool result correlation mismatch", ErrReplayDiverged)
	}
	return *result, nil
}
