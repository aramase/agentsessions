package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// ErrApprovalBlocked rejects a new turn while this session owns an approval intent without receipt.
var ErrApprovalBlocked = errors.New("controller: unresolved approval blocks a new execution")

// ErrApprovalUnavailable rejects gating on a harness that cannot reconstruct its Run by replay.
var ErrApprovalUnavailable = errors.New("controller: approval requires STATELESS_REPLAY")

// ErrInvalidApprovalCall rejects missing or reused gated call identities before intent is written.
var ErrInvalidApprovalCall = errors.New("controller: invalid approval call identity")

// ErrApprovalReceiptReport rejects in-harness reports carrying host-owned approval receipt fields.
var ErrApprovalReceiptReport = errors.New("controller: approval receipts are host owned")

// ErrMissingToolExecutor is a recoverable host precondition, not an executor-failure receipt.
var ErrMissingToolExecutor = errors.New("controller: no tool executor configured")

func approvalPark(state *ApprovalState) error {
	return &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: state.ExecutionID, ToolCallID: state.Call.ID, RequestSeq: state.Request.Seq}}
}

func approvalReportError(result api.ToolResult) error {
	if result.Approval != nil || result.ApprovalRequestSeq != 0 || result.ApprovalDecisionSeq != 0 || result.Code == api.ToolResultCodeApprovalDenied {
		return ErrApprovalReceiptReport
	}
	return nil
}

// approvalDescriptor is called only on the gate path. Ordinary identity-less histories must not
// acquire a new Describe dependency; modern invocations reuse their preflight descriptor.
func (s *liveSink) approvalDescriptor(ctx context.Context) error {
	if s.desc == nil {
		desc, err := describeHarness(ctx, s.har)
		if err != nil {
			return err
		}
		s.desc = &desc
	}
	if s.desc.Capabilities.Resumability != api.ResumabilityStatelessReplay {
		return ErrApprovalUnavailable
	}
	return nil
}

func (s *liveSink) parkTool(ctx context.Context, call api.ToolCall) (api.ToolResult, error) {
	if err := s.approvalDescriptor(ctx); err != nil {
		return api.ToolResult{}, err
	}
	if s.c.sessionUID == "" {
		return api.ToolResult{}, ErrMissingSessionUID
	}
	if s.legacyRecovery || call.ID == "" {
		return api.ToolResult{}, ErrInvalidApprovalCall
	}
	if call.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
	}
	if _, exists := s.calls[call.ID]; exists {
		return api.ToolResult{}, ErrInvalidApprovalCall
	}
	if _, err := s.append(api.Event{Kind: api.EventToolCall, ToolCall: &call}); err != nil {
		return api.ToolResult{}, err
	}
	s.calls[call.ID] = true
	// After intent commits, any failure must stop continuation: a handled append error must not
	// leave fallback effects or END beyond an open approval intent.
	req, err := s.append(api.Event{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: call.ID}})
	if err != nil {
		s.guard.stop = err
		return api.ToolResult{}, err
	}
	s.guard.stop = &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: s.executionID, ToolCallID: call.ID, RequestSeq: req.Seq}}
	return api.ToolResult{}, s.guard.stop
}

// completeApproval executes only a scanned, committed decision. The executor gets the original
// session/key namespace; after an uncertain receipt write it must durably deduplicate.
func (s *liveSink) completeApproval(ctx context.Context, state *ApprovalState) (api.ToolResult, error) {
	if err := s.approvalDescriptor(ctx); err != nil {
		s.guard.stop = err
		return api.ToolResult{}, err
	}
	if s.c.sessionUID == "" {
		s.guard.stop = ErrMissingSessionUID
		return api.ToolResult{}, s.guard.stop
	}
	decision := state.Decision.Event.ApprovalResult
	var result api.ToolResult
	if !decision.Approved {
		const denial = "Tool execution denied by approval decision."
		result = api.ToolResult{IsError: true, Code: api.ToolResultCodeApprovalDenied, Error: denial, Content: api.TextMessage("tool", denial).Parts}
	} else {
		if s.c.tool == nil {
			s.guard.stop = ErrMissingToolExecutor
			return api.ToolResult{}, s.guard.stop
		}
		res, err := s.c.tool(ctx, ToolCallContext{SessionUID: s.c.sessionUID}, *state.Call)
		s.c.liveToolCalls++
		result = res
		if err != nil {
			result.IsError = true
			result.Error = err.Error()
		}
		result.Code = api.ToolResultCodeUnspecified
		if result.IsError {
			result.Code = api.ToolResultCodeExecutorError
		}
	}
	result.ID = state.Call.ID
	result.ApprovalRequestSeq = state.Request.Seq
	result.ApprovalDecisionSeq = state.Decision.Seq
	result.Approval = nil
	receipt := result // Append may retain this pointer; transient reply decoration must stay separate.
	if _, err := s.append(api.Event{Kind: api.EventToolResult, Result: &receipt}); err != nil {
		s.guard.stop = err
		return api.ToolResult{}, err
	}
	// A decision is transient reply data only after its receipt commits. Return a copy so harness
	// code cannot change the authority kept in scanned evidence.
	approval := *decision
	result.Approval = &approval
	return result, nil
}

func approvalEvidence(scan *approvalScan, executionID string) map[string]*ApprovalState {
	states := make(map[string]*ApprovalState)
	for _, state := range scan.states {
		if state.ExecutionID == executionID {
			states[state.Call.ID] = state
		}
	}
	return states
}

// consumeApprovalPrefix uses the scanned record envelopes for authority; stream position only
// establishes deterministic ordering. Pending gates are intercepted by Resume before Run.
func consumeApprovalPrefix(stream []api.Event, index *int, state *ApprovalState) error {
	if state == nil || state.Request == nil || state.Decision == nil {
		return fmt.Errorf("%w: missing approval decision evidence", ErrReplayDiverged)
	}
	for _, kind := range []api.EventKind{api.EventApprovalRequest, api.EventApprovalResult} {
		if *index >= len(stream) || stream[*index].Kind != kind {
			return fmt.Errorf("%w: missing recorded %s", ErrReplayDiverged, kind)
		}
		(*index)++
	}
	return nil
}

func serveApproval(state *ApprovalState, receipt *api.ToolResult) (api.ToolResult, error) {
	if state == nil || state.Request == nil || state.Decision == nil || state.Receipt == nil {
		return api.ToolResult{}, fmt.Errorf("%w: incomplete approval receipt evidence", ErrReplayDiverged)
	}
	result, err := recordedToolResult(state.Call, receipt)
	if err != nil {
		return api.ToolResult{}, err
	}
	if result.ApprovalRequestSeq != state.Request.Seq || result.ApprovalDecisionSeq != state.Decision.Seq {
		return api.ToolResult{}, fmt.Errorf("%w: approval receipt sequence mismatch", ErrReplayDiverged)
	}
	if err := validateApprovalReceipt(state.Decision.Event.ApprovalResult, &result); err != nil {
		return api.ToolResult{}, err
	}
	decision := *state.Decision.Event.ApprovalResult
	result.Approval = &decision
	return result, nil
}
