package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrInvalidApprovalDecision identifies a missing, wrong, superseded, inherited, or changed
// decision tuple. It is distinct from corrupt recorded evidence and operational store failures.
var ErrInvalidApprovalDecision = errors.New("controller: invalid approval decision")

// ApprovalState is derived exclusively from records in one session's journal. Request is nil
// for a cut after the call but before the host request; that cut is not addressable by Approve.
// Completed means the execution has an END; Receipt independently records call completion.
// Inherited identifies copied evidence even when a later matching receipt completes the call.
type ApprovalState struct {
	ExecutionID                string
	Invocation                 *api.ExecutionStart
	Call                       *api.ToolCall
	Request, Decision, Receipt *eventlog.Record
	Head                       int64
	Inherited, Completed       bool
}

// InspectApproval selects the latest execution by first-seen non-lifecycle identity, then its
// last gated call. A nongated latest execution returns nil. Zero reads the current journal;
// positive throughSeq bounds both selection and validation to that captured query prefix.
// Inspection does not fence, invoke a harness, or validate unrelated ordinary invocations.
// Callers must serialize inspection/decision with other session operations.
func InspectApproval(log eventlog.Store, throughSeq int64) (*ApprovalState, error) {
	scan, err := scanApprovals(log, throughSeq)
	if err != nil {
		return nil, err
	}
	return scan.selected, nil
}

// Approve commits only a decision, never an effect or a compute transition. Validation and
// exact historical retries happen before NewFence, so neither invalid input nor retries evict
// an existing writer. A new decision uses the head from that same validated scan for append CAS.
// Callers must serialize this operation with other session work; CAS also protects against
// external writers advancing the journal between validation and append.
func Approve(log eventlog.Store, decision api.ApprovalDecision) (eventlog.Record, error) {
	if decision.ExecutionID == "" || decision.ToolCallID == "" || decision.RequestSeq <= 0 {
		return eventlog.Record{}, fmt.Errorf("%w: execution, call and positive request sequence are required", ErrInvalidApprovalDecision)
	}
	scan, err := scanApprovals(log, 0)
	if err != nil {
		return eventlog.Record{}, err
	}
	var target *ApprovalState
	for _, state := range scan.states {
		if state.ExecutionID == decision.ExecutionID && state.Call.ID == decision.ToolCallID && state.Request != nil && state.Request.Seq == decision.RequestSeq {
			target = state
			break
		}
	}
	if target == nil || target.Inherited {
		return eventlog.Record{}, fmt.Errorf("%w: request is not owned by this session", ErrInvalidApprovalDecision)
	}
	if target.Decision != nil {
		old := target.Decision.Event.ApprovalResult
		// IdentityRef is a value: omitted and explicitly all-empty identity normalize alike.
		if old.Approved != decision.Approved || old.Reason != decision.Reason || target.Decision.Event.Actor != decision.Identity {
			return eventlog.Record{}, fmt.Errorf("%w: request already has a different decision", ErrInvalidApprovalDecision)
		}
		return *target.Decision, nil
	}
	if target != scan.selected || target.Completed || target.Receipt != nil {
		return eventlog.Record{}, fmt.Errorf("%w: request is superseded or completed", ErrInvalidApprovalDecision)
	}
	fence, err := log.NewFence()
	if err != nil {
		return eventlog.Record{}, err
	}
	return log.Append(scan.head, fence, api.Event{
		ExecutionID: decision.ExecutionID, Timestamp: time.Now().UTC(), Kind: api.EventApprovalResult,
		Actor: decision.Identity,
		ApprovalResult: &api.ApprovalResult{
			ToolCallID: decision.ToolCallID, RequestSeq: decision.RequestSeq,
			Approved: decision.Approved, Reason: decision.Reason,
		},
	})
}

// approvalScan retains every gated call, not just the projection, to support exact retries
// after a receipt or a newer execution. It is local to one read, never cached mutable authority.
type approvalScan struct {
	head     int64
	states   []*ApprovalState
	selected *ApprovalState
}

type approvalExecution struct {
	id      string
	records []eventlog.Record
	gated   bool
}

func scanApprovals(log eventlog.Store, throughSeq int64) (*approvalScan, error) {
	if throughSeq < 0 {
		return nil, fmt.Errorf("%w: negative approval cursor", ErrInvalidExecutionLog)
	}
	records, err := log.Read(1)
	if err != nil {
		return nil, err
	}
	return scanApprovalRecords(records, throughSeq)
}

func scanApprovalRecords(records []eventlog.Record, throughSeq int64) (*approvalScan, error) {
	scan := &approvalScan{}
	var executions []approvalExecution
	byID := make(map[string]int)
	var forkSeq int64
	for _, record := range records {
		if throughSeq > 0 && record.Seq > throughSeq {
			break
		}
		scan.head = record.Seq
		event := record.Event
		if event.Kind == api.EventLifecycle {
			if event.Lifecycle != nil && event.Lifecycle.Kind == api.LifecycleFork {
				forkSeq = record.Seq
			}
			continue
		}
		index, exists := byID[event.ExecutionID]
		if !exists {
			index = len(executions)
			byID[event.ExecutionID] = index
			executions = append(executions, approvalExecution{id: event.ExecutionID})
		}
		execution := &executions[index]
		execution.records = append(execution.records, record)
		if event.Kind == api.EventApprovalRequest || event.Kind == api.EventApprovalResult ||
			event.Kind == api.EventToolCall && event.ToolCall != nil && event.ToolCall.Mediation == api.MediationRequiresApproval ||
			event.Kind == api.EventToolResult && event.Result != nil && (event.Result.ApprovalRequestSeq != 0 || event.Result.ApprovalDecisionSeq != 0) {
			execution.gated = true
		}
	}
	for i, execution := range executions {
		if !execution.gated {
			continue
		}
		states, err := inspectApprovalExecution(execution, scan.head, forkSeq)
		if err != nil {
			return nil, err
		}
		scan.states = append(scan.states, states...)
		if i == len(executions)-1 && len(states) > 0 {
			scan.selected = states[len(states)-1]
		}
	}
	return scan, nil
}

func inspectApprovalExecution(execution approvalExecution, head, forkSeq int64) ([]*ApprovalState, error) {
	if execution.id == "" {
		return nil, fmt.Errorf("%w: approval evidence has no execution_id", ErrInvalidExecutionLog)
	}
	invocation := recordedExecution{id: execution.id}
	var states []*ApprovalState
	var last *ApprovalState
	calls := make(map[string]bool) // true for a gated call; ordinary calls cannot reuse its identity
	for i := range execution.records {
		record := &execution.records[i]
		event := record.Event
		open := last != nil && last.Receipt == nil
		switch event.Kind {
		case api.EventExecutionStart:
			if i != 0 || event.ExecutionStart == nil {
				return nil, fmt.Errorf("%w: invalid approval execution start", ErrInvalidExecutionLog)
			}
			start := event.ExecutionStart
			invocation.hasStart = true
			invocation.config, invocation.resumeFromSeq, invocation.inputCount = start.Config, start.ResumeFromSeq, start.InputCount
			invocation.harness, invocation.harnessVersion = start.Harness, start.HarnessVersion
		case api.EventInput:
			if open {
				return nil, approvalContinuationError(execution.id)
			}
			invocation.inputRecords++
			if event.Message != nil {
				invocation.inputs = append(invocation.inputs, *event.Message)
			}
		case api.EventToolCall:
			if open {
				return nil, approvalContinuationError(execution.id)
			}
			if event.ToolCall == nil {
				return nil, fmt.Errorf("%w: missing tool call payload in approval execution", ErrInvalidExecutionLog)
			}
			call := event.ToolCall
			gated := call.Mediation == api.MediationRequiresApproval
			if priorGated, exists := calls[call.ID]; exists && (priorGated || gated) {
				return nil, fmt.Errorf("%w: duplicate approval call identity", ErrInvalidExecutionLog)
			}
			calls[call.ID] = gated
			if !gated {
				continue
			}
			if invocation.completed {
				return nil, approvalContinuationError(execution.id)
			}
			if call.ID == "" {
				return nil, fmt.Errorf("%w: approval call has no ID", ErrInvalidExecutionLog)
			}
			if call.IdempotencyKey == "" {
				return nil, fmt.Errorf("%w: approval call: %w", ErrInvalidExecutionLog, ErrMissingIdempotencyKey)
			}
			last = &ApprovalState{ExecutionID: execution.id, Call: call, Head: head, Inherited: record.Seq < forkSeq}
			states = append(states, last)
		case api.EventApprovalRequest:
			if event.Approval == nil || last == nil || !open || last.Request != nil {
				return nil, fmt.Errorf("%w: missing or duplicate approval request intent", ErrInvalidExecutionLog)
			}
			if event.Approval.ToolCallID != last.Call.ID {
				return nil, fmt.Errorf("%w: approval request call mismatch", ErrReplayDiverged)
			}
			last.Request = record
		case api.EventApprovalResult:
			if event.ApprovalResult == nil || last == nil || !open || last.Request == nil || last.Decision != nil {
				return nil, fmt.Errorf("%w: missing or duplicate approval decision request", ErrInvalidExecutionLog)
			}
			decision := event.ApprovalResult
			if decision.ToolCallID != last.Call.ID || decision.RequestSeq != last.Request.Seq {
				return nil, fmt.Errorf("%w: approval decision tuple mismatch", ErrReplayDiverged)
			}
			last.Decision = record
		case api.EventToolResult:
			if open {
				if event.Result == nil || last.Request == nil || last.Decision == nil {
					return nil, fmt.Errorf("%w: approval receipt lacks request or decision", ErrInvalidExecutionLog)
				}
				result := event.Result
				if result.ID != last.Call.ID || result.ApprovalRequestSeq != last.Request.Seq || result.ApprovalDecisionSeq != last.Decision.Seq {
					return nil, fmt.Errorf("%w: approval receipt tuple mismatch", ErrReplayDiverged)
				}
				if err := validateApprovalReceipt(last.Decision.Event.ApprovalResult, result); err != nil {
					return nil, err
				}
				last.Receipt = record
			} else if event.Result != nil && (calls[event.Result.ID] || event.Result.ApprovalRequestSeq != 0 || event.Result.ApprovalDecisionSeq != 0) {
				return nil, fmt.Errorf("%w: duplicate or unbound approval receipt", ErrInvalidExecutionLog)
			}
		case api.EventEnd:
			if open {
				return nil, approvalContinuationError(execution.id)
			}
			invocation.completed = true
		case api.EventError:
			// An error is an audit trail, not a decision, receipt, or modern turn boundary.
		default:
			if open {
				return nil, approvalContinuationError(execution.id)
			}
		}
	}
	// Reuse invocation completeness rules only for approval-bearing executions. Checking for
	// approvals must not remove ordinary incomplete/malformed histories' new-Exec escape path.
	if err := invocation.validateInputs(); err != nil {
		return nil, err
	}
	for _, state := range states {
		state.Invocation = invocation.invocation()
		state.Completed = invocation.completed
	}
	return states, nil
}

func approvalContinuationError(executionID string) error {
	return fmt.Errorf("%w: execution %q continued past an unresolved approval call", ErrInvalidExecutionLog, executionID)
}

func validateApprovalReceipt(decision *api.ApprovalResult, result *api.ToolResult) error {
	valid := result.IsError && result.Code == api.ToolResultCodeApprovalDenied
	if decision.Approved {
		valid = !result.IsError && result.Code == api.ToolResultCodeUnspecified || result.IsError && result.Code == api.ToolResultCodeExecutorError
	}
	if !valid {
		return fmt.Errorf("%w: receipt status contradicts approval decision", ErrReplayDiverged)
	}
	return nil
}
