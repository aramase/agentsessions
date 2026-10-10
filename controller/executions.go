package controller

import (
	"bytes"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// recordedExecution is the replay projection of one modern Harness.Run, or the whole legacy
// prefix. Modern identities establish exact turn boundaries; legacyEnd retains the old whole-log
// boundary without inventing an identity or inferring multiple invocations.
type recordedExecution struct {
	id             string
	start          int
	inputs         []api.Message
	stream         []api.Event
	completed      bool
	config         []byte
	resumeFromSeq  int64
	hasStart       bool
	inputCount     *int64
	inputRecords   int
	harness        string
	harnessVersion string
	legacyEnd      int
}

// recordedExecutions validates identities and completed modern invocations before any harness
// runs. It preserves an ID-less legacy prefix, then groups modern events by ID in first-seen order.
// Lifecycle is session-scoped.
func recordedExecutions(events []api.Event) ([]recordedExecution, error) {
	prefixEnd, err := legacyPrefix(events)
	if err != nil {
		return nil, err
	}
	var executions []recordedExecution
	if prefixEnd > 0 {
		replayEnd := prefixEnd
		if prefixEnd < len(events) {
			// A new modern turn abandons an incomplete/failed legacy tail. Replay only the
			// last successful whole-log prefix; never infer separate legacy invocations.
			// Modern Start.History still uses the original, untrimmed events.
			replayEnd = 0
			for i, event := range events[:prefixEnd] {
				if event.Kind == api.EventEnd {
					replayEnd = i + 1
				}
			}
		}
		if replayEnd > 0 {
			executions = append(executions, legacyReplayExecution(events[:replayEnd]))
		}
	}
	byID := make(map[string]int)

	for i := prefixEnd; i < len(events); i++ {
		event := events[i]
		if event.Kind == api.EventLifecycle {
			continue
		}
		if event.ExecutionID == "" {
			return nil, fmt.Errorf("%w: event %d (%s) has no execution_id",
				ErrInvalidExecutionLog, i+1, event.Kind)
		}

		executionIndex, exists := byID[event.ExecutionID]
		if !exists {
			executionIndex = len(executions)
			byID[event.ExecutionID] = executionIndex
			executions = append(executions, recordedExecution{
				id:    event.ExecutionID,
				start: i,
			})
		}

		execution := &executions[executionIndex]
		switch event.Kind {
		case api.EventExecutionStart:
			if execution.start != i || event.ExecutionStart == nil {
				return nil, fmt.Errorf("%w: execution %q has an invalid start event", ErrInvalidExecutionLog, execution.id)
			}
			execution.config = bytes.Clone(event.ExecutionStart.Config)
			execution.resumeFromSeq = event.ExecutionStart.ResumeFromSeq
			execution.hasStart = true
			execution.inputCount = event.ExecutionStart.InputCount
			execution.harness = event.ExecutionStart.Harness
			execution.harnessVersion = event.ExecutionStart.HarnessVersion
		case api.EventInput:
			execution.inputRecords++
			if event.Message != nil {
				execution.inputs = append(execution.inputs, *event.Message)
			}
		case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult, api.EventUsage, api.EventApprovalRequest, api.EventApprovalResult:
			execution.stream = append(execution.stream, event)
		case api.EventEnd:
			execution.completed = true
		}
	}

	// Validate every completed invocation before any harness runs. Incomplete invocations are
	// skipped by Replay; Resume validates its selected invocation separately.
	for _, execution := range executions {
		if execution.completed {
			if err := execution.validateInputs(); err != nil {
				return nil, err
			}
		}
	}
	return executions, nil
}

// pendingExecution selects the trailing replay projection. Journal order is first-seen
// execution order, not the execution ID on the final record.
func pendingExecution(executions []recordedExecution) (*recordedExecution, error) {
	if len(executions) == 0 || executions[len(executions)-1].completed {
		return nil, nil
	}
	execution := &executions[len(executions)-1]
	if err := execution.validateInputs(); err != nil {
		return nil, err
	}
	return execution, nil
}

// pendingResumeExecution is shared by routing and recovery: a whole-log legacy replay
// projection must be replaced by v0.1.2's last-INPUT recovery projection before either selects
// an invocation. Legacy END/ERROR may make that replacement a completed no-op.
func pendingResumeExecution(events []api.Event, executions []recordedExecution) (*recordedExecution, error) {
	execution, err := pendingExecution(executions)
	if err != nil || execution == nil {
		return nil, err
	}
	if execution.legacyEnd > 0 {
		legacy := legacyResumeExecution(events[:execution.legacyEnd])
		if legacy.completed {
			return nil, nil
		}
		execution = &legacy
	}
	return execution, nil
}

func (e recordedExecution) hasApproval() bool {
	for _, ev := range e.stream {
		if ev.Kind == api.EventToolCall && ev.ToolCall != nil && ev.ToolCall.Mediation == api.MediationRequiresApproval {
			return true
		}
	}
	return false
}

func (e recordedExecution) invocation() *api.ExecutionStart {
	if !e.hasStart {
		return nil
	}
	var count *int64
	if e.inputCount != nil {
		value := *e.inputCount
		count = &value
	}
	return &api.ExecutionStart{
		Config: bytes.Clone(e.config), ResumeFromSeq: e.resumeFromSeq, InputCount: count,
		Harness: e.harness, HarnessVersion: e.harnessVersion,
	}
}

func (e recordedExecution) validateInputs() error {
	if !e.hasStart {
		// Only older writers omit the start marker; their logs have no completeness count.
		return nil
	}
	if e.inputCount == nil {
		return fmt.Errorf("%w: execution %q start has no input_count", ErrInvalidExecutionLog, e.id)
	}
	if *e.inputCount < 0 {
		return fmt.Errorf("%w: execution %q has negative input_count %d", ErrInvalidExecutionLog, e.id, *e.inputCount)
	}
	if e.inputRecords != len(e.inputs) {
		return fmt.Errorf("%w: execution %q has an INPUT without a message", ErrInvalidExecutionLog, e.id)
	}
	if int64(e.inputRecords) != *e.inputCount {
		errKind := ErrInvalidExecutionLog
		if !e.completed && int64(e.inputRecords) < *e.inputCount {
			errKind = ErrIncompleteInvocation
		}
		return fmt.Errorf("%w: execution %q expected %d INPUT events, committed %d",
			errKind, e.id, *e.inputCount, e.inputRecords)
	}
	return nil
}
