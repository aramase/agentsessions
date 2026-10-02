package controller

import (
	"bytes"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// recordedExecution is the replay projection of one Harness.Run. Its first event establishes the
// exact History boundary; every execution-scoped event carries id, so no content-based inference
// is needed.
type recordedExecution struct {
	id            string
	start         int
	inputs        []api.Message
	stream        []api.Event
	completed     bool
	config        []byte
	resumeFromSeq int64
	hasStart      bool
	inputCount    *int64
	inputRecords  int
}

// recordedExecutions groups execution-scoped events by ID in first-seen journal order. Lifecycle
// events are session-scoped and carry no ID.
func recordedExecutions(events []api.Event) ([]recordedExecution, error) {
	var executions []recordedExecution
	byID := make(map[string]int)

	for i, event := range events {
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
		case api.EventInput:
			execution.inputRecords++
			if event.Message != nil {
				execution.inputs = append(execution.inputs, *event.Message)
			}
		case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult, api.EventUsage:
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

func (e recordedExecution) validateInputs() error {
	if !e.hasStart {
		return nil // Markerless legacy logs have no recorded completeness information.
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
		return fmt.Errorf("%w: execution %q expected %d INPUT events, committed %d",
			ErrInvalidExecutionLog, e.id, *e.inputCount, e.inputRecords)
	}
	return nil
}
