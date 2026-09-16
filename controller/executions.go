package controller

import (
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// recordedExecution is the replay projection of one Harness.Run. Its first event establishes the
// exact History boundary; every execution-scoped event carries id, so no content-based inference
// is needed.
type recordedExecution struct {
	id        string
	start     int
	inputs    []api.Message
	stream    []api.Event
	completed bool
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
		case api.EventInput:
			if event.Message != nil {
				execution.inputs = append(execution.inputs, *event.Message)
			}
		case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult:
			execution.stream = append(execution.stream, event)
		case api.EventEnd:
			execution.completed = true
		}
	}

	return executions, nil
}
