package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/eventlog"
)

// legacyPrefix recognizes only the ID-less kinds persisted by v0.1.2. An execution ID or
// EXECUTION_START ends compatibility mode permanently; lifecycle markers remain session-scoped.
func legacyPrefix(events []api.Event) (int, error) {
	hasLegacy := false
	for i, event := range events {
		if event.Kind == api.EventExecutionStart || event.Kind != api.EventLifecycle && event.ExecutionID != "" {
			// An explicit start supersedes an abandoned old tail. Without that marker, an
			// unfinished old turn remains ambiguous (including a modern INPUT missing its ID).
			if hasLegacy && event.Kind != api.EventExecutionStart && !legacyBoundaryComplete(events[:i]) {
				return 0, fmt.Errorf("%w: unfinished legacy prefix before event %d (%s)",
					ErrInvalidExecutionLog, i+1, event.Kind)
			}
			if !hasLegacy {
				return 0, nil // lifecycle-only history does not establish a legacy invocation
			}
			return i, nil
		}
		switch event.Kind {
		case api.EventInput, api.EventModelCall, api.EventOutput, api.EventToolCall,
			api.EventToolResult, api.EventUsage, api.EventEnd, api.EventError:
			hasLegacy = true
		case api.EventLifecycle:
		default:
			return 0, fmt.Errorf("%w: event %d (%s) is not a legacy event",
				ErrInvalidExecutionLog, i+1, event.Kind)
		}
	}
	if !hasLegacy {
		return 0, nil
	}
	return len(events), nil
}

// Without a new start marker, only a terminal legacy END/ERROR establishes a clear boundary.
func legacyBoundaryComplete(events []api.Event) bool {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind != api.EventLifecycle {
			return events[i].Kind == api.EventEnd || events[i].Kind == api.EventError
		}
	}
	return true
}

// legacyReplayExecution preserves the old whole-log Run, rather than inferring turn identities
// from INPUT/END. In particular, a pure unfinished legacy log is still replayed.
func legacyReplayExecution(events []api.Event) recordedExecution {
	execution := recordedExecution{legacyEnd: len(events)}
	for _, event := range events {
		if event.Kind == api.EventInput && event.Message != nil {
			execution.inputs = append(execution.inputs, *event.Message)
		}
		if legacyEffect(event.Kind) {
			execution.stream = append(execution.stream, event)
		}
	}
	return execution
}

// legacyResumeExecution deliberately retains v0.1.2's single-last-INPUT recovery semantics.
// Config, resume cursor, and execution identity were not persisted and retain their defaults.
func legacyResumeExecution(events []api.Event) recordedExecution {
	execution := recordedExecution{start: -1, legacyEnd: len(events)}
	lastFinished := -1
	for i, event := range events {
		switch event.Kind {
		case api.EventInput:
			execution.start = i
		case api.EventEnd, api.EventError:
			lastFinished = i
		}
	}
	execution.completed = execution.start < 0 || lastFinished > execution.start
	if execution.completed {
		return execution
	}
	if message := events[execution.start].Message; message != nil {
		execution.inputs = append(execution.inputs, *message)
	}
	for _, event := range events[execution.start+1:] {
		if legacyEffect(event.Kind) {
			execution.stream = append(execution.stream, event)
		}
	}
	return execution
}

func legacyEffect(kind api.EventKind) bool {
	switch kind {
	case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult:
		return true
	default:
		return false
	}
}

// legacyExecutionID supplies a stable, session-scoped correlation token without assigning
// journal identity. Hash the original invocation record, not the evolving recovery tail or fence.
// Modern IDs are hexadecimal, so the legacy- namespace is disjoint from live controller IDs.
func (c *Controller) legacyExecutionID(record eventlog.Record) (string, error) {
	if c.sessionUID == "" {
		return "", fmt.Errorf("%w: legacy harness invocation", ErrMissingSessionUID)
	}
	content, err := canon.Event(record.Event)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		SessionUID string          `json:"session_uid"`
		Seq        string          `json:"seq"`
		Event      json.RawMessage `json:"event"`
	}{c.sessionUID, strconv.FormatInt(record.Seq, 10), content})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("legacy-%x", sha256.Sum256(payload)), nil
}

// append allows ID-less writes only on the sink constructed for legacy Resume. Ordinary live
// sinks still use appendSeq's nonempty-ID guard; no controller-wide compatibility state leaks
// into a later Exec. Streaming deltas also retain the empty legacy identity without journaling it.
func (s *liveSink) append(event api.Event) (eventlog.Record, error) {
	if !s.legacyRecovery {
		return s.c.appendSeq(s.executionID, event)
	}
	head, err := s.c.log.Head()
	if err != nil {
		return eventlog.Record{}, err
	}
	// Never persist the compatibility token, even if a caller supplied a wire event identity.
	event.ExecutionID = ""
	record, err := s.c.log.Append(head, s.c.fence, event)
	if err == nil {
		s.c.observe(record)
	}
	return record, err
}
