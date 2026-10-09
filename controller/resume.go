package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/observability"
)

// Resume re-drives an interrupted last execution (crash-recovery, I4). If the journal's last turn
// did not complete (no END), Resume re-runs the harness with a hybrid sink that SERVES the
// already-recorded effects — never re-invoking a recorded model/tool call (at-most-once, I3) — and
// switches to LIVE (invoke + record) for anything past the crash point, then appends END. If the
// last turn is complete or the log is empty, it is a no-op (returns false). A start marker with
// missing or inconsistent input completeness information is rejected before the harness runs.
// An unresolved tool intent inherited across a fork returns ErrInheritedToolIntent before running
// the harness or appending events: the child's dedup namespace cannot recover the parent's effect.
func (c *Controller) Resume(ctx context.Context, har api.Harness) (resumed bool, err error) {
	ctx = observability.EnsureRequestID(ctx)
	var recordCount, recordedEffectCount int
	finish := observability.StartDebug(ctx, c.logger, "controller", "resume", "session_uid", c.sessionUID)
	defer func() {
		finish(err,
			"error_kind", controllerErrorKind(err),
			"resumed", resumed,
			"record_count", recordCount,
			"recorded_effect_count", recordedEffectCount,
		)
	}()

	recs, err := c.log.Read(1)
	if err != nil {
		return false, err
	}
	recordCount = len(recs)
	if len(recs) == 0 {
		return false, nil
	}

	events := make([]api.Event, 0, len(recs))
	for _, record := range recs {
		events = append(events, record.Event)
	}
	executions, err := recordedExecutions(events)
	if err != nil {
		return false, err
	}
	if len(executions) == 0 {
		return false, nil
	}

	execution := executions[len(executions)-1]
	legacy := execution.legacyEnd > 0
	if legacy {
		execution = legacyResumeExecution(events[:execution.legacyEnd])
	}
	if execution.completed {
		return false, nil
	}
	if err := execution.validateInputs(); err != nil {
		return false, err
	}
	// An inherited intent belongs to the parent's dedup namespace, not this controller's UID.
	// Scan to the end: legacy child recovery may have recorded the result after the fork marker.
	// Check before running the harness so even a harness that handles errors cannot bypass it.
	unresolvedTools, inheritedTools := 0, 0
	var precedingCall *api.ToolCall
	precedingInherited := false
	for _, event := range events[execution.start:] {
		if event.Kind == api.EventLifecycle && event.Lifecycle != nil && event.Lifecycle.Kind == api.LifecycleFork {
			inheritedTools = unresolvedTools
			precedingInherited = precedingCall != nil
		}
		if event.ExecutionID != execution.id {
			continue
		}
		switch event.Kind {
		case api.EventToolCall:
			unresolvedTools++
			precedingCall = event.ToolCall
			precedingInherited = false
		case api.EventToolResult:
			if precedingCall != nil && event.Result != nil && event.Result.ID == precedingCall.ID {
				unresolvedTools--
				if precedingInherited {
					inheritedTools--
				}
			}
			precedingCall = nil
		case api.EventModelCall, api.EventOutput, api.EventUsage:
			precedingCall = nil
		}
	}
	if inheritedTools > 0 {
		return false, fmt.Errorf("%w: execution %q", ErrInheritedToolIntent, execution.id)
	}
	invocationID := execution.id
	if legacy {
		// Select the original last INPUT, so retries retain identity even after ERROR/live appends.
		invocationID, err = c.legacyExecutionID(recs[execution.start])
		if err != nil {
			return false, err
		}
	}
	sink := &resumeSink{
		live:   liveSink{c: c, executionID: execution.id, legacyRecovery: legacy},
		stream: execution.stream,
	}
	recordedEffectCount = len(execution.stream)
	start := &api.Start{
		ExecutionID:   invocationID,
		Inputs:        execution.inputs,
		History:       events[:execution.start],
		Config:        execution.config,
		ResumeFromSeq: execution.resumeFromSeq,
	}
	if err := har.Run(ctx, start, sink); err != nil {
		_, _ = sink.live.append(api.Event{Kind: api.EventError, Err: &api.Error{Description: err.Error()}})
		return true, err
	}
	_, err = sink.live.append(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	return true, err
}

// resumeSink serves already-recorded effects (in order) and, once they are exhausted, delegates to
// a liveSink to invoke-and-record the remainder. Serving never invokes the underlying op, so a
// recorded effect is executed at most once across a crash (I3).
type resumeSink struct {
	live   liveSink
	stream []api.Event
	i      int
}

var _ api.EventSink = (*resumeSink)(nil)

func (s *resumeSink) recordedNext(kind api.EventKind) (api.Event, bool) {
	if s.i >= len(s.stream) || s.stream[s.i].Kind != kind {
		return api.Event{}, false
	}
	ev := s.stream[s.i]
	s.i++
	return ev, true
}

func (s *resumeSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if s.i < len(s.stream) {
		mc, ok := s.recordedNext(api.EventModelCall)
		if !ok {
			return api.ModelResponse{}, errors.New("resume: recorded stream diverged (expected model call)")
		}
		if mc.ModelCall.InputHash != hashModelInput(req) {
			return api.ModelResponse{}, errors.New("resume: model input hash mismatch (I0)")
		}
		out, ok := s.recordedNext(api.EventOutput)
		if !ok {
			return api.ModelResponse{}, errors.New("resume: recorded completion missing")
		}
		var msg api.Message
		if out.Message != nil {
			msg = *out.Message
		}
		return api.ModelResponse{Message: msg}, nil // served — model NOT re-invoked
	}
	return s.live.Model(ctx, req) // past the crash point: first-ever execution
}

func (s *resumeSink) Output(ctx context.Context, delta string) error {
	if s.i < len(s.stream) {
		out, ok := s.recordedNext(api.EventOutput)
		if !ok {
			return errors.New("resume: recorded stream diverged (expected output)")
		}
		recorded := ""
		if out.Message != nil {
			recorded = out.Message.Text()
		}
		if delta != recorded {
			return fmt.Errorf("resume: output mismatch — %q != recorded %q", delta, recorded)
		}
		return nil
	}
	return s.live.Output(ctx, delta)
}

func (s *resumeSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	if s.i < len(s.stream) {
		call, ok := s.recordedNext(api.EventToolCall)
		if !ok {
			return api.ToolResult{}, errors.New("resume: recorded stream diverged (expected tool call)")
		}
		if tr, ok := s.recordedNext(api.EventToolResult); ok {
			// Intent AND result recorded: served, the tool is NOT re-executed (at-most-once, I3).
			if tr.Result == nil {
				return api.ToolResult{}, nil
			}
			return *tr.Result, nil
		}
		// Intent recorded but no result: the crash fell between execute and result-append. Re-drive
		// the effect under the SAME recorded idempotency key (§3) — an idempotent tool dedups it —
		// and write-ahead the result. This closes the tool half of I3, symmetric to the model
		// re-drive above.
		if call.ToolCall == nil {
			return api.ToolResult{}, errors.New("resume: recorded tool call missing its payload")
		}
		return s.live.execTool(ctx, *call.ToolCall)
	}
	return s.live.ToolCall(ctx, tc)
}

func (s *resumeSink) Report(ctx context.Context, tr api.ToolResult) error {
	if s.i < len(s.stream) {
		if _, ok := s.recordedNext(api.EventToolResult); !ok {
			return errors.New("resume: recorded stream diverged (expected tool result)")
		}
		return nil
	}
	return s.live.Report(ctx, tr)
}

func (s *resumeSink) Usage(ctx context.Context, u api.Usage) error {
	if s.live.legacyRecovery {
		if s.i < len(s.stream) {
			return nil // v0.1.2 did not serve usage; past the crash point it journals it live.
		}
		return s.live.Usage(ctx, u)
	}
	if s.i < len(s.stream) {
		ev, ok := s.recordedNext(api.EventUsage)
		if !ok {
			return errors.New("resume: recorded stream diverged (expected usage)")
		}
		if ev.Usage == nil {
			return errors.New("resume: recorded usage is missing its payload")
		}
		if *ev.Usage != u {
			return fmt.Errorf("resume: usage mismatch — %+v != recorded %+v", u, *ev.Usage)
		}
		return nil
	}
	return s.live.Usage(ctx, u)
}
