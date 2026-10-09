package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/observability"
)

// ResumeInvocation returns a copy of the start marker for the pending execution Resume would
// select, so callers can resolve its recorded harness before routing. Completed/empty journals
// and legacy markerless turns return nil. Invalid invocation evidence fails closed as in Resume.
// This query is read-only; the caller must serialize routing and recovery with other session work.
func ResumeInvocation(log eventlog.Store) (*api.ExecutionStart, error) {
	recs, err := log.Read(1)
	if err != nil {
		return nil, err
	}
	events := make([]api.Event, 0, len(recs))
	for _, record := range recs {
		events = append(events, record.Event)
	}
	executions, err := recordedExecutions(events)
	if err != nil {
		return nil, err
	}
	execution, err := pendingResumeExecution(events, executions)
	if err != nil || execution == nil {
		return nil, err
	}
	return execution.invocation(), nil
}

// Resume re-drives an interrupted last execution (crash-recovery, I4). If the journal's last turn
// did not complete (no END), Resume re-runs the harness with a hybrid sink that SERVES the
// already-recorded effects — never re-invoking a model/tool call whose result is recorded
// (at-most-once, I3) — and switches to LIVE (invoke + record) for anything past the crash point,
// then appends END. A MODEL_CALL that is the last recorded effect, with no OUTPUT, is the crash
// point: when the harness re-issues it with the same input hash, the model is invoked once and its
// completion recorded against that call (see the model re-drive in resumeSink.Model). A legacy
// ID-less (v0.1.2) journal cut at that point is not re-driven: Resume fails with "recorded
// completion missing" and does not call the model. If the last turn is complete or the log is
// empty, it is a no-op (returns false); legacy ERROR also finishes a turn. A start marker with
// missing or inconsistent input completeness information is rejected before the harness runs.
// An unresolved tool intent inherited across a fork returns ErrInheritedToolIntent before running
// the harness or appending events: the child's dedup namespace cannot recover the parent's effect.
//
// Outside a legacy ID-less prefix, ERROR events, including those left by earlier failed Resume
// attempts, are not effects: they stay in the journal as an audit trail, are skipped when the
// effect stream is rebuilt, and do not end the execution, so a later Resume still recovers it.
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
	execution, err := pendingResumeExecution(events, executions)
	if err != nil || execution == nil {
		return false, err
	}
	legacy := execution.legacyEnd > 0
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
	// Reject identity mismatches before Run or its best-effort ERROR append path.
	if err := c.checkHarness(ctx, har, []recordedExecution{*execution}); err != nil {
		return false, err
	}
	invocationID := execution.id
	if legacy {
		// Select the original last INPUT, so retries retain identity while recovery remains pending.
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
	runErr := har.Run(ctx, start, sink)
	if sink.failure != nil {
		runErr = sink.failure
	}
	if runErr == nil && sink.i != len(sink.stream) {
		runErr = fmt.Errorf("%w: recovery consumed %d of %d recorded effects", ErrReplayDiverged, sink.i, len(sink.stream))
	}
	if runErr != nil {
		_, _ = sink.live.append(api.Event{Kind: api.EventError, Err: &api.Error{Description: runErr.Error()}})
		return true, runErr
	}
	_, err = sink.live.append(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	return true, err
}

// resumeSink serves already-recorded effects (in order) and, once they are exhausted, delegates to
// a liveSink to invoke-and-record the remainder. Serving never invokes the underlying op, so a
// completed effect is served without invocation; an unanswered trailing model intent (a MODEL_CALL
// with no recorded completion) may be re-invoked once per Resume (I3).
type resumeSink struct {
	live    liveSink
	stream  []api.Event
	i       int
	failure error // Rejecting a recorded tool prefix must never unlock the live path.
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
	if s.failure != nil {
		return api.ModelResponse{}, s.failure
	}
	if s.i < len(s.stream) {
		mc, ok := s.recordedNext(api.EventModelCall)
		if !ok {
			return api.ModelResponse{}, errors.New("resume: recorded stream diverged (expected model call)")
		}
		if mc.ModelCall.InputHash != hashModelInput(req) {
			return api.ModelResponse{}, fmt.Errorf("%w: resume: model input hash mismatch (I0)", ErrReplayDiverged)
		}
		out, ok := s.recordedNext(api.EventOutput)
		if !ok {
			if s.i != len(s.stream) {
				return api.ModelResponse{}, errors.New("resume: recorded completion missing")
			}
			// The journal's last effect is this MODEL_CALL with no completion: the cut fell
			// between recording the call and recording its OUTPUT, so whether the provider ran is
			// unknown. The input hash matched above, so invoke live and record the completion
			// after the existing MODEL_CALL rather than recording a second call. If the provider
			// fails again, the call stays unanswered exactly as a failed live call would.
			if s.live.legacyRecovery {
				// A v0.1.2 ID-less journal keeps the contract of rejecting a missing completion
				// without calling the model; only journals with execution IDs are re-driven.
				return api.ModelResponse{}, errors.New("resume: recorded completion missing")
			}
			return s.live.completeModel(ctx, req)
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
	if s.failure != nil {
		return s.failure
	}
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
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	// A keyless live rejection has no intent in the prefix. Reproduce it without consuming
	// another call's evidence, just as on replay.
	if tc.Mediation == api.MediationControllerMediated && tc.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
	}
	if s.i < len(s.stream) {
		// Preserve a distinct fallback's intent while reproducing a pre-intent rejection.
		next := s.stream[s.i]
		if next.Kind == api.EventToolCall && next.ToolCall != nil && next.ToolCall.ID != tc.ID {
			if err := toolMediationError(tc.Mediation); err != nil {
				return api.ToolResult{}, err
			}
		}
		call, ok := s.recordedNext(api.EventToolCall)
		if !ok {
			if err := toolMediationError(tc.Mediation); err != nil {
				return api.ToolResult{}, err
			}
			s.failure = fmt.Errorf("%w: resume expected a recorded tool call, found none", ErrReplayDiverged)
			return api.ToolResult{}, s.failure
		}
		if err := matchToolCall(tc, call.ToolCall); err != nil {
			s.failure = err
			return api.ToolResult{}, err
		}
		if tr, ok := s.recordedNext(api.EventToolResult); ok {
			result, err := recordedToolResult(call.ToolCall, tr.Result)
			if err != nil {
				s.failure = err
			}
			return result, err
		}
		// A following effect proves the harness continued after the unresolved call. Return a
		// bounded failure without repeating the uncertain effect or consuming its continuation.
		if s.i != len(s.stream) {
			return api.ToolResult{}, errors.New("controller: recorded tool call has no result")
		}
		// Only a terminal intent is re-driven under its recorded key. Executor errors are live
		// outcomes, not evidence failures: the harness may handle them and finish the turn.
		return s.live.execTool(ctx, *call.ToolCall)
	}
	return s.live.ToolCall(ctx, tc)
}

func (s *resumeSink) Report(ctx context.Context, tr api.ToolResult) error {
	if s.failure != nil {
		return s.failure
	}
	if s.i < len(s.stream) {
		if _, ok := s.recordedNext(api.EventToolResult); !ok {
			return errors.New("resume: recorded stream diverged (expected tool result)")
		}
		return nil
	}
	return s.live.Report(ctx, tr)
}

func (s *resumeSink) Usage(ctx context.Context, u api.Usage) error {
	if s.failure != nil {
		return s.failure
	}
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
