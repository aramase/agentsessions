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
// An owned unanswered approval returns (true, *api.ApprovalParkedError) without Describe or Run.
// A call-only cut repairs its request exactly once using the captured head and current fence;
// repair failure returns (true, operational error). A decided call resumes under the original key.
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
	scan, err := scanApprovalRecords(recs, 0)
	if err != nil {
		return false, err
	}
	execution, err := pendingResumeExecution(events, executions)
	if err != nil || execution == nil {
		return false, err
	}
	legacy := execution.legacyEnd > 0
	// Missing-request repair and pending queries do not need a harness, model, or executor.
	if state := scan.selected; state != nil && state.ExecutionID == execution.id && state.Receipt == nil {
		if state.Inherited {
			return false, ErrInheritedToolIntent
		}
		if state.Request == nil {
			record, err := c.log.Append(scan.head, c.fence, api.Event{ExecutionID: state.ExecutionID, Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: state.Call.ID}})
			if err != nil {
				return true, err
			}
			c.observe(record)
			state.Request = &record
		}
		if state.Decision == nil {
			return true, approvalPark(state)
		}
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
	// Reject identity mismatches before Run or its best-effort ERROR append path.
	desc, err := c.checkHarness(ctx, har, []recordedExecution{*execution})
	if err != nil {
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
		live:      liveSink{c: c, executionID: execution.id, legacyRecovery: legacy, guard: &sinkGuard{}, har: har, desc: desc, calls: make(map[string]bool)},
		stream:    execution.stream,
		approvals: approvalEvidence(scan, execution.id),
	}
	recordedEffectCount = len(execution.stream)
	start := &api.Start{
		ExecutionID:   invocationID,
		SessionUID:    c.sessionUID,
		Inputs:        execution.inputs,
		History:       events[:execution.start],
		Config:        execution.config,
		ResumeFromSeq: execution.resumeFromSeq,
	}
	runErr := har.Run(ctx, start, sink)
	stop := sink.live.guard.close()
	if stop != nil {
		if runErr == nil || !errors.Is(stop, api.ErrApprovalParked) {
			runErr = stop
		}
		return true, runErr // open approval intent cannot be followed by ERROR/END
	}
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
	live      liveSink
	stream    []api.Event
	i         int
	failure   error // Rejecting a recorded tool prefix must never unlock the live path.
	approvals map[string]*ApprovalState
}

var _ api.EventSink = (*resumeSink)(nil)

func (s *resumeSink) diverged(err error) error {
	s.failure = fmt.Errorf("%w: %w", ErrReplayDiverged, err)
	return s.failure
}

func (s *resumeSink) recordedNext(kind api.EventKind) (api.Event, bool) {
	if s.i >= len(s.stream) || s.stream[s.i].Kind != kind {
		return api.Event{}, false
	}
	ev := s.stream[s.i]
	s.i++
	return ev, true
}

func (s *resumeSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	s.live.guard.mu.Lock()
	defer s.live.guard.mu.Unlock()
	if err := s.live.guard.err(); err != nil {
		return api.ModelResponse{}, err
	}
	if s.failure != nil {
		return api.ModelResponse{}, s.failure
	}
	if s.i < len(s.stream) {
		mc, ok := s.recordedNext(api.EventModelCall)
		if !ok {
			return api.ModelResponse{}, s.diverged(errors.New("resume: recorded stream diverged (expected model call)"))
		}
		if mc.ModelCall == nil || mc.ModelCall.InputHash != hashModelInput(req) {
			return api.ModelResponse{}, s.diverged(errors.New("resume: model input hash mismatch (I0)"))
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
	return s.live.model(ctx, req) // past the crash point: first-ever execution
}

func (s *resumeSink) Output(ctx context.Context, delta string) error {
	s.live.guard.mu.Lock()
	defer s.live.guard.mu.Unlock()
	if err := s.live.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	if s.i < len(s.stream) {
		out, ok := s.recordedNext(api.EventOutput)
		if !ok {
			return s.diverged(errors.New("resume: recorded stream diverged (expected output)"))
		}
		recorded := ""
		if out.Message != nil {
			recorded = out.Message.Text()
		}
		if delta != recorded {
			return s.diverged(fmt.Errorf("resume: output mismatch — %q != recorded %q", delta, recorded))
		}
		return nil
	}
	return s.live.output(ctx, delta)
}

func (s *resumeSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	s.live.guard.mu.Lock()
	defer s.live.guard.mu.Unlock()
	if err := s.live.guard.err(); err != nil {
		return api.ToolResult{}, err
	}
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	// Preserve historical keyless ordinary rejections without consuming fallback evidence.
	// A same-ID gated intent must still validate identity before recovery can execute it.
	if tc.Mediation == api.MediationControllerMediated && tc.IdempotencyKey == "" {
		if s.i >= len(s.stream) || s.stream[s.i].Kind != api.EventToolCall || s.stream[s.i].ToolCall == nil ||
			s.stream[s.i].ToolCall.ID != tc.ID || s.stream[s.i].ToolCall.Mediation != api.MediationRequiresApproval {
			return api.ToolResult{}, ErrMissingIdempotencyKey
		}
	}
	if s.i < len(s.stream) {
		// Preserve a distinct fallback's intent while reproducing a pre-intent rejection.
		next := s.stream[s.i]
		if next.Kind == api.EventToolCall && next.ToolCall != nil && next.ToolCall.ID != tc.ID && next.ToolCall.Mediation != api.MediationRequiresApproval {
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
		s.live.calls[call.ToolCall.ID] = call.ToolCall.Mediation == api.MediationRequiresApproval
		if call.ToolCall.Mediation == api.MediationRequiresApproval {
			state := s.approvals[call.ToolCall.ID]
			if err := consumeApprovalPrefix(s.stream, &s.i, state); err != nil {
				s.failure = err
				return api.ToolResult{}, err
			}
			if state.Receipt == nil {
				return s.live.completeApproval(ctx, state)
			}
			tr, ok := s.recordedNext(api.EventToolResult)
			if !ok {
				s.failure = fmt.Errorf("%w: missing approval receipt", ErrReplayDiverged)
				return api.ToolResult{}, s.failure
			}
			result, err := serveApproval(state, tr.Result)
			if err != nil {
				s.failure = err
			}
			return result, err
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
	return s.live.toolCall(ctx, tc)
}

func (s *resumeSink) Report(ctx context.Context, tr api.ToolResult) error {
	s.live.guard.mu.Lock()
	defer s.live.guard.mu.Unlock()
	if err := s.live.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	if s.live.calls[tr.ID] {
		if s.i < len(s.stream) {
			return s.diverged(ErrApprovalReceiptReport)
		}
		s.live.guard.stop = ErrApprovalReceiptReport
		return s.live.guard.stop
	}
	if err := approvalReportError(tr); err != nil {
		return err
	}
	if s.i < len(s.stream) {
		if s.stream[s.i].Result != nil && approvalReportError(*s.stream[s.i].Result) != nil {
			s.failure = fmt.Errorf("%w: Report cannot consume approval receipt", ErrReplayDiverged)
			return s.failure
		}
		if _, ok := s.recordedNext(api.EventToolResult); !ok {
			return s.diverged(errors.New("resume: recorded stream diverged (expected tool result)"))
		}
		return nil
	}
	return s.live.report(ctx, tr)
}

func (s *resumeSink) Usage(ctx context.Context, u api.Usage) error {
	s.live.guard.mu.Lock()
	defer s.live.guard.mu.Unlock()
	if err := s.live.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	if s.live.legacyRecovery {
		if s.i < len(s.stream) {
			return nil // v0.1.2 did not serve usage; past the crash point it journals it live.
		}
		return s.live.usage(ctx, u)
	}
	if s.i < len(s.stream) {
		ev, ok := s.recordedNext(api.EventUsage)
		if !ok {
			return s.diverged(errors.New("resume: recorded stream diverged (expected usage)"))
		}
		if ev.Usage == nil {
			return s.diverged(errors.New("resume: recorded usage is missing its payload"))
		}
		if *ev.Usage != u {
			return s.diverged(fmt.Errorf("resume: usage mismatch — %+v != recorded %+v", u, *ev.Usage))
		}
		return nil
	}
	return s.live.usage(ctx, u)
}
