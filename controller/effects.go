package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// liveSink is the host-mediated EventSink for a live turn: it invokes each nondeterministic op and
// records it to the log, so the identical events can be served on a later replay.
type liveSink struct {
	c              *Controller
	executionID    string
	legacyRecovery bool
	guard          *sinkGuard
	desc           *api.Descriptor
	har            api.Harness
	calls          map[string]bool // true for approval; ordinary IDs cannot later be reused by a gate
}

var _ api.EventSink = (*liveSink)(nil)

// Model records the request (EVENT_MODEL_CALL with the input hash), invokes the model, records the
// completion as an EVENT_OUTPUT, and returns it. On replay the recorded completion is served in
// place of the invocation (see replaySink) — the harness code path is identical either way.
//
// Footgun: the completion is ALREADY recorded as the output here. A harness that also calls
// Output() with the same content double-records it; use Output() only for additional/streamed text.
func (s *liveSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return api.ModelResponse{}, err
	}
	return s.model(ctx, req)
}

func (s *liveSink) model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if _, err := s.append(api.Event{
		Kind:      api.EventModelCall,
		ModelCall: &api.ModelCall{Model: req.Model, InputHash: hashModelInput(req), ID: newID()},
	}); err != nil {
		return api.ModelResponse{}, err
	}
	return s.completeModel(ctx, req)
}

// completeModel invokes the model for a MODEL_CALL that is already recorded and appends the
// completion as the OUTPUT that answers it: OUTPUT carries no call ID, so its position directly
// after the MODEL_CALL in the execution's effect stream is the correlation. It is reused verbatim
// when Resume re-drives a call cut before its completion was recorded.
func (s *liveSink) completeModel(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	resp, err := s.c.invokeModel(ctx, s.executionID, req)
	if err != nil {
		return api.ModelResponse{}, err
	}
	s.c.liveModelCalls++
	msg := resp.Message
	if _, err := s.append(api.Event{Kind: api.EventOutput, Message: &msg}); err != nil {
		return api.ModelResponse{}, err
	}
	return resp, nil
}

func (s *liveSink) Output(ctx context.Context, delta string) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	return s.output(ctx, delta)
}

func (s *liveSink) output(_ context.Context, delta string) error {
	_, err := s.append(api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", delta)})
	return err
}

func (s *liveSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return api.ToolResult{}, err
	}
	return s.toolCall(ctx, tc)
}

func (s *liveSink) toolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	call := tc
	if call.Mediation == api.MediationRequiresApproval {
		return s.parkTool(ctx, call)
	}
	if s.calls[call.ID] {
		return api.ToolResult{}, ErrInvalidApprovalCall
	}
	if err := toolMediationError(call.Mediation); err != nil {
		return api.ToolResult{}, err
	}
	// Every host-executed tool MUST carry an idempotency key: the crash-recovery re-drive (§3/I3)
	// dedups on session UID plus key. Reject before recording, so a keyless call leaves no
	// unrecoverable intent.
	if call.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
	}
	if _, err := s.append(api.Event{Kind: api.EventToolCall, ToolCall: &call}); err != nil {
		return api.ToolResult{}, err
	}
	s.calls[call.ID] = false
	return s.execTool(ctx, call)
}

// toolMediationError is the live pre-intent rejection. Replay may reproduce it when no TOOL_CALL
// is next or the next call has a different ID; same-ID evidence must still pass the identity check.
func toolMediationError(mediation api.Mediation) error {
	switch mediation {
	case api.MediationControllerMediated:
		return nil
	case api.MediationRequiresApproval:
		return ErrApprovalUnavailable // historical pre-intent rejection in a recorded prefix
	default:
		return fmt.Errorf("%w (got %q)", ErrUnmediatedToolCall, mediation)
	}
}

// execTool runs a controller-mediated tool and write-ahead records its TOOL_RESULT. The TOOL_CALL
// intent MUST already be recorded by the caller — execTool performs only phases 2 and 3 of §3
// (execute, then append the result), so it is reused verbatim on crash-recovery, where the intent
// is already durable in the journal.
func (s *liveSink) execTool(ctx context.Context, call api.ToolCall) (api.ToolResult, error) {
	if s.c.tool == nil {
		// ToolCall is the controller-mediated path; without an executor it is a misconfiguration
		// (in-harness-reported tools use Report instead).
		return api.ToolResult{}, errors.New("controller: no tool executor configured")
	}
	res, err := s.c.tool(ctx, ToolCallContext{SessionUID: s.c.sessionUID}, call)
	if err != nil {
		return api.ToolResult{}, err
	}
	s.c.liveToolCalls++
	result := res
	result.ID = call.ID // the host owns tool-result correlation: the result references its call's ID
	if _, err := s.append(api.Event{Kind: api.EventToolResult, Result: &result}); err != nil {
		return api.ToolResult{}, err
	}
	return result, nil
}

func (s *liveSink) Report(ctx context.Context, tr api.ToolResult) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	return s.report(ctx, tr)
}

func (s *liveSink) report(_ context.Context, tr api.ToolResult) error {
	if s.calls[tr.ID] {
		s.guard.stop = ErrApprovalReceiptReport
		return s.guard.stop
	}
	if err := approvalReportError(tr); err != nil {
		return err
	}
	res := tr
	_, err := s.append(api.Event{Kind: api.EventToolResult, Result: &res})
	return err
}

func (s *liveSink) Usage(ctx context.Context, u api.Usage) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	return s.usage(ctx, u)
}

func (s *liveSink) usage(_ context.Context, u api.Usage) error {
	usage := u
	_, err := s.append(api.Event{Kind: api.EventUsage, Usage: &usage})
	return err
}

// replaySink serves recorded results from the journal in order and never invokes a live op. It
// enforces the I0 model-input-hash check. A deterministic harness requests the recorded model,
// output, tool, and usage events in the same order.
type replaySink struct {
	stream    []api.Event
	i         int
	outputs   []string
	legacy    bool
	failure   error // Tool evidence rejection remains fatal even if the harness handles the error.
	guard     *sinkGuard
	approvals map[string]*ApprovalState
	calls     map[string]bool // gated IDs matched during this invocation, not all scanned history
}

var _ api.EventSink = (*replaySink)(nil)

func (s *replaySink) diverged(err error) error {
	s.failure = fmt.Errorf("%w: %w", ErrReplayDiverged, err)
	return s.failure
}

func (s *replaySink) nextOf(kind api.EventKind) (api.Event, bool) {
	if s.i >= len(s.stream) || s.stream[s.i].Kind != kind {
		return api.Event{}, false
	}
	ev := s.stream[s.i]
	s.i++
	return ev, true
}

func (s *replaySink) Model(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return api.ModelResponse{}, err
	}
	if s.failure != nil {
		return api.ModelResponse{}, s.failure
	}
	mc, ok := s.nextOf(api.EventModelCall)
	if !ok {
		return api.ModelResponse{}, s.diverged(errors.New("replay: expected a recorded model call, found none"))
	}
	if mc.ModelCall == nil || mc.ModelCall.InputHash != hashModelInput(req) {
		return api.ModelResponse{}, s.diverged(errors.New("replay: model input hash mismatch — harness violated STATELESS_REPLAY (I0)"))
	}
	out, ok := s.nextOf(api.EventOutput)
	if !ok {
		// A handled provider failure can leave a call without completion. Preserve its
		// bounded, handleable historical outcome rather than latching identity divergence.
		return api.ModelResponse{}, errors.New("replay: expected a recorded completion, found none")
	}
	var msg api.Message
	if out.Message != nil {
		msg = *out.Message
		s.outputs = append(s.outputs, out.Message.Text())
	}
	return api.ModelResponse{Message: msg}, nil
}

func (s *replaySink) Output(_ context.Context, delta string) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	ev, ok := s.nextOf(api.EventOutput)
	if !ok {
		return s.diverged(errors.New("replay: unexpected output (no matching recorded event)"))
	}
	recorded := ""
	if ev.Message != nil {
		recorded = ev.Message.Text()
	}
	// Symmetric with Model's I0 input-hash check: output emitted directly (not via the model) must
	// match what was journaled, or the harness is nondeterministic and the "byte-identical" claim
	// would silently break.
	if delta != recorded {
		return s.diverged(fmt.Errorf("replay: output mismatch — harness emitted %q, journal recorded %q (I0)", delta, recorded))
	}
	s.outputs = append(s.outputs, delta)
	return nil
}

func (s *replaySink) ToolCall(_ context.Context, tc api.ToolCall) (api.ToolResult, error) {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return api.ToolResult{}, err
	}
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	// Historical keyless ordinary rejections have no intent. A same-ID gated intent instead
	// requires identity validation, so changing mediation and dropping its key cannot hide it.
	if tc.Mediation == api.MediationControllerMediated && tc.IdempotencyKey == "" {
		if s.i >= len(s.stream) || s.stream[s.i].Kind != api.EventToolCall || s.stream[s.i].ToolCall == nil ||
			s.stream[s.i].ToolCall.ID != tc.ID || s.stream[s.i].ToolCall.Mediation != api.MediationRequiresApproval {
			return api.ToolResult{}, ErrMissingIdempotencyKey
		}
	}
	// A rejected pre-intent call must not consume a distinct fallback's recorded intent.
	if s.i < len(s.stream) {
		next := s.stream[s.i]
		if next.Kind == api.EventToolCall && next.ToolCall != nil && next.ToolCall.ID != tc.ID && next.ToolCall.Mediation != api.MediationRequiresApproval {
			if err := toolMediationError(tc.Mediation); err != nil {
				return api.ToolResult{}, err
			}
		}
	}
	call, ok := s.nextOf(api.EventToolCall)
	if !ok {
		if err := toolMediationError(tc.Mediation); err != nil {
			return api.ToolResult{}, err
		}
		s.failure = fmt.Errorf("%w: replay expected a recorded tool call, found none", ErrReplayDiverged)
		return api.ToolResult{}, s.failure
	}
	if err := matchToolCall(tc, call.ToolCall); err != nil {
		s.failure = err
		return api.ToolResult{}, err
	}
	var state *ApprovalState
	if call.ToolCall.Mediation == api.MediationRequiresApproval {
		s.calls[call.ToolCall.ID] = true
		state = s.approvals[call.ToolCall.ID]
		if err := consumeApprovalPrefix(s.stream, &s.i, state); err != nil {
			s.failure = err
			return api.ToolResult{}, err
		}
	}
	tr, ok := s.nextOf(api.EventToolResult)
	if !ok {
		// Older writers leave no result when the harness handles an executor error. There is
		// no diagnostic payload to recover, and the next effect belongs to the continuation.
		return api.ToolResult{}, errors.New("controller: recorded tool call has no result")
	}
	result, err := recordedToolResult(call.ToolCall, tr.Result)
	if state != nil {
		result, err = serveApproval(state, tr.Result)
	}
	if err != nil {
		s.failure = err
	}
	return result, err
}

func (s *replaySink) Report(_ context.Context, tr api.ToolResult) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	if s.calls[tr.ID] {
		return s.diverged(ErrApprovalReceiptReport)
	}
	if err := approvalReportError(tr); err != nil {
		return err
	}
	if s.i < len(s.stream) && s.stream[s.i].Result != nil && approvalReportError(*s.stream[s.i].Result) != nil {
		s.failure = fmt.Errorf("%w: Report cannot consume approval receipt", ErrReplayDiverged)
		return s.failure
	}
	if _, ok := s.nextOf(api.EventToolResult); !ok {
		return s.diverged(errors.New("replay: unexpected report (no matching recorded event)"))
	}
	return nil
}

func (s *replaySink) Usage(_ context.Context, usage api.Usage) error {
	s.guard.mu.Lock()
	defer s.guard.mu.Unlock()
	if err := s.guard.err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	if s.legacy {
		return nil // auxiliary accounting was excluded from v0.1.2's effect stream
	}
	ev, ok := s.nextOf(api.EventUsage)
	if !ok {
		return s.diverged(errors.New("replay: unexpected usage (no matching recorded event)"))
	}
	if ev.Usage == nil {
		return s.diverged(errors.New("replay: recorded usage is missing its payload"))
	}
	if *ev.Usage != usage {
		return s.diverged(fmt.Errorf("replay: usage mismatch — harness emitted %+v, journal recorded %+v (I0)",
			usage, *ev.Usage))
	}
	return nil
}
