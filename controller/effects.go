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
	c           *Controller
	executionID string
}

var _ api.EventSink = (*liveSink)(nil)

// Model records the request (EVENT_MODEL_CALL with the input hash), invokes the model, records the
// completion as an EVENT_OUTPUT, and returns it. On replay the recorded completion is served in
// place of the invocation (see replaySink) — the harness code path is identical either way.
//
// Footgun: the completion is ALREADY recorded as the output here. A harness that also calls
// Output() with the same content double-records it; use Output() only for additional/streamed text.
func (s *liveSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if _, err := s.c.appendSeq(s.executionID, api.Event{
		Kind:      api.EventModelCall,
		ModelCall: &api.ModelCall{Model: req.Model, InputHash: hashModelInput(req), ID: newID()},
	}); err != nil {
		return api.ModelResponse{}, err
	}
	resp, err := s.c.invokeModel(ctx, s.executionID, req)
	if err != nil {
		return api.ModelResponse{}, err
	}
	s.c.liveModelCalls++
	msg := resp.Message
	if _, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventOutput, Message: &msg}); err != nil {
		return api.ModelResponse{}, err
	}
	return resp, nil
}

func (s *liveSink) Output(_ context.Context, delta string) error {
	_, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", delta)})
	return err
}

func (s *liveSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	call := tc
	// ToolCall is the HOST-EXECUTED path. Branch on the mediation tier up front, before recording,
	// so nothing the controller cannot safely run reaches execTool:
	//   - CONTROLLER_MEDIATED: the host executes and records (record-before-effect, §3).
	//   - REQUIRES_APPROVAL:   needs the approval gate (record request -> decision -> execute), not
	//     yet implemented, so fail closed rather than execute unapproved.
	//   - anything else (UNSPECIFIED, or IN_HARNESS_REPORTED which must use Report): reject, so an
	//     unmediated keyless call cannot slip through and execute/re-drive without dedup (I3 bypass).
	if err := toolMediationError(call.Mediation); err != nil {
		return api.ToolResult{}, err
	}
	// Every host-executed tool MUST carry an idempotency key: the crash-recovery re-drive (§3/I3)
	// dedups on session UID plus key. Reject before recording, so a keyless call leaves no
	// unrecoverable intent.
	if call.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
	}
	if _, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventToolCall, ToolCall: &call}); err != nil {
		return api.ToolResult{}, err
	}
	return s.execTool(ctx, call)
}

// toolMediationError is the live pre-intent rejection. Replay may reproduce it only when no
// TOOL_CALL is next; recorded call evidence must still pass the identity check.
func toolMediationError(mediation api.Mediation) error {
	switch mediation {
	case api.MediationControllerMediated:
		return nil
	case api.MediationRequiresApproval:
		return errors.New("controller: REQUIRES_APPROVAL mediation is not yet implemented")
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
	if _, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventToolResult, Result: &result}); err != nil {
		return api.ToolResult{}, err
	}
	return result, nil
}

func (s *liveSink) Report(_ context.Context, tr api.ToolResult) error {
	res := tr
	_, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventToolResult, Result: &res})
	return err
}

func (s *liveSink) Usage(_ context.Context, u api.Usage) error {
	usage := u
	_, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventUsage, Usage: &usage})
	return err
}

// replaySink serves recorded results from the journal in order and never invokes a live op. It
// enforces the I0 model-input-hash check. A deterministic harness requests the recorded model,
// output, tool, and usage events in the same order.
type replaySink struct {
	stream  []api.Event
	i       int
	outputs []string
	failure error // Tool evidence rejection remains fatal even if the harness handles the error.
}

var _ api.EventSink = (*replaySink)(nil)

func (s *replaySink) nextOf(kind api.EventKind) (api.Event, bool) {
	if s.i >= len(s.stream) || s.stream[s.i].Kind != kind {
		return api.Event{}, false
	}
	ev := s.stream[s.i]
	s.i++
	return ev, true
}

func (s *replaySink) Model(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if s.failure != nil {
		return api.ModelResponse{}, s.failure
	}
	mc, ok := s.nextOf(api.EventModelCall)
	if !ok {
		return api.ModelResponse{}, errors.New("replay: expected a recorded model call, found none")
	}
	if mc.ModelCall.InputHash != hashModelInput(req) {
		return api.ModelResponse{}, errors.New("replay: model input hash mismatch — harness violated STATELESS_REPLAY (I0)")
	}
	out, ok := s.nextOf(api.EventOutput)
	if !ok {
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
	if s.failure != nil {
		return s.failure
	}
	ev, ok := s.nextOf(api.EventOutput)
	if !ok {
		return errors.New("replay: unexpected output (no matching recorded event)")
	}
	recorded := ""
	if ev.Message != nil {
		recorded = ev.Message.Text()
	}
	// Symmetric with Model's I0 input-hash check: output emitted directly (not via the model) must
	// match what was journaled, or the harness is nondeterministic and the "byte-identical" claim
	// would silently break.
	if delta != recorded {
		return fmt.Errorf("replay: output mismatch — harness emitted %q, journal recorded %q (I0)", delta, recorded)
	}
	s.outputs = append(s.outputs, delta)
	return nil
}

func (s *replaySink) ToolCall(_ context.Context, tc api.ToolCall) (api.ToolResult, error) {
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	// Live rejects keyless calls before recording intent. Do not consume a later valid call
	// while reproducing that handleable rejection.
	if tc.Mediation == api.MediationControllerMediated && tc.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
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
	tr, ok := s.nextOf(api.EventToolResult)
	if !ok {
		// Older writers leave no result when the harness handles an executor error. There is
		// no diagnostic payload to recover, and the next effect belongs to the continuation.
		return api.ToolResult{}, errors.New("controller: recorded tool call has no result")
	}
	result, err := recordedToolResult(call.ToolCall, tr.Result)
	if err != nil {
		s.failure = err
	}
	return result, err
}

func (s *replaySink) Report(context.Context, api.ToolResult) error {
	if s.failure != nil {
		return s.failure
	}
	if _, ok := s.nextOf(api.EventToolResult); !ok {
		return errors.New("replay: unexpected report (no matching recorded event)")
	}
	return nil
}

func (s *replaySink) Usage(_ context.Context, usage api.Usage) error {
	if s.failure != nil {
		return s.failure
	}
	ev, ok := s.nextOf(api.EventUsage)
	if !ok {
		return errors.New("replay: unexpected usage (no matching recorded event)")
	}
	if ev.Usage == nil {
		return errors.New("replay: recorded usage is missing its payload")
	}
	if *ev.Usage != usage {
		return fmt.Errorf("replay: usage mismatch — harness emitted %+v, journal recorded %+v (I0)",
			usage, *ev.Usage)
	}
	return nil
}
