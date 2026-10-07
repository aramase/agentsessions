package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/wire"
)

// liveSink is the host-mediated EventSink for a live turn: it invokes each nondeterministic op and
// records it to the log, so the identical events can be served on a later replay.
type liveSink struct {
	c           *Controller
	executionID string
	failure     error
}

var _ api.EventSink = (*liveSink)(nil)

// Model records the request (EVENT_MODEL_CALL with the input hash), invokes the model, records the
// completion as an EVENT_OUTPUT, and returns it. On replay the recorded completion is served in
// place of the invocation (see replaySink) — the harness code path is identical either way.
//
// Footgun: the completion is ALREADY recorded as the output here. A harness that also calls
// Output() with the same content double-records it; use Output() only for additional/streamed text.
func (s *liveSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if s.failure != nil {
		return api.ModelResponse{}, s.failure
	}
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
	if s.failure != nil {
		return s.failure
	}
	_, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", delta)})
	return err
}

// RejectToolCall is the optional wire rejection hook. Latch validation failures before a
// harness can handle the error and issue another effect or claim successful completion.
func (s *liveSink) RejectToolCall(err error) error {
	if s.failure == nil {
		s.failure = err
	}
	return s.failure
}

func (s *liveSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	// Use the same policy/key/argument guards for new calls and interrupted-intent redrive. Reject
	// before recording, so invalid arguments cannot silently become nil in the journal.
	call, err := validateLiveToolCall(tc)
	if errors.Is(err, wire.ErrInvalidToolArgs) {
		return api.ToolResult{}, s.RejectToolCall(err)
	}
	if err != nil {
		return api.ToolResult{}, err
	}
	if _, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventToolCall, ToolCall: &call}); err != nil {
		return api.ToolResult{}, err
	}
	return s.execTool(ctx, call)
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
	res, err := s.c.tool(ctx, call)
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
	if s.failure != nil {
		return s.failure
	}
	res := tr
	_, err := s.c.appendSeq(s.executionID, api.Event{Kind: api.EventToolResult, Result: &res})
	return err
}

func (s *liveSink) Usage(_ context.Context, u api.Usage) error {
	if s.failure != nil {
		return s.failure
	}
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

func (s *replaySink) RejectToolCall(err error) error {
	if s.failure == nil {
		s.failure = fmt.Errorf("%w: invalid emitted tool arguments: %w", ErrReplayDiverged, err)
	}
	return s.failure
}

func (s *replaySink) ToolCall(_ context.Context, tc api.ToolCall) (_ api.ToolResult, err error) {
	defer func() {
		if err != nil {
			s.failure = err
		}
	}()
	if s.failure != nil {
		return api.ToolResult{}, s.failure
	}
	call, ok := s.nextOf(api.EventToolCall)
	if !ok {
		return api.ToolResult{}, fmt.Errorf("%w: replay expected a recorded tool call, found none", ErrReplayDiverged)
	}
	if err := matchToolCall(tc, call.ToolCall); err != nil {
		return api.ToolResult{}, err
	}
	tr, ok := s.nextOf(api.EventToolResult)
	if !ok {
		return api.ToolResult{}, fmt.Errorf("%w: replay expected a recorded tool result, found none", ErrReplayDiverged)
	}
	return recordedToolResult(call.ToolCall, tr.Result)
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
