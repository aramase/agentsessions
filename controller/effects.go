package controller

import (
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// liveSink is the host-mediated EventSink for a live turn: it invokes each nondeterministic op and
// records it to the log, so the identical events can be served on a later replay.
type liveSink struct{ c *Controller }

var _ api.EventSink = (*liveSink)(nil)

// Model records the request (EVENT_MODEL_CALL with the input hash), invokes the model, records the
// completion as an EVENT_OUTPUT, and returns it. On replay the recorded completion is served in
// place of the invocation (see replaySink) — the harness code path is identical either way.
//
// Footgun: the completion is ALREADY recorded as the output here. A harness that also calls
// Output() with the same content double-records it; use Output() only for additional/streamed text.
func (s *liveSink) Model(req api.ModelRequest) (api.ModelResponse, error) {
	if _, err := s.c.appendSeq(api.Event{
		Kind:      api.EventModelCall,
		ModelCall: &api.ModelCall{Model: req.Model, InputHash: hashModelInput(req), ID: newID()},
	}); err != nil {
		return api.ModelResponse{}, err
	}
	resp, err := s.c.model(req)
	if err != nil {
		return api.ModelResponse{}, err
	}
	s.c.liveModelCalls++
	msg := resp.Message
	if _, err := s.c.appendSeq(api.Event{Kind: api.EventOutput, Message: &msg}); err != nil {
		return api.ModelResponse{}, err
	}
	return resp, nil
}

func (s *liveSink) Output(delta string) error {
	_, err := s.c.appendSeq(api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", delta)})
	return err
}

func (s *liveSink) ToolCall(tc api.ToolCall) (api.ToolResult, error) {
	call := tc
	// Record-before-effect (§3/I3): the TOOL_CALL intent (+ idempotency key) is durable BEFORE the
	// side effect runs, so a crash after execute re-drives under the same key (tool-side dedup).
	if _, err := s.c.appendSeq(api.Event{Kind: api.EventToolCall, ToolCall: &call}); err != nil {
		return api.ToolResult{}, err
	}
	return s.execTool(call)
}

// execTool runs a controller-mediated tool and write-ahead records its TOOL_RESULT. The TOOL_CALL
// intent MUST already be recorded by the caller — execTool performs only phases 2 and 3 of §3
// (execute, then append the result), so it is reused verbatim on crash-recovery, where the intent
// is already durable in the journal.
func (s *liveSink) execTool(call api.ToolCall) (api.ToolResult, error) {
	if s.c.tool == nil {
		// ToolCall is the controller-mediated path; without an executor it is a misconfiguration
		// (in-harness-reported tools use Report instead).
		return api.ToolResult{}, errors.New("controller: no tool executor configured")
	}
	res, err := s.c.tool(call)
	if err != nil {
		return api.ToolResult{}, err
	}
	result := res
	if _, err := s.c.appendSeq(api.Event{Kind: api.EventToolResult, Result: &result}); err != nil {
		return api.ToolResult{}, err
	}
	return res, nil
}

func (s *liveSink) Report(tr api.ToolResult) error {
	res := tr
	_, err := s.c.appendSeq(api.Event{Kind: api.EventToolResult, Result: &res})
	return err
}

func (s *liveSink) Usage(u api.Usage) error {
	usage := u
	_, err := s.c.appendSeq(api.Event{Kind: api.EventUsage, Usage: &usage})
	return err
}

// replaySink serves recorded results from the journal in order and never invokes a live op. It
// enforces the I0 model-input-hash check. The recorded effect stream is the ordered MODEL_CALL /
// OUTPUT / TOOL_CALL / TOOL_RESULT events; a deterministic harness requests them in the same order.
type replaySink struct {
	stream  []api.Event
	i       int
	outputs []string
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

func (s *replaySink) Model(req api.ModelRequest) (api.ModelResponse, error) {
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

func (s *replaySink) Output(delta string) error {
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

func (s *replaySink) ToolCall(api.ToolCall) (api.ToolResult, error) {
	if _, ok := s.nextOf(api.EventToolCall); !ok {
		return api.ToolResult{}, errors.New("replay: expected a recorded tool call, found none")
	}
	tr, ok := s.nextOf(api.EventToolResult)
	if !ok {
		return api.ToolResult{}, errors.New("replay: expected a recorded tool result, found none")
	}
	if tr.Result == nil {
		return api.ToolResult{}, nil
	}
	return *tr.Result, nil
}

func (s *replaySink) Report(api.ToolResult) error {
	if _, ok := s.nextOf(api.EventToolResult); !ok {
		return errors.New("replay: unexpected report (no matching recorded event)")
	}
	return nil
}

// Usage is auxiliary accounting and not part of the served effect stream, so replay ignores it.
func (s *replaySink) Usage(api.Usage) error { return nil }
