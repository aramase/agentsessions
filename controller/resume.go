package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// Resume re-drives an interrupted last execution (crash-recovery, I4). If the journal's last turn
// did not complete (no END), Resume re-runs the harness with a hybrid sink that SERVES the
// already-recorded effects — never re-invoking a recorded model/tool call (at-most-once, I3) — and
// switches to LIVE (invoke + record) for anything past the crash point, then appends END. If the
// last turn is complete or the log is empty, it is a no-op (returns false).
//
// It assumes one INPUT per turn (the echo/demo shape); multi-input turns are a later refinement.
func (c *Controller) Resume(ctx context.Context, har api.Harness) (bool, error) {
	recs, err := c.log.Read(1)
	if err != nil {
		return false, err
	}
	if len(recs) == 0 {
		return false, nil
	}

	lastInput, lastEnd := -1, -1
	for i, r := range recs {
		switch r.Event.Kind {
		case api.EventInput:
			lastInput = i
		case api.EventEnd:
			lastEnd = i
		}
	}
	// Complete (or nothing to do) when there is no turn, or an END follows the last INPUT. A
	// trailing lifecycle marker (e.g. SUSPEND) after a completed turn is not an interrupted turn.
	if lastInput < 0 || lastEnd > lastInput {
		return false, nil
	}

	var inputs []api.Message
	var histEvents, stream []api.Event
	for i, r := range recs {
		switch {
		case i < lastInput:
			histEvents = append(histEvents, r.Event)
		case i == lastInput:
			if r.Event.Message != nil {
				inputs = append(inputs, *r.Event.Message)
			}
		default: // i > lastInput: already-recorded effects of the incomplete turn
			switch r.Event.Kind {
			case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult:
				stream = append(stream, r.Event)
			}
		}
	}

	sink := &resumeSink{live: liveSink{c: c}, stream: stream}
	if err := har.Run(ctx, &api.Start{Inputs: inputs, History: histEvents}, sink); err != nil {
		_, _ = c.appendSeq(api.Event{Kind: api.EventError, Err: &api.Error{Description: err.Error()}})
		return true, err
	}
	_, err = c.appendSeq(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
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

func (s *resumeSink) Model(req api.ModelRequest) (api.ModelResponse, error) {
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
	return s.live.Model(req) // past the crash point: first-ever execution
}

func (s *resumeSink) Output(delta string) error {
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
	return s.live.Output(delta)
}

func (s *resumeSink) ToolCall(tc api.ToolCall) (api.ToolResult, error) {
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
		return s.live.execTool(*call.ToolCall)
	}
	return s.live.ToolCall(tc)
}

func (s *resumeSink) Report(tr api.ToolResult) error {
	if s.i < len(s.stream) {
		if _, ok := s.recordedNext(api.EventToolResult); !ok {
			return errors.New("resume: recorded stream diverged (expected tool result)")
		}
		return nil
	}
	return s.live.Report(tr)
}

func (s *resumeSink) Usage(u api.Usage) error {
	if s.i >= len(s.stream) {
		return s.live.Usage(u)
	}
	return nil // usage is not part of the served effect stream
}
