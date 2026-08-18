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
	// ToolCall is the HOST-EXECUTED path. Branch on the mediation tier up front, before recording,
	// so nothing the controller cannot safely run reaches execTool:
	//   - CONTROLLER_MEDIATED: the host executes and records (record-before-effect, §3).
	//   - REQUIRES_APPROVAL:   needs the approval gate (record request -> decision -> execute), not
	//     yet implemented, so fail closed rather than execute unapproved.
	//   - anything else (UNSPECIFIED, or IN_HARNESS_REPORTED which must use Report): reject, so an
	//     unmediated keyless call cannot slip through and execute/re-drive without dedup (I3 bypass).
	switch call.Mediation {
	case api.MediationControllerMediated:
		// handled below
	case api.MediationRequiresApproval:
		return api.ToolResult{}, errors.New("controller: REQUIRES_APPROVAL mediation is not yet implemented")
	default:
		return api.ToolResult{}, fmt.Errorf("%w (got %q)", ErrUnmediatedToolCall, call.Mediation)
	}
	// Every host-executed tool MUST carry an idempotency key: the crash-recovery re-drive (§3/I3)
	// dedups on it. Reject before recording, so a keyless call leaves no unrecoverable intent.
	if call.IdempotencyKey == "" {
		return api.ToolResult{}, ErrMissingIdempotencyKey
	}
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
	result.ID = call.ID // the host owns tool-result correlation: the result references its call's ID
	if _, err := s.c.appendSeq(api.Event{Kind: api.EventToolResult, Result: &result}); err != nil {
		return api.ToolResult{}, err
	}
	return result, nil
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

// Credential vends authority for the session's principal. It records the REQUEST — who asked for
// what, in turn order, under the same hash chain as every other effect — and then returns the
// token WITHOUT recording it. That split is the whole point: the log stays a complete audit of the
// authority a turn exercised, and never becomes a durable, forkable, replayable store of secrets.
//
// The request is recorded BEFORE the vend (record-before-effect, §3): a credential handed out is
// an external effect, so a crash must not be able to hide that it happened.
func (s *liveSink) Credential(req api.CredentialRequest) (api.Credential, error) {
	if s.c.creds == nil {
		return api.Credential{}, ErrNoCredentialSource
	}
	r := req
	if _, err := s.c.appendSeq(api.Event{
		Kind:       api.EventCredentialRequest,
		Credential: &r,
		Actor:      s.c.principal,
	}); err != nil {
		return api.Credential{}, err
	}
	return s.c.creds(s.c.principal, r)
}

// replaySink serves recorded results from the journal in order and never invokes a live op. It
// enforces the I0 model-input-hash check. The recorded effect stream is the ordered MODEL_CALL /
// OUTPUT / TOOL_CALL / TOOL_RESULT events; a deterministic harness requests them in the same order.
type replaySink struct {
	c       *Controller
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

// Credential is the one op replay does NOT serve from the journal: no token was ever recorded, so
// there is nothing to serve, and serving a stale one would hand the harness an expired secret. It
// re-vends live instead. This does not weaken replay determinism — a credential is a capability,
// not content: it never enters the effect stream, never enters the hash chain, and (unlike a model
// call) an I1-style "replay invoked it" assertion would be the wrong invariant here. Replay is
// read-only, so the request is not re-journaled either.
func (s *replaySink) Credential(req api.CredentialRequest) (api.Credential, error) {
	if s.c == nil || s.c.creds == nil {
		return api.Credential{}, ErrNoCredentialSource
	}
	return s.c.creds(s.c.principal, req)
}
