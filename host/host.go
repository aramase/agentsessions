// Package host is the reference walking-skeleton host for agentsessions. It realizes the
// central mechanism of the durable-log & replay contract: a harness performs ALL
// nondeterministic operations through the host-mediated Effects interface, so the host
// can either invoke-and-record them (live) or serve them from the journal (replay). On
// replay the host re-executes the harness deterministically and never invokes the model.
//
// See agentsessions-replay-determinism-contract.md — §"How replay works", I0, I1, I5, §5.
package host

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

// ModelRequest / ModelResponse are the skeleton's minimal model call. A real content
// model (reasoning parts, tool calls, multimodal) is Track B; the shape here is enough
// to prove the replay mechanism.
type ModelRequest struct{ Prompt string }
type ModelResponse struct{ Text string }

// Effects is the host-mediated interface through which a harness performs every
// nondeterministic operation. This is the load-bearing SPI rule: a STATELESS_REPLAY
// harness must not call a provider SDK / clock / RNG directly, or the host cannot serve
// those results on replay and I0/I1 become unenforceable. (Now()/Random() follow the
// same pattern; omitted from the skeleton's echo harness.)
type Effects interface {
	Model(ModelRequest) (ModelResponse, error)
	Emit(text string) error
}

// Harness is the skeleton's minimal Bring-Your-Own-Harness contract.
type Harness interface {
	Run(e Effects, input string) error
}

// Host drives one session's log with a single incarnation (fence).
type Host struct {
	log            *eventlog.Log
	fence          int64
	model          func(ModelRequest) ModelResponse
	liveModelCalls int
}

// New creates a host with a fresh log and the given (live) model function.
func New(model func(ModelRequest) ModelResponse) *Host {
	l := eventlog.New()
	return &Host{log: l, fence: l.NewFence(), model: model}
}

// Advance runs one execution/turn. The first append (INPUT) is guarded by the caller's
// expectedLastSeq (the single-writer CAS at the session API); subsequent appends are
// sequential within this incarnation.
func (h *Host) Advance(expectedLastSeq int64, har Harness, input string) error {
	if _, err := h.log.Append(expectedLastSeq, h.fence,
		api.Event{Kind: api.EventInput, Message: api.TextMessage("user", input)}); err != nil {
		return err
	}
	if err := har.Run(liveEffects{h}, input); err != nil {
		return err
	}
	_, err := h.appendSeq(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	return err
}

// Replay reconstructs the session by re-executing the harness with results served from
// the journal. It asserts (a) the model is never invoked (I1) and (b) each recorded model
// input hash matches (I0). It returns the reconstructed outputs for equivalence checks.
func (h *Host) Replay(har Harness) ([]string, error) {
	var input string
	var stream []api.Event
	for _, r := range h.log.Read(1) {
		switch r.Event.Kind {
		case api.EventInput:
			input = r.Event.Message.Text()
		case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult:
			stream = append(stream, r.Event)
		}
	}
	before := h.liveModelCalls
	re := &replayEffects{stream: stream}
	if err := har.Run(re, input); err != nil {
		return nil, err
	}
	if h.liveModelCalls != before {
		return nil, errors.New("host: replay invoked the model — I1 violated")
	}
	return re.outputs, nil
}

// Fork returns a child host seeded from this session's prefix [1..atSeq] (a replay-fork).
// The child's chain continues from the parent's hash at the fork point (hash-tree).
func (h *Host) Fork(atSeq int64) *Host {
	var prefix []eventlog.Record
	for _, r := range h.log.Read(1) {
		if r.Seq <= atSeq {
			prefix = append(prefix, r)
		}
	}
	child := eventlog.NewFrom(prefix)
	ch := &Host{log: child, fence: child.NewFence(), model: h.model}
	// Record the fork as a lifecycle marker (the child's first new event).
	_, _ = child.Append(child.Head(), ch.fence, api.Event{
		Kind:      api.EventLifecycle,
		Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: fmt.Sprintf("parent@%d", atSeq)},
	})
	return ch
}

// Outputs returns the recorded assistant outputs in order.
func (h *Host) Outputs() []string {
	var o []string
	for _, r := range h.log.Read(1) {
		if r.Event.Kind == api.EventOutput {
			o = append(o, r.Event.Message.Text())
		}
	}
	return o
}

// ModelInvocations is the number of times the live model function was actually called.
func (h *Host) ModelInvocations() int { return h.liveModelCalls }

// Head is the current log head seq.
func (h *Host) Head() int64 { return h.log.Head() }

func (h *Host) appendSeq(ev api.Event) (eventlog.Record, error) {
	return h.log.Append(h.log.Head(), h.fence, ev)
}

// liveEffects invokes the real op and records the result.
type liveEffects struct{ h *Host }

func (e liveEffects) Model(req ModelRequest) (ModelResponse, error) {
	resp := e.h.model(req)
	e.h.liveModelCalls++
	_, err := e.h.appendSeq(api.Event{
		Kind:      api.EventModelCall,
		ModelCall: &api.ModelCall{Model: "echo", Params: map[string]string{"req_hash": hashReq(req), "resp": resp.Text}},
	})
	if err != nil {
		return ModelResponse{}, err
	}
	return resp, nil
}

func (e liveEffects) Emit(text string) error {
	_, err := e.h.appendSeq(api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", text)})
	return err
}

// replayEffects serves recorded results from the journal in order, never invoking the
// model. It enforces the I0 input-hash check on model calls.
type replayEffects struct {
	stream  []api.Event
	i       int
	outputs []string
}

func (e *replayEffects) nextOf(kind api.EventKind) (api.Event, bool) {
	if e.i >= len(e.stream) || e.stream[e.i].Kind != kind {
		return api.Event{}, false
	}
	ev := e.stream[e.i]
	e.i++
	return ev, true
}

func (e *replayEffects) Model(req ModelRequest) (ModelResponse, error) {
	ev, ok := e.nextOf(api.EventModelCall)
	if !ok {
		return ModelResponse{}, errors.New("replay: expected a recorded model call, found none")
	}
	if ev.ModelCall.Params["req_hash"] != hashReq(req) {
		return ModelResponse{}, errors.New("replay: model input hash mismatch — harness violated STATELESS_REPLAY (I0)")
	}
	return ModelResponse{Text: ev.ModelCall.Params["resp"]}, nil
}

func (e *replayEffects) Emit(text string) error {
	if _, ok := e.nextOf(api.EventOutput); !ok {
		return errors.New("replay: unexpected emit (no matching recorded output)")
	}
	e.outputs = append(e.outputs, text)
	return nil
}

func hashReq(req ModelRequest) string {
	sum := sha256.Sum256([]byte(req.Prompt))
	return hex.EncodeToString(sum[:])
}
