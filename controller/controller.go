// Package controller is the single-writer, event-sourced core of agentsessions. It drives one
// session's executions against a durable event log and host-mediates every nondeterministic effect
// through api.EventSink, so it can either invoke-and-record it (live) or serve it from the journal
// (replay). On replay it re-executes the harness deterministically and never invokes the model —
// the load-bearing rule that makes resume-on-a-fresh-pod byte-identical. The log is any
// eventlog.Store (in-memory or sqlite); CAS + fencing are enforced on every append.
//
// This drives the api.Harness Go interface in-process. A gRPC bridge that makes a harness running
// in a pod look like an api.Harness (the Harness.Connect stream) is a separate transport layer.
package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrReplayInvokedModel is returned when a replay caused a live model call — an I1 violation.
var ErrReplayInvokedModel = errors.New("controller: replay invoked the model (I1 violated)")

// ErrReplayDiverged is returned when a replay does not consume the recorded effect stream exactly:
// the harness requested fewer effects than were journaled, so it took a different path than when
// the log was written (a determinism violation, symmetric to the I0 input-hash check).
var ErrReplayDiverged = errors.New("controller: replay diverged from the journal")

// ModelFunc performs a live model invocation. It is the nondeterministic op the controller records
// on the live path and serves from the journal on replay.
type ModelFunc func(api.ModelRequest) (api.ModelResponse, error)

// Controller drives one session's log with a single incarnation (fence). It is meant to be driven
// by a single goroutine: Exec and Replay are NOT safe to call concurrently on the same Controller
// (liveModelCalls is unsynchronized). Concurrency BETWEEN controllers/processes is safe — the log's
// CAS + fencing reject a superseded writer.
type Controller struct {
	log            eventlog.Store
	model          ModelFunc
	fence          int64
	liveModelCalls int
}

// New starts a fresh incarnation over log: it advances the fencing token (superseding any prior
// incarnation, e.g. a dead pod) and returns a controller ready to Exec or Replay.
func New(log eventlog.Store, model ModelFunc) (*Controller, error) {
	fence, err := log.NewFence()
	if err != nil {
		return nil, err
	}
	return &Controller{log: log, model: model, fence: fence}, nil
}

// Exec runs one live execution/turn. The first INPUT append is guarded by the caller's
// expectedLastSeq (the single-writer CAS at the session boundary); the harness then runs
// host-mediated, and the turn ends with an END event.
func (c *Controller) Exec(ctx context.Context, har api.Harness, inputs []api.Message, expectedLastSeq int64) error {
	// The harness receives the committed conversation so far as History (a stateless harness
	// reconstructs its context from it); the echo harness ignores it, but a real one needs it.
	prior, err := c.log.Read(1)
	if err != nil {
		return err
	}
	history := make([]api.Event, 0, len(prior))
	for _, r := range prior {
		history = append(history, r.Event)
	}

	last := expectedLastSeq
	for i := range inputs {
		in := inputs[i]
		rec, err := c.log.Append(last, c.fence, api.Event{Kind: api.EventInput, Message: &in})
		if err != nil {
			return err
		}
		last = rec.Seq
	}
	if err := har.Run(ctx, &api.Start{History: history, Inputs: inputs}, &liveSink{c: c}); err != nil {
		// Best-effort: record the failure. If this append itself fails we still surface the
		// original harness error to the caller.
		_, _ = c.appendSeq(api.Event{Kind: api.EventError, Err: &api.Error{Description: err.Error()}})
		return err
	}
	_, err = c.appendSeq(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	return err
}

// Replay reconstructs the session by re-executing the harness with every effect served from the
// journal. It asserts the model is never invoked (I1) and that each recorded model-input hash
// matches (I0), returning the reconstructed outputs for an equivalence check. It is read-only.
func (c *Controller) Replay(ctx context.Context, har api.Harness) ([]string, error) {
	recs, err := c.log.Read(1)
	if err != nil {
		return nil, err
	}
	var inputs []api.Message
	var stream, history []api.Event
	for _, r := range recs {
		history = append(history, r.Event)
		switch r.Event.Kind {
		case api.EventInput:
			if r.Event.Message != nil {
				inputs = append(inputs, *r.Event.Message)
			}
		case api.EventModelCall, api.EventOutput, api.EventToolCall, api.EventToolResult:
			stream = append(stream, r.Event)
		}
	}
	before := c.liveModelCalls
	sink := &replaySink{stream: stream}
	if err := har.Run(ctx, &api.Start{Inputs: inputs, History: history}, sink); err != nil {
		return nil, err
	}
	if c.liveModelCalls != before {
		return nil, ErrReplayInvokedModel
	}
	// The harness must consume the recorded effect stream exactly. Over-consumption already errors
	// inside the sink (nextOf); this catches under-consumption — a harness that took a shorter path
	// on replay would otherwise return success with truncated outputs.
	if sink.i != len(sink.stream) {
		return nil, fmt.Errorf("%w: consumed %d of %d recorded effects", ErrReplayDiverged, sink.i, len(sink.stream))
	}
	return sink.outputs, nil
}

// Outputs returns the recorded assistant outputs in order.
func (c *Controller) Outputs() ([]string, error) {
	recs, err := c.log.Read(1)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range recs {
		if r.Event.Kind == api.EventOutput && r.Event.Message != nil {
			out = append(out, r.Event.Message.Text())
		}
	}
	return out, nil
}

// ModelInvocations is the number of live model calls made so far (0 across a pure replay).
func (c *Controller) ModelInvocations() int { return c.liveModelCalls }

// Head returns the current log head seq.
func (c *Controller) Head() (int64, error) { return c.log.Head() }

func (c *Controller) appendSeq(ev api.Event) (eventlog.Record, error) {
	head, err := c.log.Head()
	if err != nil {
		return eventlog.Record{}, err
	}
	return c.log.Append(head, c.fence, ev)
}

// Fork branches parent's log at atSeq into child: it copies the prefix [1..atSeq] verbatim (the
// canonical hashes reproduce identically, so the child shares the prefix chain) and records a
// LIFECYCLE_FORK marker as the child's first new event, whose prev_hash links to parent@atSeq —
// turning the hash-chain into a hash-tree. child must be an empty log.
func Fork(parent, child eventlog.Store, atSeq int64) error {
	recs, err := parent.Read(1)
	if err != nil {
		return err
	}
	fence, err := child.NewFence()
	if err != nil {
		return err
	}
	var last int64
	for _, r := range recs {
		if r.Seq > atSeq {
			break
		}
		if _, err := child.Append(last, fence, r.Event); err != nil {
			return err
		}
		last = r.Seq
	}
	_, err = child.Append(last, fence, api.Event{
		Kind:      api.EventLifecycle,
		Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: fmt.Sprintf("parent@%d", atSeq)},
	})
	return err
}

// CanPlace reports whether a harness can be scheduled on a runtime. The load-bearing rule: a
// harness that needs a memory snapshot cannot run on a snapshot-incapable runtime (e.g. a plain
// pod); a STATELESS_REPLAY harness runs anywhere.
func CanPlace(harness api.Capabilities, runtime api.RuntimeCapabilities) bool {
	if harness.Resumability == api.ResumabilityRequiresMemorySnapshot && !runtime.MemorySnapshot {
		return false
	}
	return true
}

// hashModelInput is the model-input fingerprint stored on EVENT_MODEL_CALL and re-checked on
// replay (I0). It is host-internal to one runtime's live/replay pairing, so a Go-stable hash
// suffices (unlike content_hash, which must be language-neutral — see package canon).
func hashModelInput(req api.ModelRequest) string {
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
