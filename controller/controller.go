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
	"log/slog"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/observability"
)

// ErrReplayInvokedModel is returned when a replay caused a live model call — an I1 violation.
var ErrReplayInvokedModel = errors.New("controller: replay invoked the model (I1 violated)")

// ErrReplayDiverged is returned when a replay does not consume the recorded effect stream exactly:
// the harness requested fewer effects than were journaled, so it took a different path than when
// the log was written (a determinism violation, symmetric to the I0 input-hash check).
var ErrReplayDiverged = errors.New("controller: replay diverged from the journal")

// ErrMissingIdempotencyKey rejects a CONTROLLER_MEDIATED tool call that omits the idempotency key
// the crash-recovery re-drive needs to dedup its side effect (I3). Without a key, at-most-once
// silently would not hold, so the host fails loud rather than record an unrecoverable intent. The
// full key contract (generation, TTL, scope) is the §10 spike; this is the minimal guard.
var ErrMissingIdempotencyKey = errors.New("controller: CONTROLLER_MEDIATED tool call requires an idempotency key")

// ErrUnmediatedToolCall rejects a ToolCall whose mediation tier is not host-executed. ToolCall is
// only for CONTROLLER_MEDIATED (and, once implemented, REQUIRES_APPROVAL); an UNSPECIFIED or
// IN_HARNESS_REPORTED call must not execute here (the latter uses Report), so it is rejected before
// anything is recorded — closing the keyless-execute bypass.
var ErrUnmediatedToolCall = errors.New("controller: ToolCall requires a host-mediated tier")

// ModelFunc performs a live model invocation. It is the nondeterministic op the controller records
// on the live path and serves from the journal on replay.
//
// ctx is the execution's context: it carries cancellation, the turn deadline, and request
// correlation, so a provider implementation can abort an in-flight completion when the turn is
// cancelled rather than running to completion against a caller that has gone away.
type ModelFunc func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error)

// StreamFunc is a model invocation that reports partial output as it arrives. onChunk is called
// zero or more times before the final response, which must still be the complete message: the log
// records that, not the chunks, so a caller watching the stream and a caller replaying it later see
// the same finalized output.
//
// It exists because the host mediates the model call. The harness blocks on one sink.Model and is
// unaware anything streamed, so live output needs no harness cooperation and no change to the
// harness wire protocol.
type StreamFunc func(ctx context.Context, req api.ModelRequest, onChunk func(string)) (api.ModelResponse, error)

// Observer receives a turn's events as they happen. Both callbacks fire on the goroutine driving
// the turn, in order, so an implementation that writes to a stream needs no synchronization of its
// own but must not block for long.
type Observer struct {
	// OnRecord fires once per committed record, after it is durable.
	OnRecord func(eventlog.Record)
	// OnDelta fires for each ephemeral chunk. Nothing is logged, and replay never calls it.
	OnDelta func(api.Delta)
}

// ToolFunc executes a CONTROLLER_MEDIATED tool. The host calls it between appending the TOOL_CALL
// intent and appending the TOOL_RESULT (the two-phase write-ahead of §3/I3). It receives the call's
// IdempotencyKey and owns tool-side deduplication: on crash-recovery the host re-executes the same
// call under the same key, and an idempotent tool MUST NOT repeat the external effect. ctx carries
// the execution's cancellation and deadline.
type ToolFunc func(ctx context.Context, tc api.ToolCall) (api.ToolResult, error)

// Option configures a Controller at construction.
type Option func(*Controller)

// WithToolExecutor sets the executor for CONTROLLER_MEDIATED tool calls. Without it, a harness that
// emits a host-mediated ToolCall gets an error (in-harness-reported tools use Report instead).
func WithToolExecutor(tool ToolFunc) Option { return func(c *Controller) { c.tool = tool } }

// WithFence binds the controller to a fence the caller already minted from the log (via NewFence),
// instead of minting its own. The placement layer uses this so the fence it stamps on the
// incarnation and the fence the controller appends under are the same log-minted token — the log
// stays the single fence authority. Fences are >= 1, so WithFence(0) is a no-op (mint-my-own).
func WithFence(token int64) Option { return func(c *Controller) { c.fence = token } }

// WithLogger enables structured operational logs. Event payloads and fence values are never logged.
func WithLogger(logger *slog.Logger) Option { return func(c *Controller) { c.logger = logger } }

// WithStreamingModel supplies a model that reports partial output. When set it is used for live
// invocations instead of the plain ModelFunc; replay ignores it entirely, since replay serves
// recorded completions and must not reach a provider.
func WithStreamingModel(fn StreamFunc) Option { return func(c *Controller) { c.stream = fn } }

// WithObserver reports a turn's records and streaming chunks as they happen, so a caller can relay
// them instead of waiting for the turn to finish and re-reading the log.
func WithObserver(o Observer) Option { return func(c *Controller) { c.observer = o } }

// WithStart carries per-execution values the caller supplied straight through to the harness: the
// opaque config and the resume cursor. They are passed rather than interpreted, since only the
// harness knows what they mean.
func WithStart(config []byte, resumeFromSeq int64) Option {
	return func(c *Controller) {
		c.startConfig = config
		c.startResumeFromSeq = resumeFromSeq
	}
}

// WithSessionUID adds session correlation to controller logs.
func WithSessionUID(sessionUID string) Option {
	return func(c *Controller) { c.sessionUID = sessionUID }
}

// Controller drives one session's log with a single incarnation (fence). It is meant to be driven
// by a single goroutine: Exec and Replay are NOT safe to call concurrently on the same Controller
// (liveModelCalls is unsynchronized). Concurrency BETWEEN controllers/processes is safe — the log's
// CAS + fencing reject a superseded writer.
type Controller struct {
	log                eventlog.Store
	model              ModelFunc
	stream             StreamFunc
	observer           Observer
	startConfig        []byte
	startResumeFromSeq int64
	executionID        string
	tool               ToolFunc
	fence              int64
	liveModelCalls     int
	liveToolCalls      int
	logger             *slog.Logger
	sessionUID         string
}

// New starts an incarnation over log. Unless the caller supplies a fence via WithFence, it advances
// the log's fencing token (superseding any prior incarnation, e.g. a dead pod) and binds to it. When
// WithFence is supplied (the placement layer minted the fence and stamped it on the incarnation), New
// uses that token instead — the log remains the single authority either way.
func New(log eventlog.Store, model ModelFunc, opts ...Option) (*Controller, error) {
	c := &Controller{log: log, model: model, logger: slog.New(slog.DiscardHandler)}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
	// Fences are >= 1, so a zero fence means no WithFence was supplied: mint one from the log.
	if c.fence == 0 {
		fence, err := log.NewFence()
		if err != nil {
			return nil, err
		}
		c.fence = fence
	}
	return c, nil
}

// Exec runs one live execution/turn. The first INPUT append is guarded by the caller's
// expectedLastSeq (the single-writer CAS at the session boundary); the harness then runs
// host-mediated, and the turn ends with an END event.
func (c *Controller) Exec(ctx context.Context, har api.Harness, inputs []api.Message, expectedLastSeq int64) (err error) {
	ctx = observability.EnsureRequestID(ctx)
	var historyEvents int
	var finalSeq int64
	modelCallsBefore := c.liveModelCalls
	toolCallsBefore := c.liveToolCalls
	finish := observability.StartDebug(ctx, c.logger, "controller", "exec",
		"session_uid", c.sessionUID,
		"expected_last_seq", expectedLastSeq,
		"input_count", len(inputs),
	)
	defer func() {
		finish(err,
			"error_kind", controllerErrorKind(err),
			"history_event_count", historyEvents,
			"final_seq", finalSeq,
			"model_call_count", c.liveModelCalls-modelCallsBefore,
			"tool_call_count", c.liveToolCalls-toolCallsBefore,
		)
	}()

	// The harness receives the committed conversation so far as History (a stateless harness
	// reconstructs its context from it); the echo harness ignores it, but a real one needs it.
	prior, err := c.log.Read(1)
	if err != nil {
		return err
	}
	historyEvents = len(prior)
	history := make([]api.Event, 0, len(prior))
	for _, r := range prior {
		history = append(history, r.Event)
	}

	last := expectedLastSeq
	for i := range inputs {
		in := inputs[i]
		// This is the CAS-guarded append, so it cannot go through appendSeq (which reads the head
		// itself). It still has to be observed, or a caller watching the turn would never see the
		// input that started it.
		rec, err := c.log.Append(last, c.fence, api.Event{Kind: api.EventInput, Message: &in})
		if err != nil {
			return err
		}
		c.observe(rec)
		last = rec.Seq
	}
	runFinished := observability.StartDebug(ctx, c.logger, "controller", "run_harness",
		"session_uid", c.sessionUID,
		"history_event_count", historyEvents,
		"input_count", len(inputs),
	)
	start := &api.Start{
		History:       history,
		Inputs:        inputs,
		Config:        c.startConfig,
		ResumeFromSeq: c.startResumeFromSeq,
	}
	if err := har.Run(ctx, start, &liveSink{c: c}); err != nil {
		runFinished(err, "error_kind", "harness_run_failed")
		// Best-effort: record the failure. If this append itself fails we still surface the
		// original harness error to the caller.
		_, _ = c.appendSeq(api.Event{Kind: api.EventError, Err: &api.Error{Description: err.Error()}})
		return err
	}
	runFinished(nil)
	rec, err := c.appendSeq(api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	finalSeq = rec.Seq
	return err
}

// Replay reconstructs the session by re-executing the harness with every effect served from the
// journal. It asserts the model is never invoked (I1) and that each recorded model-input hash
// matches (I0), returning the reconstructed outputs for an equivalence check. It is read-only.
func (c *Controller) Replay(ctx context.Context, har api.Harness) (outputs []string, err error) {
	ctx = observability.EnsureRequestID(ctx)
	var recordCount, effectCount int
	finish := observability.StartDebug(ctx, c.logger, "controller", "replay", "session_uid", c.sessionUID)
	defer func() {
		finish(err,
			"error_kind", controllerErrorKind(err),
			"record_count", recordCount,
			"effect_count", effectCount,
			"output_count", len(outputs),
		)
	}()

	recs, err := c.log.Read(1)
	if err != nil {
		return nil, err
	}
	recordCount = len(recs)
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
	effectCount = len(stream)
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
	outputs = sink.outputs
	return outputs, nil
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

// ToolInvocations is the number of live tool calls made so far (0 across a pure replay).
func (c *Controller) ToolInvocations() int { return c.liveToolCalls }

// Head returns the current log head seq.
func (c *Controller) Head() (int64, error) { return c.log.Head() }

func (c *Controller) appendSeq(ev api.Event) (eventlog.Record, error) {
	head, err := c.log.Head()
	if err != nil {
		return eventlog.Record{}, err
	}
	rec, err := c.log.Append(head, c.fence, ev)
	if err != nil {
		return rec, err
	}
	c.observe(rec)
	return rec, nil
}

// observe reports a committed record. It runs after the append succeeds, so an observer only ever
// sees what is durable: a record it was told about cannot later turn out not to exist.
func (c *Controller) observe(rec eventlog.Record) {
	if c.observer.OnRecord != nil {
		c.observer.OnRecord(rec)
	}
}

// emitDelta reports an ephemeral chunk. Nothing is logged and no sequence is assigned, so a delta
// has no effect on the hash chain and a turn produces the same journal whether or not anyone was
// watching it.
func (c *Controller) emitDelta(partIndex int32, chunk string, done bool) {
	if c.observer.OnDelta == nil || chunk == "" && !done {
		return
	}
	c.observer.OnDelta(api.Delta{
		ExecutionID: c.executionID,
		PartIndex:   partIndex,
		Chunk:       chunk,
		Done:        done,
	})
}

// invokeModel performs the live model call, streaming partial output when a streaming model is
// configured. Either way it returns the complete response, which is what gets recorded: the chunks
// are a view of the call in progress, not the record of it.
func (c *Controller) invokeModel(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	if c.stream == nil {
		return c.model(ctx, req)
	}
	var index int32
	resp, err := c.stream(ctx, req, func(chunk string) {
		c.emitDelta(index, chunk, false)
	})
	if err != nil {
		return resp, err
	}
	c.emitDelta(index, "", true)
	return resp, nil
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

func controllerErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, eventlog.ErrConflict):
		return "conflict"
	case errors.Is(err, eventlog.ErrFenced):
		return "fenced"
	case errors.Is(err, ErrReplayInvokedModel):
		return "replay_invoked_model"
	case errors.Is(err, ErrReplayDiverged):
		return "replay_diverged"
	case errors.Is(err, ErrMissingIdempotencyKey):
		return "missing_idempotency_key"
	case errors.Is(err, ErrUnmediatedToolCall):
		return "unmediated_tool_call"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "operation_failed"
	}
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
