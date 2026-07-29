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
type ModelFunc func(api.ModelRequest) (api.ModelResponse, error)

// ToolFunc executes a CONTROLLER_MEDIATED tool. The host calls it between appending the TOOL_CALL
// intent and appending the TOOL_RESULT (the two-phase write-ahead of §3/I3). It receives the call's
// IdempotencyKey and owns tool-side deduplication: on crash-recovery the host re-executes the same
// call under the same key, and an idempotent tool MUST NOT repeat the external effect.
type ToolFunc func(api.ToolCall) (api.ToolResult, error)

// CredentialFunc vends a short-lived credential for the session's principal: a downstream
// credential the platform holds on its behalf (req.Provider) or a scoped delegated token minted
// for an audience (req.Audience). It is the host's authority seam — the harness never reaches a
// credential source itself, in-process or in a sandbox.
//
// Unlike ModelFunc this is NOT a recorded effect: the controller journals the request and returns
// the token in-band without writing it, so the log is a complete audit of the authority a turn
// exercised and never a store of the secrets themselves.
type CredentialFunc func(principal api.IdentityRef, req api.CredentialRequest) (api.Credential, error)

// ErrNoCredentialSource is returned to a harness that asks for a credential on a host with no
// credential source configured. It fails closed: a harness must not silently proceed unauthorized.
var ErrNoCredentialSource = errors.New("controller: no credential source configured")

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

// WithCredentialSource sets the host's authority seam: what vends credentials to the harness.
// Without it, a harness that asks for one gets ErrNoCredentialSource and Start reports
// CanMintTokens=false, so a harness can degrade rather than fail blind.
func WithCredentialSource(creds CredentialFunc) Option {
	return func(c *Controller) { c.creds = creds }
}

// WithPrincipal binds the session's principal to the controller. It travels to the harness in
// Start.Identity and is what the credential source vends FOR, so a harness never names the
// subject it acts as — the host does.
func WithPrincipal(p api.IdentityRef) Option { return func(c *Controller) { c.principal = p } }

// Controller drives one session's log with a single incarnation (fence). It is meant to be driven
// by a single goroutine: Exec and Replay are NOT safe to call concurrently on the same Controller
// (liveModelCalls is unsynchronized). Concurrency BETWEEN controllers/processes is safe — the log's
// CAS + fencing reject a superseded writer.
type Controller struct {
	log            eventlog.Store
	model          ModelFunc
	tool           ToolFunc
	creds          CredentialFunc
	principal      api.IdentityRef
	fence          int64
	liveModelCalls int
}

// New starts an incarnation over log. Unless the caller supplies a fence via WithFence, it advances
// the log's fencing token (superseding any prior incarnation, e.g. a dead pod) and binds to it. When
// WithFence is supplied (the placement layer minted the fence and stamped it on the incarnation), New
// uses that token instead — the log remains the single authority either way.
func New(log eventlog.Store, model ModelFunc, opts ...Option) (*Controller, error) {
	c := &Controller{log: log, model: model}
	for _, o := range opts {
		o(c)
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
	if err := har.Run(ctx, &api.Start{History: history, Inputs: inputs, Identity: c.identity()}, &liveSink{c: c}); err != nil {
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
	sink := &replaySink{c: c, stream: stream}
	if err := har.Run(ctx, &api.Start{Inputs: inputs, History: history, Identity: c.identity()}, sink); err != nil {
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

// identity is what the harness is told about who it acts as: the bound principal, and whether the
// host can vend credentials for it.
func (c *Controller) identity() api.IdentityContext {
	return api.IdentityContext{Principal: c.principal, CanMintTokens: c.creds != nil}
}

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
