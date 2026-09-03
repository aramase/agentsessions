package api

import "context"

// Harness is the Bring-Your-Own-Harness SPI. A Microsoft Agent Framework (MAF) agent,
// a GitHub Copilot agent, or a custom agent implements it via a thin adapter. The host
// drives one execution per Run; the harness runs inside the sandbox provided by a
// Runtime.
//
// The wire contract is api/harness.proto (Harness.Connect / Harness.Describe); an SDK
// adapts between them and hides sequence numbers and the tool-mediation round-trip.
type Harness interface {
	// Describe returns the static contract, used to match the harness to a Runtime.
	Describe(ctx context.Context) (Descriptor, error)

	// Run drives exactly one execution (turn). It reads the Start, emits events via
	// sink, and returns nil on COMPLETED or an error on FAILED. Cancellation via ctx.
	Run(ctx context.Context, s *Start, sink EventSink) error
}

// Descriptor is a harness's static contract.
type Descriptor struct {
	ID           string
	Models       []string // model-agnostic: supported/required model ids
	Tools        []ToolSpec
	Capabilities Capabilities
}

// ToolSpec declares a tool the harness can call and its default mediation.
type ToolSpec struct {
	Name        string
	Description string
	Mediation   Mediation
}

// Resumability declares how a harness survives suspend/resume/fork.
type Resumability string

const (
	// ResumabilityStatelessReplay: the harness holds no durable in-memory state beyond
	// the event log. On resume/fork the host replays history via Start.History. Runs on
	// any runtime, including a plain pod. Default.
	ResumabilityStatelessReplay Resumability = "STATELESS_REPLAY"
	// ResumabilityRequiresMemorySnapshot: the harness holds in-process state (a REPL, a
	// browser, a long-running process). The host only schedules it on a runtime whose
	// RuntimeCapabilities.MemorySnapshot is true.
	ResumabilityRequiresMemorySnapshot Resumability = "REQUIRES_MEMORY_SNAPSHOT"
)

// Capabilities is what a harness needs from the runtime and how it may be moved.
type Capabilities struct {
	Resumability Resumability
	ForkSafe     bool // no un-replayable side effects mid-turn -> fork via replay is safe
	RequiresGPU  bool
	Streaming    bool
	// ReasoningReplay: the harness persists and replays opaque provider reasoning parts
	// verbatim via the provider's stateless path (not previous_response_id / server lineage).
	// This keeps a reasoning harness on STATELESS_REPLAY — it adds NO snapshot edge. Verified
	// across Anthropic/OpenAI/Gemini in the Track A reasoning-continuity spike.
	ReasoningReplay bool
}

// Start is the per-execution invocation the host sends to the harness.
type Start struct {
	Config        []byte    // opaque per-execution config
	History       []Event   // replay context; empty if the sandbox was memory-restored
	Inputs        []Message // new input(s); empty = resume/re-drive an interrupted execution
	Identity      IdentityContext
	ResumeFromSeq int64
}

// IdentityContext carries the session principal and, optionally, a minter so the
// harness can obtain scoped, delegated tokens (Transaction Tokens) for tool calls
// rather than passing the raw principal downstream.
type IdentityContext struct {
	Principal IdentityRef
	MintToken TokenMinter // nil when identity is not configured (dev)
}

// TokenMinter returns a scoped token for a downstream audience.
type TokenMinter func(ctx context.Context, audience []string) (string, error)

// EventSink is the harness author's handle for emitting events. The SDK hides the gRPC
// stream, sequence assignment, and the emit -> wait-for-host round-trip.
type EventSink interface {
	// Model performs a model call and returns the completion. Live: the host invokes the
	// model and records the result. Replay: the host serves the recorded result from the
	// journal and does not invoke the model. Because the result flows back through this
	// mediated call, the harness never touches a provider SDK directly — the load-bearing
	// replay rule. Reasoning parts ride in the returned ModelResponse.Message and are
	// recorded verbatim for replay/fork continuity (I2). The completion is recorded as the
	// turn's output by the host; do not also emit the same content via Output (double-record).
	//
	// ctx carries the execution's cancellation and deadline through to the provider call, so
	// a cancelled turn stops an in-flight completion instead of orphaning it. Pass the ctx
	// from Run, or one derived from it.
	Model(ctx context.Context, req ModelRequest) (ModelResponse, error)
	// Output streams assistant output (a delta or a full message).
	Output(ctx context.Context, delta string) error
	// ToolCall emits a tool call and returns its result. For CONTROLLER_MEDIATED or
	// REQUIRES_APPROVAL tools it blocks until the host returns a result; for
	// IN_HARNESS_REPORTED tools the author executes the tool and calls Report instead.
	// ctx bounds that wait: a cancelled ctx unblocks it rather than hanging on a host that
	// never replies.
	ToolCall(ctx context.Context, tc ToolCall) (ToolResult, error)
	// Report records the result of a tool the harness executed itself.
	Report(ctx context.Context, tr ToolResult) error
	// Usage records token/cost accounting.
	Usage(ctx context.Context, u Usage) error
}

// ModelRequest is a model call: the message context + model selection.
type ModelRequest struct {
	Model    string
	Messages []Message // context (history + new input) sent to the model
	Params   map[string]string
}

// ModelResponse is the model's completion: an assistant message (which may carry text and
// opaque reasoning parts) plus usage.
type ModelResponse struct {
	Message Message
	Usage   Usage
}
