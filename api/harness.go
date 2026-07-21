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
	// Model records a model call (for audit/cost).
	Model(ModelCall) error
	// Output streams assistant output (a delta or a full message).
	Output(delta string) error
	// ToolCall emits a tool call and returns its result. For CONTROLLER_MEDIATED or
	// REQUIRES_APPROVAL tools it blocks until the host returns a result; for
	// IN_HARNESS_REPORTED tools the author executes the tool and calls Report instead.
	ToolCall(ToolCall) (ToolResult, error)
	// Report records the result of a tool the harness executed itself.
	Report(ToolResult) error
	// Usage records token/cost accounting.
	Usage(Usage) error
}
