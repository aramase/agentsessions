// Package api defines the agentsessions contracts: the Sessions control-plane API,
// the Harness BYOH SPI, and the Runtime compute SPI.
//
// The .proto files in this directory are the wire format; these Go types are the
// hand-written SPI used by hosts and backends (to be reconciled with generated code).
//
// Design rule: the core is vendor-neutral. Producers (GitHub, Foundry, custom apps)
// plug in on top as adapters; compute backends (pod, Kata, Cloud Hypervisor, substrate)
// plug in underneath via Runtime. No producer- or backend-specific fields in the core —
// specifics ride in Origin, annotations, and ComputeRef.Attributes.
package api

import "time"

// Phase is the session lifecycle state. It flattens two axes — execution (the event
// log) and compute (the incarnation) — into a single enum.
type Phase string

const (
	PhasePending    Phase = "PENDING"
	PhaseRunning    Phase = "RUNNING"
	PhasePausing    Phase = "PAUSING"
	PhasePaused     Phase = "PAUSED" // warm: resident on worker, instant resume
	PhaseSuspending Phase = "SUSPENDING"
	PhaseSuspended  Phase = "SUSPENDED" // cold: snapshot in storage, worker freed
	PhaseResuming   Phase = "RESUMING"
	PhaseForking    Phase = "FORKING"
	PhaseTerminated Phase = "TERMINATED" // log retained + replayable
	PhaseFailed     Phase = "FAILED"
)

// EventKind classifies an Event. Typed events (vs opaque messages) are what make
// provenance, audit, cost, and tool-approval first-class.
type EventKind string

const (
	EventInput           EventKind = "INPUT"
	EventModelCall       EventKind = "MODEL_CALL"
	EventModelDelta      EventKind = "MODEL_DELTA"
	EventOutput          EventKind = "OUTPUT"
	EventToolCall        EventKind = "TOOL_CALL"
	EventToolResult      EventKind = "TOOL_RESULT"
	EventApprovalRequest EventKind = "APPROVAL_REQUEST"
	EventUsage           EventKind = "USAGE"
	EventLog             EventKind = "LOG"
	EventEnd             EventKind = "END"
	EventError           EventKind = "ERROR"
)

// Message is one turn of content in the history. Simplified for v0; a richer Content
// type (parts, tool blocks, media) comes later.
type Message struct {
	Role    string // user | assistant | model | tool
	Content string
}

// Event is the shared unit of the session log and the harness stream. The host assigns
// the authoritative Seq on append.
type Event struct {
	Seq         int64
	ExecutionID string // which execution/turn produced this event
	Timestamp   time.Time
	Kind        EventKind

	Message   *Message
	ModelCall *ModelCall
	ToolCall  *ToolCall
	Result    *ToolResult
	Approval  *ApprovalRequest
	Usage     *Usage
	End       *HarnessEnd
	Err       *Error

	Actor IdentityRef // emitter principal -> provenance on every action
}

// ModelCall records a call to a model (model-agnostic) for audit and cost.
type ModelCall struct {
	Model  string
	Params map[string]string
}

// Usage is per-model-call token/cost accounting.
type Usage struct {
	Model        string
	InputTokens  int64
	OutputTokens int64
}

// Mediation controls how a tool call is executed.
type Mediation string

const (
	// MediationInHarnessReported: the harness executes the tool in-sandbox and reports
	// the result as an event for audit. Default; fastest.
	MediationInHarnessReported Mediation = "IN_HARNESS_REPORTED"
	// MediationControllerMediated: the harness emits the call; the host/gateway executes
	// it (authz + policy + audit) and returns the result.
	MediationControllerMediated Mediation = "CONTROLLER_MEDIATED"
	// MediationRequiresApproval: the call pauses for human or policy approval.
	MediationRequiresApproval Mediation = "REQUIRES_APPROVAL"
)

// ToolCall is a tool invocation. Its shape aligns with an MCP tool call (name +
// structured arguments) so MCP tools map onto it directly.
type ToolCall struct {
	ID        string
	Tool      string // tool name / MCP method
	Args      map[string]any
	Mediation Mediation
}

// ToolResult is the outcome of a ToolCall.
type ToolResult struct {
	ID      string
	Output  map[string]any
	IsError bool
	Error   string
}

// ApprovalRequest asks a human or policy engine to allow a tool call.
type ApprovalRequest struct {
	ToolCallID string
	Reason     string
}

// ApprovalResult answers an ApprovalRequest (sent by the host).
type ApprovalResult struct {
	ToolCallID string
	Approved   bool
	Reason     string
}

// HarnessEnd is the terminal state of one execution.
type HarnessEnd struct {
	State string // COMPLETED | FAILED | CANCELED
}

// Error mirrors a gRPC status code + description.
type Error struct {
	Code        int32
	Description string
}

// IdentityRef is the bound agent principal. It is OIDC-neutral: an Entra Agent ID is
// one issuer; a SPIFFE ID or a GitHub App identity are others. Carried on every event.
type IdentityRef struct {
	Principal string
	Issuer    string
	Subject   string // session-scoped sub
}

// Origin is the neutral external context a session is tied to: where it came from and
// what it is about. Producer adapters (GitHub, Foundry, custom apps) fill it; the core
// defines no producer-specific fields. Modeled on CloudEvents source/subject.
type Origin struct {
	Source     string            // e.g. "github.com/acme/repo", "foundry/project-x"
	Subject    string            // e.g. "issues/42", "threads/abc"
	URI        string            // optional link
	Attributes map[string]string // adapter-specific extras; well-known keys via profiles
}
