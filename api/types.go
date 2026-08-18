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
	PhaseUnspecified Phase = "UNSPECIFIED"
	PhasePending     Phase = "PENDING"
	PhaseRunning     Phase = "RUNNING"
	PhasePausing     Phase = "PAUSING"
	PhasePaused      Phase = "PAUSED" // warm: resident on worker, instant resume
	PhaseSuspending  Phase = "SUSPENDING"
	PhaseSuspended   Phase = "SUSPENDED" // cold: snapshot in storage, worker freed
	PhaseResuming    Phase = "RESUMING"
	PhaseForking     Phase = "FORKING"
	PhaseTerminated  Phase = "TERMINATED" // log retained + replayable
	PhaseFailed      Phase = "FAILED"
)

// EventKind classifies an Event. Typed events (vs opaque messages) are what make
// provenance, audit, cost, and tool-approval first-class.
type EventKind string

const (
	EventInput           EventKind = "INPUT"
	EventModelCall       EventKind = "MODEL_CALL"
	EventOutput          EventKind = "OUTPUT"
	EventToolCall        EventKind = "TOOL_CALL"
	EventToolResult      EventKind = "TOOL_RESULT"
	EventApprovalRequest EventKind = "APPROVAL_REQUEST"
	EventApprovalResult  EventKind = "APPROVAL_RESULT"
	EventUsage           EventKind = "USAGE"
	EventLifecycle       EventKind = "LIFECYCLE"
	EventEnd             EventKind = "END"
	EventError           EventKind = "ERROR"
	// EventCredentialRequest records that the harness asked the host for a credential.
	// The request is journaled (who asked for what); the token never is.
	EventCredentialRequest EventKind = "CREDENTIAL_REQUEST"
)

// Message is a role-tagged sequence of content parts (A2A Message = role + Part[]).
type Message struct {
	Role  string // user | assistant (A2A calls this "agent") | tool
	Parts []Part
}

// Part is one unit of content: text, file (inline or URI), structured data, or an opaque
// reasoning block. Aligned with A2A Part + MCP content. Exactly one field is set.
type Part struct {
	Text      *TextPart
	File      *FilePart
	Data      map[string]any
	Reasoning *ReasoningPart
}

// TextPart is a text content block.
type TextPart struct{ Text string }

// FilePart carries media inline or by URI (A2A file part; MCP resource_link).
type FilePart struct {
	MIME   string
	Bytes  []byte
	URI    string
	Digest string // content digest of the URI target, so the hash-chain covers externalized bytes
	Name   string
}

// ReasoningPart is an opaque, provider-tagged reasoning block replayed verbatim to
// preserve reasoning continuity within a turn (I2). agentsessions never interprets
// Opaque; provider specifics live inside it. One part type across all providers (Track A
// spike: no per-provider sub-shapes, no memory-snapshot edge).
type ReasoningPart struct {
	Provider      string // "anthropic" | "openai" | "google"
	ModelID       string
	Opaque        []byte // set, OR OpaqueURI when externalized
	OpaqueURI     string
	OpaqueDigest  string // content digest of OpaqueURI target (hash-chain covers externalized bytes)
	Summary       []Part // optional, non-authoritative, compaction-droppable
	ItemID        string
	Ordinal       int32
	ValidityScope string // = execution id (turn)
}

// TextMessage constructs a single-text-part message.
func TextMessage(role, text string) *Message {
	return &Message{Role: role, Parts: []Part{{Text: &TextPart{Text: text}}}}
}

// Text concatenates the text parts of a message (ignoring non-text parts).
func (m *Message) Text() string {
	var s string
	for _, p := range m.Parts {
		if p.Text != nil {
			s += p.Text.Text
		}
	}
	return s
}

// Event is the shared unit of the session log and the harness stream. The host assigns
// the authoritative Seq on append. Ordering + integrity + identity live on the envelope;
// typed content lives in the body.
type Event struct {
	// ExecutionID, timestamp, and content live on the Event; ordering (seq) and integrity
	// (prev_hash / content_hash) are host-assigned and live on the log-record envelope
	// (eventlog.Record / proto LogRecord), not on the harness-emitted Event.
	ExecutionID   string
	Timestamp     time.Time
	SchemaVersion int32 // versions the event body; replay survives schema skew

	Kind EventKind

	// typed body (exactly one set)
	Message        *Message
	ModelCall      *ModelCall
	ToolCall       *ToolCall
	Result         *ToolResult
	Approval       *ApprovalRequest
	ApprovalResult *ApprovalResult
	Usage          *Usage
	Lifecycle      *Lifecycle
	End            *HarnessEnd
	Err            *Error
	Credential     *CredentialRequest

	Actor IdentityRef // emitter principal -> provenance on every action
}

// Lifecycle marks a compute/session transition in the log (§7). Baseline is a replay /
// compaction checkpoint (§1).
type Lifecycle struct {
	Kind     LifecycleKind
	Detail   string
	Snapshot *SnapshotRef // captured state, set on SUSPEND (§5.1)
}

// LifecycleKind enumerates the lifecycle transitions recorded in the log.
type LifecycleKind string

const (
	LifecycleSuspend  LifecycleKind = "SUSPEND"
	LifecycleResume   LifecycleKind = "RESUME"
	LifecycleFork     LifecycleKind = "FORK"
	LifecycleBaseline LifecycleKind = "BASELINE"
	LifecycleCancel   LifecycleKind = "CANCEL"
)

// ModelCall records a call to a model (model-agnostic) for audit and cost.
type ModelCall struct {
	Model     string
	Params    map[string]string
	InputHash string // required for STATELESS_REPLAY so the §9.1 I0 check can run
	ID        string // correlation id for the served ModelResult (proto ModelCall.id)
}

// Usage is per-model-call token/cost accounting.
type Usage struct {
	Model           string
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
}

// CredentialRequest is how a harness obtains authority it is not trusted to hold: it asks the
// HOST, which vends for the session's principal. One channel, two forms — Provider names a
// downstream credential the platform holds on the principal's behalf (ingress: a user's GitHub
// token, a model key), Audience asks for a scoped, delegated token minted for a downstream
// service (egress: a Transaction Token, what IdentityContext.CanMintTokens advertises).
//
// The request is recorded so the log stays a complete audit of the authority a turn exercised.
// The resulting token is not — see Credential.
type CredentialRequest struct {
	Provider string   // downstream provider to vend for ("github", "ai")
	Audience []string // audience(s) to mint a delegated token for
}

// Credential is a short-lived bearer credential served to the harness in-band. It is wire-only:
// the host never writes it to the log, so a durable, hash-chained, forkable journal never becomes
// a secret store. Hold it for the turn; never log or persist it.
type Credential struct {
	Token     string
	ExpiresIn int64 // seconds; 0 if the issuer did not say
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
	ID             string
	Tool           string // tool name / MCP method
	Args           map[string]any
	Mediation      Mediation
	IdempotencyKey string // dedups a retried side-effecting call tool-side (§3/I3)
}

// ToolResult is the outcome of a ToolCall.
type ToolResult struct {
	ID           string
	Output       map[string]any
	OutputURI    string // externalized large output (MCP resource_link); else inline Output
	OutputDigest string // content digest of OutputURI target (hash-chain covers externalized bytes)
	IsError      bool
	Error        string
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
	Error *Error
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
