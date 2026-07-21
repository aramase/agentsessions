package api

import "context"

// Session is the durable unit: a conversation (event log) plus zero-or-one live compute
// incarnation. Unifying the two into one resource is what makes fork, provenance, and
// identity first-class.
type Session struct {
	UID     string
	Project string // tenant / namespace
	Name    string

	Harness string
	Model   string // model-agnostic id, e.g. "azure-openai/gpt-x", "anthropic/..."

	Phase   Phase
	LastSeq int64 // event-log cursor = the resume/replay point

	ParentUID string // set iff this session was forked
	ForkSeq   int64  // parent event seq forked at

	Identity IdentityRef
	Compute  *ComputeRef // current incarnation; nil when SUSPENDED/TERMINATED
	Origin   *Origin     // neutral external context; filled by a producer adapter

	Labels      map[string]string
	Annotations map[string]string // free-form adapter metadata (K8s-style)
}

// ComputeRef describes the session's current incarnation.
type ComputeRef struct {
	Runtime      string
	Worker       string // pod/worker id when live/warm; empty when cold
	Snapshot     *SnapshotRef
	Capabilities RuntimeCapabilities
	Attributes   map[string]string // backend-specific, e.g. substrate actor/atespace
}

// Sessions is the client-facing control-plane API (wire: api/session.proto).
//
// Single-writer invariant: a session cannot start a new Exec until the prior execution
// reaches a terminal state. The event-log Seq is authoritative and defines fork points.
type Sessions interface {
	Create(ctx context.Context, s *Session) (*Session, error)
	Get(ctx context.Context, uid string) (*Session, error)
	List(ctx context.Context, project string) ([]*Session, error)
	Delete(ctx context.Context, uid string) (*Session, error)

	// Exec runs one execution (turn); events are delivered to sink until terminal.
	// If the session exists, it is continued from its last state. Passing no inputs
	// (empty slice) resumes/re-drives the last non-terminal execution with no new
	// input (crash/interruption recovery); passing inputs starts a new turn and is
	// rejected until any in-flight execution reaches a terminal state (single-writer).
	Exec(ctx context.Context, uid string, inputs []Message, sink func(Event) error) error
	// Replay re-delivers committed events from fromSeq (read-only; audit / provenance).
	Replay(ctx context.Context, uid string, fromSeq int64, sink func(Event) error) error

	Pause(ctx context.Context, uid string) (*Session, error)             // warm, keep worker
	Suspend(ctx context.Context, uid string) (*Session, error)           // cold, free worker
	Resume(ctx context.Context, uid string, boot bool) (*Session, error) // boot = cold+replay vs restore
	Fork(ctx context.Context, uid string, atSeq int64, count int) ([]*Session, error)
}
