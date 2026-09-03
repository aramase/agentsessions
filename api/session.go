package api

// Session is the durable unit: a conversation (event log) plus zero-or-one live compute
// incarnation. Unifying the two into one resource is what makes fork, provenance, and
// identity first-class.
type Session struct {
	UID     string
	Project string // tenant / namespace
	Name    string

	Harness string
	Model   string // model-agnostic id, e.g. "azure-openai/gpt-x", "anthropic/..."

	Exec         ExecState    // execution/turn axis (the event-log axis)
	ComputeState ComputeState // incarnation axis
	LastSeq      int64        // event-log cursor = the resume/replay point

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
