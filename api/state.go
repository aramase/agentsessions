package api

// ExecState is the execution/turn axis of a session's lifecycle — the event-log axis. It
// is modeled separately from ComputeState (the incarnation axis) rather than flattened
// into a single Phase, matching the durable-log & replay contract (§6). A client-facing
// summary MAY be derived via DerivePhase.
type ExecState string

const (
	ExecPending   ExecState = "PENDING"
	ExecRunning   ExecState = "RUNNING"
	ExecAwaiting  ExecState = "AWAITING" // blocked on approval/input, resolved from the log
	ExecCompleted ExecState = "COMPLETED"
	ExecFailed    ExecState = "FAILED"
	ExecCanceled  ExecState = "CANCELED"
)

// ComputeState is the incarnation (compute/sandbox) axis of a session's lifecycle.
type ComputeState string

const (
	ComputeNone       ComputeState = "NONE" // no incarnation
	ComputeLive       ComputeState = "LIVE" // running
	ComputeWarm       ComputeState = "WARM" // paused: resident, worker held
	ComputeCold       ComputeState = "COLD" // suspended: snapshot in storage, worker freed
	ComputeTerminated ComputeState = "TERMINATED"
)

// DerivePhase collapses the two axes into the legacy summary Phase for client display.
// The two axes remain authoritative.
func DerivePhase(e ExecState, c ComputeState) Phase {
	switch {
	case e == ExecFailed:
		return PhaseFailed
	case e == ExecCompleted && c == ComputeTerminated:
		return PhaseTerminated
	case c == ComputeCold:
		return PhaseSuspended
	case c == ComputeWarm:
		return PhasePaused
	case c == ComputeLive && (e == ExecRunning || e == ExecAwaiting || e == ExecCompleted):
		return PhaseRunning
	case c == ComputeNone && e == ExecPending:
		return PhasePending
	default:
		return PhaseUnspecified
	}
}
