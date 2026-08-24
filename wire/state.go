package wire

import (
	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
)

// The domain types are strings and the wire types are enums, so an unknown value has to land
// somewhere. Both directions fall back to the UNSPECIFIED/zero member rather than guessing: a
// state this build does not know about is better reported as unknown than as a concrete state a
// caller would act on.

// ComputeStateToProto converts a domain ComputeState to its wire enum.
func ComputeStateToProto(c api.ComputeState) v1.ComputeState {
	switch c {
	case api.ComputeNone:
		return v1.ComputeState_COMPUTE_NONE
	case api.ComputeLive:
		return v1.ComputeState_COMPUTE_LIVE
	case api.ComputeWarm:
		return v1.ComputeState_COMPUTE_WARM
	case api.ComputeCold:
		return v1.ComputeState_COMPUTE_COLD
	case api.ComputeTerminated:
		return v1.ComputeState_COMPUTE_TERMINATED
	default:
		return v1.ComputeState_COMPUTE_STATE_UNSPECIFIED
	}
}

// ComputeStateFromProto converts a wire ComputeState enum to its domain form.
func ComputeStateFromProto(c v1.ComputeState) api.ComputeState {
	switch c {
	case v1.ComputeState_COMPUTE_NONE:
		return api.ComputeNone
	case v1.ComputeState_COMPUTE_LIVE:
		return api.ComputeLive
	case v1.ComputeState_COMPUTE_WARM:
		return api.ComputeWarm
	case v1.ComputeState_COMPUTE_COLD:
		return api.ComputeCold
	case v1.ComputeState_COMPUTE_TERMINATED:
		return api.ComputeTerminated
	default:
		return ""
	}
}

// ExecStateToProto converts a domain ExecState to its wire enum.
func ExecStateToProto(e api.ExecState) v1.ExecState {
	switch e {
	case api.ExecPending:
		return v1.ExecState_EXEC_PENDING
	case api.ExecRunning:
		return v1.ExecState_EXEC_RUNNING
	case api.ExecAwaiting:
		return v1.ExecState_EXEC_AWAITING
	case api.ExecCompleted:
		return v1.ExecState_EXEC_COMPLETED
	case api.ExecFailed:
		return v1.ExecState_EXEC_FAILED
	case api.ExecCanceled:
		return v1.ExecState_EXEC_CANCELED
	default:
		return v1.ExecState_EXEC_STATE_UNSPECIFIED
	}
}

// ExecStateFromProto converts a wire ExecState enum to its domain form.
func ExecStateFromProto(e v1.ExecState) api.ExecState {
	switch e {
	case v1.ExecState_EXEC_PENDING:
		return api.ExecPending
	case v1.ExecState_EXEC_RUNNING:
		return api.ExecRunning
	case v1.ExecState_EXEC_AWAITING:
		return api.ExecAwaiting
	case v1.ExecState_EXEC_COMPLETED:
		return api.ExecCompleted
	case v1.ExecState_EXEC_FAILED:
		return api.ExecFailed
	case v1.ExecState_EXEC_CANCELED:
		return api.ExecCanceled
	default:
		return ""
	}
}
