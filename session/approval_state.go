package session

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

// approvalAwaiting derives unanswered requests in the latest first-seen execution at the
// returned cursor. Lifecycle records are session-scoped; late records for older executions
// cannot select them again. This is reporting evidence, not permission to execute a tool.
func approvalAwaiting(records []eventlog.Record, throughSeq int64) bool {
	seen := make(map[string]bool)
	var selected string
	var pending map[string]bool
	finished := false
	for _, record := range records {
		if record.Seq > throughSeq {
			break
		}
		event := record.Event
		if event.Kind == api.EventLifecycle || event.ExecutionID == "" {
			continue
		}
		if !seen[event.ExecutionID] {
			seen[event.ExecutionID] = true
			selected = event.ExecutionID
			pending = make(map[string]bool)
			finished = false
		}
		if event.ExecutionID != selected {
			continue
		}
		switch event.Kind {
		case api.EventApprovalRequest:
			if event.Approval != nil && event.Approval.ToolCallID != "" {
				pending[event.Approval.ToolCallID] = true
			}
		case api.EventApprovalResult:
			if event.ApprovalResult != nil {
				delete(pending, event.ApprovalResult.ToolCallID)
			}
		case api.EventEnd:
			finished = true
		}
	}
	return !finished && len(pending) > 0
}

// querySessionProto adds approval reporting only to Get/List. Mutation responses retain the
// metadata-only rendering, avoiding a new journal read that could fail after a side effect.
func (s *Service) querySessionProto(info sqlitelog.SessionInfo) (*v1.Session, error) {
	out, err := sessionProto(info)
	if err != nil || info.LastSeq == 0 {
		return out, err
	}
	records, err := s.store.Session(info.UID).Read(1)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "session %q: read approval state: %v", info.UID, err)
	}
	if approvalAwaiting(records, info.LastSeq) {
		out.ExecState = v1.ExecState_EXEC_AWAITING
	}
	return out, nil
}
