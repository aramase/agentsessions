package client_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func approvalSDKClient(t *testing.T, svc v1.SessionsServer) *client.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := client.Dial("passthrough:///bufnet", client.WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type sessionFrameServer struct {
	v1.UnimplementedSessionsServer
	records, fail bool
}

func (s sessionFrameServer) Exec(_ *v1.ExecRequest, stream v1.Sessions_ExecServer) error {
	if err := stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Session{Session: &v1.Session{Metadata: &v1.ResourceMetadata{Uid: "known"}, LastSeq: 10}}}); err != nil {
		return err
	}
	if s.records {
		if err := stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Record{Record: &v1.LogRecord{Seq: 11}}}); err != nil {
			return err
		}
	}
	if s.fail {
		return status.Error(codes.Aborted, "stale cursor")
	}
	return stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Session{Session: &v1.Session{Metadata: &v1.ResourceMetadata{Uid: "known"}, LastSeq: 12, ExecState: v1.ExecState_EXEC_AWAITING, PendingApproval: &v1.ApprovalRef{ExecutionId: "execution", ToolCallId: "call", RequestSeq: 12}}}})
}

// The latest Session may advance the recovery cursor beyond the streamed records. Error paths
// must retain the initial cursor as well as the UID already delivered to OnSession.
func TestApprovalSDKLatestSessionCursorAndError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		records, fail bool
		wantSeq       int64
		wantFrames    int
	}{
		{"final session beyond records", true, false, 12, 2},
		{"query without records", false, false, 12, 2},
		{"failure retains initial cursor", false, true, 10, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := approvalSDKClient(t, sessionFrameServer{records: tc.records, fail: tc.fail})
			var sessions []*v1.Session
			turn, err := c.Exec(t.Context(), client.ExecOptions{Session: "known", OnSession: func(s *v1.Session) { sessions = append(sessions, s) }})
			wantCode := codes.OK
			if tc.fail {
				wantCode = codes.Aborted
			}
			if status.Code(err) != wantCode {
				t.Fatal(err)
			}
			if turn == nil || turn.LastSeq != tc.wantSeq || turn.Session.GetLastSeq() != tc.wantSeq || len(sessions) != tc.wantFrames || turn.Session.GetMetadata().GetUid() != "known" {
				t.Fatalf("turn=%+v sessions=%v error=%v", turn, sessions, err)
			}
		})
	}
}

func TestApprovalSDKDecisionOnlyOriginalRecord(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "deny", true: "approve"}[approved], func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			b := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = b.Close() })
			r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(b, echoagent.Model)})
			if err != nil {
				t.Fatal(err)
			}
			c := approvalSDKClient(t, session.NewService(store, r))
			sess, err := c.CreateSession(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			uid := sess.GetMetadata().GetUid()
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range []api.Event{
				{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "unserved", InputCount: proto.Int64(0)}},
				{Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "call", Tool: "write", IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}},
				{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}},
			} {
				ev.ExecutionID = "execution"
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			decision := api.ApprovalDecision{ExecutionID: "execution", ToolCallID: "call", RequestSeq: 3, Approved: approved, Reason: "reviewed", Identity: api.IdentityRef{Principal: "human", Issuer: "issuer", Subject: "subject"}}
			first, err := c.Approve(t.Context(), uid, decision)
			if err != nil {
				t.Fatal(err)
			}
			retry, err := c.Approve(t.Context(), uid, decision)
			if err != nil || !proto.Equal(first.Decision, retry.GetDecision()) {
				t.Fatalf("retry=%v %v", retry, err)
			}
			rec := first.GetDecision()
			if rec.GetSeq() != 4 || rec.GetEvent().GetApprovalResult().GetApproved() != approved || rec.GetEvent().GetActor().GetSubject() != "subject" || first.Session.GetLastSeq() != 4 || first.Session.PendingApproval != nil {
				t.Fatalf("decision=%v", first)
			}
			if records, err := log.Read(1); err != nil || len(records) != 4 || records[3].Event.Kind != api.EventApprovalResult {
				t.Fatalf("SDK implicitly resumed or lost decision: %v %v", records, err)
			}
			if _, err := c.Resume(t.Context(), uid, false); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("separate recovery=%v", err)
			}
			if head, _ := log.Head(); head != 4 {
				t.Fatal("failed recovery altered committed decision")
			}
		})
	}
}
