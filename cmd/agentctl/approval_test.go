package main

import (
	"context"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/aramase/agentsessions/api/genpb"
)

// A remote server makes request ordering/explicit bool presence observable without adding
// injection hooks to the production CLI. The committed response retains a historical record.
type approvalCLIRecorder struct {
	v1.UnimplementedSessionsServer
	pending   *v1.ApprovalRef
	requests  []*v1.ApproveRequest
	calls     []string
	resumeErr error
}

func (s *approvalCLIRecorder) GetSession(_ context.Context, q *v1.GetSessionRequest) (*v1.Session, error) {
	s.calls = append(s.calls, "get")
	return &v1.Session{Metadata: &v1.ResourceMetadata{Uid: q.Uid}, LastSeq: 10, PendingApproval: s.pending}, nil
}
func (s *approvalCLIRecorder) Approve(_ context.Context, q *v1.ApproveRequest) (*v1.ApproveResponse, error) {
	s.calls = append(s.calls, "decision")
	s.requests = append(s.requests, q)
	return &v1.ApproveResponse{Session: &v1.Session{Metadata: &v1.ResourceMetadata{Uid: q.Session}, LastSeq: 11}, Decision: &v1.LogRecord{Seq: 11, ContentHash: "original-hash", Event: &v1.Event{Kind: v1.EventKind_EVENT_APPROVAL_RESULT, ExecutionId: q.ExecutionId, Actor: q.Identity, Body: &v1.Event_ApprovalResult{ApprovalResult: &v1.ApprovalResult{ToolCallId: q.ToolCallId, RequestSeq: q.RequestSeq, Approved: q.GetApproved(), Reason: q.Reason}}}}}, nil
}
func (s *approvalCLIRecorder) Resume(_ context.Context, q *v1.ResumeRequest) (*v1.Session, error) {
	s.calls = append(s.calls, "resume")
	if s.resumeErr != nil {
		return nil, s.resumeErr
	}
	return &v1.Session{Metadata: &v1.ResourceMetadata{Uid: q.Session}, LastSeq: 15, ComputeState: v1.ComputeState_COMPUTE_LIVE}, nil
}
func serveApprovalCLI(t *testing.T, s v1.SessionsServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}
func captureApprovalOutput(t *testing.T, run func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old; _ = r.Close() }()
	runErr := run()
	_ = w.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), runErr
}

func TestCLIApproveAndDenyDecisionThenResume(t *testing.T) {
	for _, tc := range []struct {
		name     string
		run      func([]string) error
		approved bool
	}{{"approve", cmdApprove, true}, {"deny", cmdDeny, false}} {
		for _, explicit := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: " discover", true: " explicit"}[explicit], func(t *testing.T) {
				s := &approvalCLIRecorder{pending: &v1.ApprovalRef{ExecutionId: "execution", ToolCallId: "call", RequestSeq: 9}}
				addr := serveApprovalCLI(t, s)
				args := []string{"--server", addr, "--reason", "reviewed", "--actor", "human", "--identity-issuer", "issuer", "--identity-subject", "subject"}
				if explicit {
					args = append(args, "--session", "uid", "--execution", "execution", "--tool-call", "call", "--request-seq", "9")
				} else {
					args = append(args, "uid")
				}
				_, err := captureApprovalOutput(t, func() error { return tc.run(args) })
				if err != nil {
					t.Fatal(err)
				}
				wantCalls := "get,decision,resume"
				if explicit {
					wantCalls = "decision,resume"
				}
				if strings.Join(s.calls, ",") != wantCalls || len(s.requests) != 1 {
					t.Fatalf("calls=%v requests=%v", s.calls, s.requests)
				}
				q := s.requests[0]
				if q.Session != "uid" || q.ExecutionId != "execution" || q.ToolCallId != "call" || q.RequestSeq != 9 || q.Approved == nil || q.GetApproved() != tc.approved || q.Reason != "reviewed" || !proto.Equal(q.Identity, &v1.IdentityRef{Principal: "human", Issuer: "issuer", Subject: "subject"}) {
					t.Fatalf("decision request=%v", q)
				}
			})
		}
	}
}

func TestCLIApprovalCommittedRecoveryFailureAndExplicitRetry(t *testing.T) {
	for _, run := range []func([]string) error{cmdApprove, cmdDeny} {
		s := &approvalCLIRecorder{resumeErr: status.Error(codes.Unavailable, "compute offline")}
		addr := serveApprovalCLI(t, s)
		// Full correlation retries must work without a currently discoverable pending request.
		args := []string{"uid", "--server", addr, "--execution", "execution", "--tool-call", "call", "--request-seq", "9"}
		_, err := captureApprovalOutput(t, func() error { return run(args) })
		if err == nil || !strings.Contains(err.Error(), "decision committed") || !strings.Contains(err.Error(), "compute offline") || status.Code(err) != codes.Unavailable {
			t.Fatalf("recovery failure=%v", err)
		}
		if strings.Join(s.calls, ",") != "decision,resume" || len(s.requests) != 1 {
			t.Fatalf("failure retracted/retried/discovered decision: %v", s.calls)
		}
	}
}

func TestCLIApprovalRejectsPartialTupleAndMissingPending(t *testing.T) {
	for _, args := range [][]string{{"--session", "uid", "--execution", "execution"}, {"--session", "uid", "--tool-call", "call"}, {"--session", "uid", "--request-seq", "9"}, {"--session", "uid", "--execution", "execution", "--tool-call", "call", "--request-seq", "0"}, {}, {"--session", "uid", "other"}} {
		s := &approvalCLIRecorder{}
		addr := serveApprovalCLI(t, s)
		_, err := captureApprovalOutput(t, func() error { return cmdApprove(append([]string{"--server", addr}, args...)) })
		if err == nil || len(s.calls) != 0 {
			t.Fatalf("invalid args=%v error=%v calls=%v", args, err, s.calls)
		}
	}
	s := &approvalCLIRecorder{}
	addr := serveApprovalCLI(t, s)
	_, err := captureApprovalOutput(t, func() error { return cmdDeny([]string{"--server", addr, "--session", "uid"}) })
	if err == nil || !strings.Contains(err.Error(), "Resume") || strings.Join(s.calls, ",") != "get" {
		t.Fatalf("no pending=%v calls=%v", err, s.calls)
	}
}

func TestCLIApprovalJSONAndHelp(t *testing.T) {
	s := &approvalCLIRecorder{pending: &v1.ApprovalRef{ExecutionId: "execution", ToolCallId: "call", RequestSeq: 9}}
	addr := serveApprovalCLI(t, s)
	output, err := captureApprovalOutput(t, func() error { return cmdDeny([]string{"--server", addr, "--session", "uid", "--json"}) })
	if err != nil {
		t.Fatal(err)
	}
	var got v1.ApproveResponse
	if err := protojson.Unmarshal([]byte(output), &got); err != nil {
		t.Fatal(err)
	}
	if got.Decision.GetContentHash() != "original-hash" || got.Decision.GetSeq() != 11 || got.Session.GetLastSeq() != 15 {
		t.Fatalf("JSON lost original decision/current cursor: %v", &got)
	}
	for _, run := range []func([]string) error{cmdApprove, cmdDeny} {
		if err := run([]string{"--help"}); err != nil {
			t.Fatalf("help=%v", err)
		}
	}
}
