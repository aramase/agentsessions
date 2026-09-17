package main

import (
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/wire"
)

func TestExecHarnessFlag(t *testing.T) {
	for _, tt := range []struct {
		name    string
		session string
		harness string
	}{
		{name: "host default"},
		{name: "create chat", harness: "chat"},
		{name: "session default", session: "existing"},
		{name: "turn override", session: "existing", harness: "chat"},
		{name: "unknown harness", harness: "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan *v1.ExecRequest, 1)
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			v1.RegisterSessionsServer(srv, &execRecorder{requests: requests})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)

			args := []string{"--server", lis.Addr().String(), "--input", "hello"}
			if tt.session != "" {
				args = append(args, "--session", tt.session)
			}
			if tt.harness != "" {
				args = append(args, "--harness", tt.harness)
			}
			err = cmdExec(args)
			if tt.harness == "unknown" {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("unknown harness error = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			select {
			case req := <-requests:
				if req.GetHarness() != tt.harness || req.GetSession() != tt.session {
					t.Fatalf("exec request = %v", req)
				}
				if len(req.GetInputs()) != 1 || wire.MessageFromProto(req.Inputs[0]).Text() != "hello" {
					t.Fatalf("inputs = %v", req.GetInputs())
				}
				if req.ExpectedLastSeq != nil {
					t.Fatal("default exec unexpectedly enabled CAS")
				}
			default:
				t.Fatal("exec sent no request")
			}
		})
	}
}

type execRecorder struct {
	v1.UnimplementedSessionsServer
	requests chan *v1.ExecRequest
}

func (s *execRecorder) Exec(req *v1.ExecRequest, stream v1.Sessions_ExecServer) error {
	s.requests <- req
	if req.GetHarness() == "unknown" {
		return status.Error(codes.InvalidArgument, "unknown harness")
	}
	return stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Session{Session: &v1.Session{
		Metadata: &v1.ResourceMetadata{Uid: "session-uid"},
		Harness:  req.GetHarness(),
	}}})
}
