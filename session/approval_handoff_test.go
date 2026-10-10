package session_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/wire"
)

// Advance the real SQLite journal between the snapshot and automatic SUSPEND append.
// Both CAS and superseded-fence failures remain errors, not successful approval pauses.
func TestPublicApprovalSuspendAppendConflictRetainsPending(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		t.Run(map[bool]string{false: "CAS", true: "fenced"}[supersede], func(t *testing.T) {
			store := openStore(t, ":memory:")
			r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			b.beforeSnapshot = func() {
				log := store.Session(uid)
				if supersede {
					if _, err := log.NewFence(); err != nil {
						t.Error(err)
					}
					return
				}
				records, err := log.Read(1)
				if err != nil {
					t.Error(err)
					return
				}
				last := records[len(records)-1]
				if _, err := log.Append(last.Seq, last.Fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleBaseline}}); err != nil {
					t.Error(err)
				}
			}
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
			if status.Code(err) != codes.Aborted {
				t.Fatalf("failed automatic append=%v", err)
			}
			if frames[len(frames)-1].GetSession() != nil {
				t.Fatal("failed append sent successful final frame")
			}
			got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil || got.PendingApproval == nil || got.ComputeState != v1.ComputeState_COMPUTE_LIVE {
				t.Fatalf("append failure projection=%v %v", got, err)
			}
			for _, rec := range routingRecords(t, store.Session(uid)) {
				if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Lifecycle != nil && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
					t.Fatalf("failed append invented event: %+v", rec.Event)
				}
			}
			b.beforeSnapshot = nil
			recovered, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
			if err != nil || recovered.ComputeState != v1.ComputeState_COMPUTE_COLD || !proto.Equal(recovered.PendingApproval, got.PendingApproval) {
				t.Fatalf("explicit retry=%v %v", recovered, err)
			}
		})
	}
}

func TestPublicApprovalDecisionOverlapUsesSharedGuard(t *testing.T) {
	store := openStore(t, ":memory:")
	r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	b.beforeSnapshot = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Exec did not reach guarded snapshot")
	}
	before, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	ref := before.PendingApproval
	if ref == nil {
		t.Fatal("no committed request before snapshot")
	}
	_, err = c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(false)})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping decision=%v", err)
	}
	if _, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping Resume=%v", err)
	}
	if _, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid}); status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping Suspend=%v", err)
	}
	after, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || after.LastSeq != before.LastSeq || !proto.Equal(after.PendingApproval, before.PendingApproval) {
		t.Fatalf("overlap changed authority=%v %v", after, err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Deliberately malformed raw peer, following harnesswire's existing Connect fixtures. The
// public host must expose actual ACK/EOF errors while retaining the committed request.
type approvalAckPeer struct {
	v1.UnimplementedHarnessServer
	mode string
}

func (approvalAckPeer) Describe(context.Context, *v1.DescribeRequest) (*v1.HarnessDescriptor, error) {
	return &v1.HarnessDescriptor{Id: "gate", Version: "v1", Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}, nil
}
func (p approvalAckPeer) Connect(stream v1.Harness_ConnectServer) error {
	start, err := stream.Recv()
	if err != nil {
		return err
	}
	call := publicGateCall("call")
	if err := stream.Send(wire.EventToProto(api.Event{ExecutionID: start.GetExecutionId(), Kind: api.EventToolCall, ToolCall: &call})); err != nil {
		return err
	}
	park, err := stream.Recv()
	if err != nil {
		return err
	}
	if p.mode == "missing" {
		return nil
	}
	ref := proto.Clone(park.GetPark()).(*v1.ApprovalRef)
	if p.mode == "mismatch" {
		ref.RequestSeq++
	}
	if err := stream.Send(&v1.Event{ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_PARKED, Body: &v1.Event_Parked{Parked: ref}}); err != nil {
		return err
	}
	if p.mode == "extra END" {
		return stream.Send(&v1.Event{ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_END})
	}
	return nil
}
func TestPublicApprovalBadAcknowledgementIsErrorNotPark(t *testing.T) {
	for _, mode := range []string{"missing", "mismatch", "extra END"} {
		t.Run(mode, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			v1.RegisterHarnessServer(server, approvalAckPeer{mode: mode})
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(server.Stop)
			b := remote.New(lis.Addr().String())
			t.Cleanup(func() { _ = b.Close() })
			r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(b, nil)})
			if err != nil {
				t.Fatal(err)
			}
			store := openStore(t, ":memory:")
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
			if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "park acknowledgement") {
				t.Fatalf("bad ACK=%v", err)
			}
			if frames[len(frames)-1].GetSession() != nil {
				t.Fatal("bad ACK sent success frame")
			}
			got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil || got.PendingApproval == nil || got.ComputeState == v1.ComputeState_COMPUTE_COLD {
				t.Fatalf("lost pending request after bad ACK: %v %v", got, err)
			}
			for _, rec := range routingRecords(t, store.Session(uid)) {
				if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Kind == api.EventParked || rec.Event.Lifecycle != nil && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
					t.Fatalf("bad ACK fabricated record: %+v", rec.Event)
				}
			}
		})
	}
}
