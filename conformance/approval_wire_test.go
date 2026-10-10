package conformance_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// This peer implements only the protobuf contract. Assertions use normative wire
// symbols and durable journal evidence, not Go park/control error sentinels.
type approvalWirePeer struct {
	v1.UnimplementedHarnessServer
	frames        chan []*v1.ControllerFrame
	wrongAck      bool
	parkRejection chan codes.Code
}

func (*approvalWirePeer) Describe(context.Context, *v1.DescribeRequest) (*v1.HarnessDescriptor, error) {
	return &v1.HarnessDescriptor{Id: "proto-gate", Version: "1", Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}, Tools: []*v1.ToolSpec{{Name: "charge", Mediation: v1.Mediation_MEDIATION_REQUIRES_APPROVAL}}}, nil
}
func (p *approvalWirePeer) Connect(s v1.Harness_ConnectServer) error {
	start, err := s.Recv()
	if err != nil {
		return err
	}
	if start.GetStart() == nil || start.GetSession() != "session" || start.GetExecutionId() == "" {
		return errors.New("invalid Start envelope")
	}
	exec := start.GetExecutionId()
	if err := s.Send(&v1.Event{ExecutionId: exec, Kind: v1.EventKind_EVENT_TOOL_CALL, Body: &v1.Event_Tool{Tool: &v1.ToolCall{Id: "call", Tool: "charge", IdempotencyKey: "key", Mediation: v1.Mediation_MEDIATION_REQUIRES_APPROVAL}}}); err != nil {
		return err
	}
	control, err := s.Recv()
	if err != nil {
		return err
	}
	if control.GetSession() != start.GetSession() || control.GetExecutionId() != exec {
		return errors.New("foreign control envelope")
	}
	if park := control.GetPark(); park != nil {
		if park.GetExecutionId() != exec || park.GetToolCallId() != "call" || park.GetRequestSeq() <= 0 {
			return errors.New("invalid park tuple")
		}
		p.frames <- []*v1.ControllerFrame{control}
		ack := &v1.ApprovalRef{ExecutionId: exec, ToolCallId: "call", RequestSeq: park.GetRequestSeq()}
		if p.wrongAck {
			ack.ToolCallId = "foreign"
		}
		if err := s.Send(&v1.Event{ExecutionId: exec, Kind: v1.EventKind_EVENT_PARKED, Body: &v1.Event_Parked{Parked: ack}}); err != nil {
			return err
		}
		if p.parkRejection != nil {
			// Hold EOF so the raw peer can distinguish rejection/cancellation from
			// a successful park, whose host would still be awaiting clean closure.
			_, err := s.Recv()
			p.parkRejection <- status.Code(err)
			return err
		}
		return nil // clean closure without END
	}
	decision := control.GetApproval()
	if decision == nil || decision.GetToolCallId() != "call" || decision.GetRequestSeq() <= 0 {
		return errors.New("expected approval before tool")
	}
	receipt, err := s.Recv()
	if err != nil {
		return err
	}
	tr := receipt.GetTool()
	if receipt.GetSession() != start.GetSession() || receipt.GetExecutionId() != exec || tr == nil || tr.GetId() != "call" || tr.GetApprovalRequestSeq() != decision.GetRequestSeq() || tr.GetApprovalDecisionSeq() <= 0 {
		return errors.New("invalid receipt tuple")
	}
	if !decision.GetApproved() {
		if tr.GetCode() != v1.ToolResult_APPROVAL_DENIED || !tr.GetIsError() {
			return errors.New("invalid denial status")
		}
	} else if !(tr.GetCode() == v1.ToolResult_CODE_UNSPECIFIED && !tr.GetIsError() || tr.GetCode() == v1.ToolResult_EXECUTOR_ERROR && tr.GetIsError()) {
		return errors.New("invalid approval status")
	}
	p.frames <- []*v1.ControllerFrame{control, receipt}
	if err := s.Send(&v1.Event{ExecutionId: exec, Kind: v1.EventKind_EVENT_OUTPUT, Body: &v1.Event_Message{Message: &v1.Message{Role: "assistant", Parts: []*v1.Part{{Part: &v1.Part_Text{Text: &v1.TextPart{Text: "continued"}}}}}}}); err != nil {
		return err
	}
	return s.Send(&v1.Event{ExecutionId: exec, Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}}})
}
func rawApprovalHarness(t *testing.T, p *approvalWirePeer) api.Harness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	v1.RegisterHarnessServer(server, p)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///approval-conformance", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); lis.Close() })
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
}

func TestWireApprovalProtoParkDecisionReceiptReplay(t *testing.T) {
	for _, shape := range []struct {
		name          string
		approved      bool
		executorError bool
		code          v1.ToolResult_Code
	}{
		{"approved", true, false, v1.ToolResult_CODE_UNSPECIFIED},
		{"denied", false, false, v1.ToolResult_APPROVAL_DENIED},
		{"approved executor failure", true, true, v1.ToolResult_EXECUTOR_ERROR},
	} {
		t.Run(shape.name, func(t *testing.T) {
			store, _ := openFile(t)
			defer store.Close()
			log := store.Session("session")
			peer := &approvalWirePeer{frames: make(chan []*v1.ControllerFrame, 4)}
			har := rawApprovalHarness(t, peer)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			effects := 0
			executor := func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				effects++
				if shape.executorError {
					return api.ToolResult{}, errors.New("executor unavailable")
				}
				return api.ToolResult{}, nil
			}
			c, err := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(executor))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Exec(ctx, har, nil, 0); err == nil {
				t.Fatal("park cannot complete the execution")
			}
			park := (<-peer.frames)[0].GetPark()
			records, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			var requestSeq int64
			for _, record := range records {
				ev := wire.EventToProto(record.Event)
				switch ev.GetKind() {
				case v1.EventKind_EVENT_APPROVAL_REQUEST:
					if ev.GetExecutionId() != park.GetExecutionId() || ev.GetApproval().GetToolCallId() != park.GetToolCallId() {
						t.Fatalf("request does not bind park: %v", ev)
					}
					requestSeq = record.Seq
				case v1.EventKind_EVENT_PARKED, v1.EventKind_EVENT_END, v1.EventKind_EVENT_ERROR, v1.EventKind_EVENT_TOOL_RESULT:
					t.Fatalf("park wrote transport/terminal/receipt event: %v", ev)
				}
			}
			if requestSeq <= 0 || requestSeq != park.GetRequestSeq() || effects != 0 {
				t.Fatalf("park request/effects = %d/%d; frame %v", requestSeq, effects, park)
			}
			committed, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: park.GetExecutionId(), ToolCallID: "call", RequestSeq: requestSeq, Approved: shape.approved, Reason: "reviewed"})
			if err != nil {
				t.Fatal(err)
			}
			// A fresh controller represents a separate invocation; wire recovery must use
			// the committed decision's actual sequence, not a fabricated positive number.
			recovering, err := controller.New(log, nil, controller.WithSessionUID("session"), controller.WithToolExecutor(executor))
			if err != nil {
				t.Fatal(err)
			}
			if resumed, err := recovering.Resume(ctx, har); err != nil || !resumed {
				t.Fatalf("Resume = %v, %v", resumed, err)
			}
			pair := <-peer.frames
			decision, receipt := pair[0].GetApproval(), pair[1].GetTool()
			if decision.GetApproved() != shape.approved || receipt.GetCode() != shape.code || receipt.GetApprovalRequestSeq() != requestSeq || receipt.GetApprovalDecisionSeq() != committed.Seq {
				t.Fatalf("decision/receipt = %v/%v, committed seq %d", decision, receipt, committed.Seq)
			}
			wantEffects := 0
			if shape.approved {
				wantEffects = 1
			}
			if effects != wantEffects {
				t.Fatalf("effects = %d, want %d", effects, wantEffects)
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if outputs, err := recovering.Replay(ctx, har); err != nil || !reflect.DeepEqual(outputs, []string{"continued"}) {
				t.Fatalf("Replay = %v, %v", outputs, err)
			}
			replayPair := <-peer.frames
			if replayPair[0].GetApproval().GetRequestSeq() != requestSeq || replayPair[1].GetTool().GetApprovalDecisionSeq() != committed.Seq {
				t.Fatalf("replayed pair = %v", replayPair)
			}
			after, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || effects != wantEffects {
				t.Fatal("replay mutated journal or invoked executor")
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWireApprovalFailedAckPreservesDurableRequest(t *testing.T) {
	store, _ := openFile(t)
	defer store.Close()
	log := store.Session("session")
	peer := &approvalWirePeer{frames: make(chan []*v1.ControllerFrame, 1), wrongAck: true, parkRejection: make(chan codes.Code, 1)}
	c, err := controller.New(log, nil, controller.WithSessionUID("session"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = c.Exec(ctx, rawApprovalHarness(t, peer), nil, 0)
	if err == nil {
		t.Fatal("foreign ack accepted")
	}
	if ctx.Err() != nil {
		t.Fatalf("ack was not rejected before the caller deadline: %v", err)
	}
	select {
	case code := <-peer.parkRejection:
		if code != codes.Canceled {
			t.Fatalf("raw peer after foreign ack received %s, want host cancellation before EOF", code)
		}
	case <-ctx.Done():
		t.Fatalf("raw peer did not observe rejection before EOF: %v", ctx.Err())
	}
	frames := <-peer.frames
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range records {
		ev := wire.EventToProto(record.Event)
		switch ev.GetKind() {
		case v1.EventKind_EVENT_APPROVAL_REQUEST:
			found = record.Seq == frames[0].GetPark().GetRequestSeq()
		case v1.EventKind_EVENT_END, v1.EventKind_EVENT_ERROR, v1.EventKind_EVENT_PARKED, v1.EventKind_EVENT_TOOL_RESULT:
			t.Fatalf("failed handoff erased awaiting state: %v", ev)
		}
	}
	if !found {
		t.Fatal("failed handoff lost committed request")
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}
