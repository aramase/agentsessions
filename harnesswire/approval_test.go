package harnesswire_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harnesswire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Every approval test traverses a real gRPC Connect. Raw peers exercise the public
// envelope; Go harnesses also exercise the transient sink reply and sealing.
type connectPeer struct {
	v1.UnimplementedHarnessServer
	run func(v1.Harness_ConnectServer) error
}

func (p connectPeer) Connect(s v1.Harness_ConnectServer) error { return p.run(s) }

func approvalClient(t *testing.T, server v1.HarnessServer) v1.HarnessClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	v1.RegisterHarnessServer(gs, server)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///approval", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); lis.Close() })
	return v1.NewHarnessClient(conn)
}
func approvalContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type gateHarness struct {
	run func(context.Context, *api.Start, api.EventSink) error
}

func (gateHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "gate"}, nil
}
func (h gateHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	return h.run(ctx, start, sink)
}

var gateCall = api.ToolCall{ID: "call", Tool: "charge", Mediation: api.MediationRequiresApproval}
var gateRef = api.ApprovalRef{ExecutionID: "exec", ToolCallID: "call", RequestSeq: 17}

type gateHost struct {
	hostSink
	result  api.ToolResult
	err     error
	outputs []string
	reports int
	calls   int
}

func (h *gateHost) ToolCall(context.Context, api.ToolCall) (api.ToolResult, error) {
	h.calls++
	return h.result, h.err
}
func (h *gateHost) Output(_ context.Context, s string) error {
	h.outputs = append(h.outputs, s)
	return nil
}
func (h *gateHost) Report(context.Context, api.ToolResult) error { h.reports++; return nil }

func gateToolEvent() *v1.Event {
	return &v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_TOOL_CALL, Body: &v1.Event_Tool{Tool: &v1.ToolCall{Id: "call", Tool: "charge", Mediation: v1.Mediation_MEDIATION_REQUIRES_APPROVAL}}}
}
func parkFrame() *v1.ControllerFrame {
	return &v1.ControllerFrame{Session: "session", ExecutionId: "exec", Frame: &v1.ControllerFrame_Park{Park: &v1.ApprovalRef{ExecutionId: "exec", ToolCallId: "call", RequestSeq: 17}}}
}
func ackEvent() *v1.Event {
	return &v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_PARKED, Body: &v1.Event_Parked{Parked: &v1.ApprovalRef{ExecutionId: "exec", ToolCallId: "call", RequestSeq: 17}}}
}
func decisionFrame(approved bool) *v1.ControllerFrame {
	return &v1.ControllerFrame{Session: "session", ExecutionId: "exec", Frame: &v1.ControllerFrame_Approval{Approval: &v1.ApprovalResult{ToolCallId: "call", RequestSeq: 17, Approved: approved, Reason: "reviewed"}}}
}
func receiptFrame() *v1.ControllerFrame {
	return &v1.ControllerFrame{Session: "session", ExecutionId: "exec", Frame: &v1.ControllerFrame_Tool{Tool: &v1.ToolResult{Id: "call", ApprovalRequestSeq: 17, ApprovalDecisionSeq: 19}}}
}
func openGate(t *testing.T, client v1.HarnessClient, session string) v1.Harness_ConnectClient {
	t.Helper()
	s, err := client.Connect(approvalContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(&v1.ControllerFrame{Session: session, ExecutionId: "exec", Frame: &v1.ControllerFrame_Start{Start: &v1.Start{}}}); err != nil {
		t.Fatal(err)
	}
	ev, err := s.Recv()
	if err != nil || ev.GetKind() != v1.EventKind_EVENT_TOOL_CALL || ev.GetTool().GetId() != "call" {
		t.Fatalf("tool event = %v, %v", ev, err)
	}
	return s
}

// Dropping the typed park before sending control, emitting END, or forgetting the
// Start session breaks this actual transport round trip.
func TestApprovalParkRoundTrip(t *testing.T) {
	starts := make(chan *api.Start, 1)
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, start *api.Start, sink api.EventSink) error {
		starts <- start
		_, err := sink.ToolCall(ctx, gateCall)
		return err
	}})
	h := harnesswire.NewClientHarness(approvalClient(t, server))
	host := &gateHost{err: fmt.Errorf("durable request: %w", &api.ApprovalParkedError{Ref: gateRef})}
	err := h.Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, host)
	var parked *api.ApprovalParkedError
	if !errors.Is(err, api.ErrApprovalParked) || !errors.As(err, &parked) || parked.Ref != gateRef {
		t.Fatalf("Run = %v, want correlated typed park", err)
	}
	if start := <-starts; start.SessionUID != "session" {
		t.Fatalf("Start session = %q", start.SessionUID)
	}
	if host.calls != 1 || host.reports != 0 {
		t.Fatalf("host calls/reports = %d/%d", host.calls, host.reports)
	}
}

func TestApprovalParkServerAckAndEOF(t *testing.T) {
	for _, swallow := range []bool{false, true} {
		t.Run(fmt.Sprint("swallow=", swallow), func(t *testing.T) {
			late := make(chan []error, 1)
			server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				_, err := sink.ToolCall(ctx, gateCall)
				if !swallow {
					return err
				}
				_, modelErr := sink.Model(ctx, api.ModelRequest{Model: "m"})
				_, toolErr := sink.ToolCall(ctx, gateCall)
				late <- []error{sink.Output(ctx, "late"), modelErr, toolErr, sink.Report(ctx, api.ToolResult{ID: "ordinary"}), sink.Usage(ctx, api.Usage{Model: "m"})}
				return nil
			}})
			stream := openGate(t, approvalClient(t, server), "session")
			if err := stream.Send(parkFrame()); err != nil {
				t.Fatal(err)
			}
			ev, err := stream.Recv()
			if err != nil || ev.GetKind() != v1.EventKind_EVENT_PARKED || ev.GetExecutionId() != "exec" || ev.GetParked().GetExecutionId() != "exec" || ev.GetParked().GetToolCallId() != "call" || ev.GetParked().GetRequestSeq() != 17 {
				t.Fatalf("park ack = %v, %v", ev, err)
			}
			if ev, err := stream.Recv(); err != io.EOF {
				t.Fatalf("after parked = %v, %v; want actual EOF, no END/effect", ev, err)
			}
			if swallow {
				for _, err := range <-late {
					if !errors.Is(err, api.ErrApprovalParked) {
						t.Fatalf("late operation = %v, want sealed park", err)
					}
				}
			}
		})
	}
}

func TestApprovalParkDistinctRunFailure(t *testing.T) {
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		_, _ = sink.ToolCall(ctx, gateCall)
		return status.Error(codes.Unavailable, "harness unwind failed")
	}})
	stream := openGate(t, approvalClient(t, server), "session")
	if err := stream.Send(parkFrame()); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("failed handoff = %v, %v; want Unavailable", ev, err)
	}
}

func TestApprovalParkJoinedRunFailure(t *testing.T) {
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		_, parked := sink.ToolCall(ctx, gateCall)
		return errors.Join(parked, status.Error(codes.Unavailable, "distinct unwind failure"))
	}})
	stream := openGate(t, approvalClient(t, server), "session")
	if err := stream.Send(parkFrame()); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("joined unwind failure = %v, %v; want Unavailable, no parked success", ev, err)
	}
}

func TestApprovalClientRefusesMissingGateNamespaceBeforeHostCall(t *testing.T) {
	for _, emptyCall := range []bool{false, true} {
		t.Run(fmt.Sprint("empty call=", emptyCall), func(t *testing.T) {
			session := ""
			if emptyCall {
				session = "session"
			}
			peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
				if _, err := s.Recv(); err != nil {
					return err
				}
				ev := gateToolEvent()
				if emptyCall {
					ev.GetTool().Id = ""
				}
				if err := s.Send(ev); err != nil {
					return err
				}
				_, err := s.Recv()
				return err
			}}
			host := &gateHost{result: api.ToolResult{ID: "call", ApprovalRequestSeq: 17, ApprovalDecisionSeq: 19, Approval: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 17, Approved: true}}}
			err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(approvalContext(t), &api.Start{SessionUID: session, ExecutionID: "exec"}, host)
			if err == nil || !strings.Contains(err.Error(), "requires session and call id") {
				t.Fatalf("missing gate namespace = %v", err)
			}
			if host.calls != 0 {
				t.Fatalf("invalid gate namespace reached host/executor: %d calls", host.calls)
			}
		})
	}
}

func TestApprovalParkClientRequiresAckAndClosure(t *testing.T) {
	tests := []struct {
		name  string
		reply func(v1.Harness_ConnectServer) error
		want  string
		code  codes.Code
	}{
		{name: "missing ack", reply: func(v1.Harness_ConnectServer) error { return nil }, want: "acknowledgement"},
		{name: "wrong ack execution", reply: func(s v1.Harness_ConnectServer) error { a := ackEvent(); a.ExecutionId = "foreign"; return s.Send(a) }, want: "acknowledgement"},
		{name: "wrong ack body execution", reply: func(s v1.Harness_ConnectServer) error {
			a := ackEvent()
			a.GetParked().ExecutionId = "foreign"
			return s.Send(a)
		}, want: "acknowledgement"},
		{name: "wrong ack call", reply: func(s v1.Harness_ConnectServer) error {
			a := ackEvent()
			a.GetParked().ToolCallId = "foreign"
			return s.Send(a)
		}, want: "acknowledgement"},
		{name: "wrong ack request", reply: func(s v1.Harness_ConnectServer) error {
			a := ackEvent()
			a.GetParked().RequestSeq = 18
			return s.Send(a)
		}, want: "acknowledgement"},
		{name: "bare ack", reply: func(s v1.Harness_ConnectServer) error {
			return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_PARKED})
		}, want: "acknowledgement"},
		{name: "END instead", reply: func(s v1.Harness_ConnectServer) error {
			return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_END})
		}, want: "acknowledgement"},
		{name: "extra END", reply: func(s v1.Harness_ConnectServer) error {
			if err := s.Send(ackEvent()); err != nil {
				return err
			}
			return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_END})
		}, want: "closure"},
		{name: "late effect", reply: func(s v1.Harness_ConnectServer) error {
			if err := s.Send(ackEvent()); err != nil {
				return err
			}
			return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_OUTPUT})
		}, want: "closure"},
		{name: "canceled EOF", reply: func(s v1.Harness_ConnectServer) error {
			if err := s.Send(ackEvent()); err != nil {
				return err
			}
			return status.Error(codes.Canceled, "unwind canceled")
		}, code: codes.Canceled},
		{name: "stalled ack", reply: func(s v1.Harness_ConnectServer) error { <-s.Context().Done(); return s.Context().Err() }, code: codes.DeadlineExceeded},
		{name: "stalled closure", reply: func(s v1.Harness_ConnectServer) error {
			if err := s.Send(ackEvent()); err != nil {
				return err
			}
			<-s.Context().Done()
			return s.Context().Err()
		}, code: codes.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotControl := make(chan *v1.ControllerFrame, 1)
			peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
				if _, err := s.Recv(); err != nil {
					return err
				}
				if err := s.Send(gateToolEvent()); err != nil {
					return err
				}
				frame, err := s.Recv()
				if err != nil {
					return err
				}
				gotControl <- frame
				return test.reply(s)
			}}
			ctx := approvalContext(t)
			if test.code == codes.DeadlineExceeded {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(ctx, &api.Start{SessionUID: "session", ExecutionID: "exec"}, &gateHost{err: &api.ApprovalParkedError{Ref: gateRef}})
			if err == nil || errors.Is(err, api.ErrApprovalParked) {
				t.Fatalf("Run = %v; invalid handoff cannot be parked success", err)
			}
			if test.want != "" && !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run = %v, want %s failure", err, test.want)
			}
			if test.code != codes.OK && status.Code(err) != test.code {
				t.Fatalf("Run = %v, want %s", err, test.code)
			}
			select {
			case f := <-gotControl:
				if f.GetSession() != "session" || f.GetExecutionId() != "exec" || f.GetPark().GetRequestSeq() != 17 {
					t.Fatalf("park control = %v", f)
				}
			default:
				t.Fatal("host never sent park control")
			}
		})
	}
}

func TestApprovalReplyRoundTrip(t *testing.T) {
	for _, shape := range []struct {
		name     string
		approved bool
		code     api.ToolResultCode
		isError  bool
	}{
		{"success", true, api.ToolResultCodeUnspecified, false},
		{"executor error", true, api.ToolResultCodeExecutorError, true},
		{"denied", false, api.ToolResultCodeApprovalDenied, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			got := make(chan api.ToolResult, 1)
			server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				res, err := sink.ToolCall(ctx, gateCall)
				if err != nil {
					return err
				}
				got <- res
				return sink.Output(ctx, "continued")
			}})
			decision := &api.ApprovalResult{ToolCallID: "call", RequestSeq: 17, Approved: shape.approved, Reason: "reviewed"}
			result := api.ToolResult{ID: "call", Code: shape.code, IsError: shape.isError, ApprovalRequestSeq: 17, ApprovalDecisionSeq: 19, Approval: decision}
			host := &gateHost{result: result}
			err := harnesswire.NewClientHarness(approvalClient(t, server)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, host)
			if err != nil {
				t.Fatalf("Run = %v", err)
			}
			res := <-got
			if !reflect.DeepEqual(res.Approval, decision) || res.ApprovalDecisionSeq != 19 || res.Code != shape.code || res.IsError != shape.isError {
				t.Fatalf("Go reply = %#v", res)
			}
			if !reflect.DeepEqual(host.outputs, []string{"continued"}) {
				t.Fatalf("continuation = %v", host.outputs)
			}
		})
	}
}

func TestApprovalClientFrameOrder(t *testing.T) {
	observed := make(chan []*v1.ControllerFrame, 1)
	peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
		start, err := s.Recv()
		if err != nil {
			return err
		}
		if err := s.Send(gateToolEvent()); err != nil {
			return err
		}
		decision, err := s.Recv()
		if err != nil {
			return err
		}
		receipt, err := s.Recv()
		if err != nil {
			return err
		}
		observed <- []*v1.ControllerFrame{start, decision, receipt}
		return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}}})
	}}
	host := &gateHost{result: api.ToolResult{ID: "call", ApprovalRequestSeq: 17, ApprovalDecisionSeq: 19, Approval: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 17, Approved: true}}}
	err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, host)
	if err != nil {
		t.Fatal(err)
	}
	frames := <-observed
	for _, f := range frames {
		if f.GetSession() != "session" || f.GetExecutionId() != "exec" {
			t.Fatalf("envelope = %v", f)
		}
	}
	if frames[1].GetApproval() == nil || frames[1].GetApproval().GetRequestSeq() != 17 || frames[2].GetTool() == nil || frames[2].GetTool().GetApprovalDecisionSeq() != 19 {
		t.Fatalf("approval-before-tool pair = %v", frames)
	}
}

func TestApprovalServerRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name    string
		session string
		frames  func() []*v1.ControllerFrame
		want    string
	}{
		{"empty start session", "", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{parkFrame()} }, "session"},
		{"wrong park session", "session", func() []*v1.ControllerFrame { p := parkFrame(); p.Session = "foreign"; return []*v1.ControllerFrame{p} }, "session"},
		{"empty park session", "session", func() []*v1.ControllerFrame { p := parkFrame(); p.Session = ""; return []*v1.ControllerFrame{p} }, "session"},
		{"wrong park envelope execution", "session", func() []*v1.ControllerFrame {
			p := parkFrame()
			p.ExecutionId = "foreign"
			return []*v1.ControllerFrame{p}
		}, "execution_id"},
		{"wrong park body execution", "session", func() []*v1.ControllerFrame {
			p := parkFrame()
			p.GetPark().ExecutionId = "foreign"
			return []*v1.ControllerFrame{p}
		}, "park"},
		{"wrong park call", "session", func() []*v1.ControllerFrame {
			p := parkFrame()
			p.GetPark().ToolCallId = "foreign"
			return []*v1.ControllerFrame{p}
		}, "park"},
		{"zero park request", "session", func() []*v1.ControllerFrame {
			p := parkFrame()
			p.GetPark().RequestSeq = 0
			return []*v1.ControllerFrame{p}
		}, "park"},
		{"bare park", "session", func() []*v1.ControllerFrame {
			p := parkFrame()
			p.Frame = &v1.ControllerFrame_Park{}
			return []*v1.ControllerFrame{p}
		}, "park"},
		{"tool before approval", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{receiptFrame()} }, "approval"},
		{"wrong approval session", "session", func() []*v1.ControllerFrame {
			p := decisionFrame(true)
			p.Session = "foreign"
			return []*v1.ControllerFrame{p}
		}, "session"},
		{"wrong approval call", "session", func() []*v1.ControllerFrame {
			p := decisionFrame(true)
			p.GetApproval().ToolCallId = "foreign"
			return []*v1.ControllerFrame{p}
		}, "approval"},
		{"zero approval request", "session", func() []*v1.ControllerFrame {
			p := decisionFrame(true)
			p.GetApproval().RequestSeq = 0
			return []*v1.ControllerFrame{p}
		}, "approval"},
		{"bare approval", "session", func() []*v1.ControllerFrame {
			p := decisionFrame(true)
			p.Frame = &v1.ControllerFrame_Approval{}
			return []*v1.ControllerFrame{p}
		}, "approval"},
		{"duplicate approval", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{decisionFrame(true), decisionFrame(true)} }, "ToolResult"},
		{"conflicting approval", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{decisionFrame(true), decisionFrame(false)} }, "ToolResult"},
		{"park after approval", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{decisionFrame(true), parkFrame()} }, "ToolResult"},
		{"wrong receipt session", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.Session = "foreign"
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "session"},
		{"wrong receipt execution", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.ExecutionId = "foreign"
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "execution_id"},
		{"wrong receipt call", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().Id = "foreign"
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "correlation"},
		{"wrong receipt request", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().ApprovalRequestSeq = 18
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "receipt"},
		{"zero receipt decision", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().ApprovalDecisionSeq = 0
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "receipt"},
		{"denied success", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{decisionFrame(false), receiptFrame()} }, "status"},
		{"approved denial", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().Code = v1.ToolResult_APPROVAL_DENIED
			p.GetTool().IsError = true
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "status"},
		{"approved unclassified error", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().IsError = true
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "status"},
		{"executor code without error", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().Code = v1.ToolResult_EXECUTOR_ERROR
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "status"},
		{"unknown receipt code", "session", func() []*v1.ControllerFrame {
			p := receiptFrame()
			p.GetTool().Code = v1.ToolResult_Code(99)
			return []*v1.ControllerFrame{decisionFrame(true), p}
		}, "status"},
		{"closure between frames", "session", func() []*v1.ControllerFrame { return []*v1.ControllerFrame{decisionFrame(true)} }, "EOF"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan error, 1)
			server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				_, err := sink.ToolCall(ctx, gateCall)
				observed <- err
				return err
			}})
			stream := openGate(t, approvalClient(t, server), test.session)
			for _, frame := range test.frames() {
				if err := stream.Send(frame); err != nil {
					t.Fatal(err)
				}
			}
			stream.CloseSend()
			// A failure END may carry diagnostics, but no parked or completed reply is legal.
			for {
				ev, err := stream.Recv()
				if err != nil {
					break
				}
				if ev.GetKind() == v1.EventKind_EVENT_PARKED || ev.GetKind() == v1.EventKind_EVENT_END && ev.GetEnd().GetState() == "COMPLETED" {
					t.Fatalf("invalid control accepted: %v", ev)
				}
			}
			err := <-observed
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ToolCall = %v, want %s refusal", err, test.want)
			}
		})
	}
}

func TestApprovalClientRejectsInvalidHostReplies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*api.ToolResult)
		want   string
	}{
		{"missing decision", func(r *api.ToolResult) { r.Approval = nil }, "approval"},
		{"foreign decision call", func(r *api.ToolResult) { r.Approval.ToolCallID = "foreign" }, "correlation"},
		{"zero decision request", func(r *api.ToolResult) { r.Approval.RequestSeq = 0 }, "approval"},
		{"foreign receipt call", func(r *api.ToolResult) { r.ID = "foreign" }, "correlation"},
		{"foreign receipt request", func(r *api.ToolResult) { r.ApprovalRequestSeq = 18 }, "receipt"},
		{"negative receipt request", func(r *api.ToolResult) { r.ApprovalRequestSeq = -1 }, "receipt"},
		{"zero receipt decision", func(r *api.ToolResult) { r.ApprovalDecisionSeq = 0 }, "receipt"},
		{"negative receipt decision", func(r *api.ToolResult) { r.ApprovalDecisionSeq = -1 }, "receipt"},
		{"denied success", func(r *api.ToolResult) { r.Approval.Approved = false }, "status"},
		{"denied executor error", func(r *api.ToolResult) {
			r.Approval.Approved = false
			r.Code = api.ToolResultCodeExecutorError
			r.IsError = true
		}, "status"},
		{"approved denial", func(r *api.ToolResult) { r.Code = api.ToolResultCodeApprovalDenied; r.IsError = true }, "status"},
		{"approved legacy error", func(r *api.ToolResult) { r.IsError = true }, "status"},
		{"executor code without error", func(r *api.ToolResult) { r.Code = api.ToolResultCodeExecutorError }, "status"},
		{"unknown code", func(r *api.ToolResult) { r.Code = api.ToolResultCode(99) }, "status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frames := make(chan *v1.ControllerFrame, 1)
			peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
				if _, err := s.Recv(); err != nil {
					return err
				}
				if err := s.Send(gateToolEvent()); err != nil {
					return err
				}
				f, err := s.Recv()
				frames <- f
				return err
			}}
			result := api.ToolResult{ID: "call", ApprovalRequestSeq: 17, ApprovalDecisionSeq: 19, Approval: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 17, Approved: true}}
			test.mutate(&result)
			err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, &gateHost{result: result})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("host reply = %v, want %s refusal", err, test.want)
			}
			select {
			case f := <-frames:
				if f != nil {
					t.Fatalf("invalid host result sent a control frame: %v", f)
				}
			case <-time.After(time.Second):
				t.Fatal("peer receive did not drain")
			}
		})
	}
}

func TestApprovalClientRejectsInvalidHostPark(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"bare sentinel", api.ErrApprovalParked},
		{"foreign execution", &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "foreign", ToolCallID: "call", RequestSeq: 17}}},
		{"foreign call", &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "exec", ToolCallID: "foreign", RequestSeq: 17}}},
		{"zero request", &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "exec", ToolCallID: "call"}}},
		{"negative request", &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "exec", ToolCallID: "call", RequestSeq: -1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
				if _, err := s.Recv(); err != nil {
					return err
				}
				if err := s.Send(gateToolEvent()); err != nil {
					return err
				}
				_, err := s.Recv()
				return err
			}}
			err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, &gateHost{err: test.err})
			if err == nil || errors.Is(err, api.ErrApprovalParked) || !strings.Contains(err.Error(), "park") {
				t.Fatalf("invalid host park = %v", err)
			}
		})
	}
}

func TestApprovalServerReportForgeryIsSticky(t *testing.T) {
	for _, test := range []struct {
		name   string
		result api.ToolResult
	}{
		{"transient", api.ToolResult{Approval: &api.ApprovalResult{ToolCallID: "call", RequestSeq: 17, Approved: true}}},
		{"request", api.ToolResult{ApprovalRequestSeq: 17}},
		{"decision", api.ToolResult{ApprovalDecisionSeq: 19}},
		{"denial", api.ToolResult{Code: api.ToolResultCodeApprovalDenied, IsError: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan []error, 1)
			server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				observed <- []error{sink.Report(ctx, test.result), sink.Output(ctx, "forged continuation")}
				return nil
			}})
			host := &gateHost{}
			err := harnesswire.NewClientHarness(approvalClient(t, server)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, host)
			if err == nil || !strings.Contains(err.Error(), "host-owned") {
				t.Fatalf("swallowed forged Report = %v", err)
			}
			for _, err := range <-observed {
				if err == nil || !strings.Contains(err.Error(), "host-owned") {
					t.Fatalf("sink refusal = %v", err)
				}
			}
			if host.reports != 0 || len(host.outputs) != 0 {
				t.Fatalf("forged receipt/effect reached host: %#v", host)
			}
		})
	}
}

func TestApprovalServerRejectsParkOnOrdinaryTool(t *testing.T) {
	observed := make(chan error, 1)
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		call := gateCall
		call.Mediation = api.MediationControllerMediated
		_, err := sink.ToolCall(ctx, call)
		observed <- err
		return nil
	}})
	stream := openGate(t, approvalClient(t, server), "session")
	if err := stream.Send(parkFrame()); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if err != nil || ev.GetKind() != v1.EventKind_EVENT_END || ev.GetEnd().GetState() != "FAILED" {
		t.Fatalf("ordinary park response = %v, %v", ev, err)
	}
	if err := <-observed; err == nil || !strings.Contains(err.Error(), "ToolResult") {
		t.Fatalf("ordinary park = %v", err)
	}
}

func TestApprovalServerCanceledBetweenFrames(t *testing.T) {
	observed := make(chan error, 1)
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		_, err := sink.ToolCall(ctx, gateCall)
		observed <- err
		return err
	}})
	ctx, cancel := context.WithCancel(approvalContext(t))
	stream, err := approvalClient(t, server).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&v1.ControllerFrame{Session: "session", ExecutionId: "exec", Frame: &v1.ControllerFrame_Start{Start: &v1.Start{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(decisionFrame(true)); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled awaiting receipt = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled ToolCall did not drain")
	}
}

func TestApprovalServerConcurrentParkSealsEffects(t *testing.T) {
	release := make(chan struct{})
	observed := make(chan error, 1)
	server := harnesswire.NewServer(gateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		toolErr := make(chan error, 1)
		go func() { _, err := sink.ToolCall(ctx, gateCall); toolErr <- err }()
		<-release // the peer has received TOOL_CALL: ToolCall owns the effect lock
		outputErr := make(chan error, 1)
		go func() { outputErr <- sink.Output(ctx, "concurrent late effect") }()
		if err := <-toolErr; !errors.Is(err, api.ErrApprovalParked) {
			return err
		}
		observed <- <-outputErr
		return nil
	}})
	stream := openGate(t, approvalClient(t, server), "session")
	close(release)
	if err := stream.Send(parkFrame()); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if err != nil || ev.GetKind() != v1.EventKind_EVENT_PARKED {
		t.Fatalf("park = %v, %v", ev, err)
	}
	if ev, err := stream.Recv(); err != io.EOF {
		t.Fatalf("concurrent park closure = %v, %v", ev, err)
	}
	if err := <-observed; !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("concurrent effect = %v", err)
	}
}

func TestServerNormalClosureInterruptsConcurrentAwait(t *testing.T) {
	for _, operation := range []struct {
		name string
		kind v1.EventKind
		call func(api.EventSink) error
	}{
		{"model", v1.EventKind_EVENT_MODEL_CALL, func(sink api.EventSink) error {
			_, err := sink.Model(context.Background(), api.ModelRequest{Model: "m"})
			return err
		}},
		{"ordinary tool", v1.EventKind_EVENT_TOOL_CALL, func(sink api.EventSink) error {
			call := gateCall
			call.Mediation = api.MediationControllerMediated
			_, err := sink.ToolCall(context.Background(), call)
			return err
		}},
		{"approval tool", v1.EventKind_EVENT_TOOL_CALL, func(sink api.EventSink) error {
			_, err := sink.ToolCall(context.Background(), gateCall)
			return err
		}},
	} {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failed=%t", operation.name, failed), func(t *testing.T) {
				var runErr error
				if failed {
					runErr = status.Error(codes.Unavailable, "actual unwind failure")
				}
				release := make(chan struct{})
				observed := make(chan error, 1)
				server := harnesswire.NewServer(gateHarness{run: func(_ context.Context, _ *api.Start, sink api.EventSink) error {
					go func() { observed <- operation.call(sink) }()
					<-release
					return runErr
				}})
				stream, err := approvalClient(t, server).Connect(approvalContext(t))
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&v1.ControllerFrame{Session: "session", ExecutionId: "exec", Frame: &v1.ControllerFrame_Start{Start: &v1.Start{}}}); err != nil {
					t.Fatal(err)
				}
				ev, err := stream.Recv()
				if err != nil || ev.GetKind() != operation.kind {
					t.Fatalf("concurrent request = %v, %v", ev, err)
				}
				close(release) // Run returns while the operation awaits a reply the peer never sends.
				ev, err = stream.Recv()
				if err != nil || ev.GetKind() != v1.EventKind_EVENT_END {
					t.Fatalf("Run closure = %v, %v", ev, err)
				}
				if failed {
					if ev.GetEnd().GetState() != "FAILED" || ev.GetEnd().GetError().GetDescription() != runErr.Error() {
						t.Fatalf("failed END = %v, want actual unwind failure diagnostics", ev)
					}
					if ev, err := stream.Recv(); status.Code(err) != codes.Unavailable {
						t.Fatalf("Run failure status = %v, %v; want Unavailable", ev, err)
					}
				} else {
					if ev.GetEnd().GetState() != "COMPLETED" || ev.GetEnd().GetError() != nil {
						t.Fatalf("successful END = %v, want COMPLETED without shutdown failure", ev)
					}
					if ev, err := stream.Recv(); err != io.EOF {
						t.Fatalf("normal closure = %v, %v", ev, err)
					}
				}
				if err := <-observed; err == nil || !strings.Contains(err.Error(), "sink closed") {
					t.Fatalf("concurrent await = %v, want rejection by sink closure", err)
				}
			})
		}
	}
}

func TestServerNormalClosureSealsRetainedSink(t *testing.T) {
	captured := make(chan api.EventSink, 1)
	server := harnesswire.NewServer(gateHarness{run: func(_ context.Context, _ *api.Start, sink api.EventSink) error { captured <- sink; return nil }})
	host := &gateHost{}
	if err := harnesswire.NewClientHarness(approvalClient(t, server)).Run(approvalContext(t), &api.Start{ExecutionID: "exec"}, host); err != nil {
		t.Fatal(err)
	}
	sink := <-captured
	_, modelErr := sink.Model(t.Context(), api.ModelRequest{Model: "m"})
	_, toolErr := sink.ToolCall(t.Context(), gateCall)
	for _, err := range []error{modelErr, toolErr, sink.Output(t.Context(), "late"), sink.Report(t.Context(), api.ToolResult{ID: "ordinary"}), sink.Usage(t.Context(), api.Usage{})} {
		if err == nil || !strings.Contains(err.Error(), "sink closed") {
			t.Fatalf("late sink operation = %v", err)
		}
	}
}

func TestApprovalClientRejectsHostAuthorityForgery(t *testing.T) {
	tests := map[string]*v1.Event{
		"request":          {Kind: v1.EventKind_EVENT_APPROVAL_REQUEST, Body: &v1.Event_Approval{Approval: &v1.ApprovalRequest{ToolCallId: "call"}}},
		"decision":         {Kind: v1.EventKind_EVENT_APPROVAL_RESULT, Body: &v1.Event_ApprovalResult{ApprovalResult: &v1.ApprovalResult{ToolCallId: "call", RequestSeq: 17, Approved: true}}},
		"execution start":  {Kind: v1.EventKind_EVENT_EXECUTION_START},
		"unsolicited park": {Kind: v1.EventKind_EVENT_PARKED},
		"request receipt":  {Kind: v1.EventKind_EVENT_TOOL_RESULT, Body: &v1.Event_Result{Result: &v1.ToolResult{Id: "call", ApprovalRequestSeq: 17}}},
		"decision receipt": {Kind: v1.EventKind_EVENT_TOOL_RESULT, Body: &v1.Event_Result{Result: &v1.ToolResult{Id: "call", ApprovalDecisionSeq: 19}}},
		"denial receipt":   {Kind: v1.EventKind_EVENT_TOOL_RESULT, Body: &v1.Event_Result{Result: &v1.ToolResult{Id: "call", Code: v1.ToolResult_APPROVAL_DENIED, IsError: true}}},
	}
	for name, event := range tests {
		t.Run(name, func(t *testing.T) {
			event.ExecutionId = "exec"
			peer := connectPeer{run: func(s v1.Harness_ConnectServer) error {
				if _, err := s.Recv(); err != nil {
					return err
				}
				if err := s.Send(event); err != nil {
					return err
				}
				return s.Send(&v1.Event{ExecutionId: "exec", Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}}})
			}}
			host := &gateHost{}
			err := harnesswire.NewClientHarness(approvalClient(t, peer)).Run(approvalContext(t), &api.Start{SessionUID: "session", ExecutionID: "exec"}, host)
			if err == nil || !strings.Contains(err.Error(), "host-owned") {
				t.Fatalf("forgery = %v, want host-owned refusal", err)
			}
			if host.reports != 0 || host.calls != 0 {
				t.Fatalf("forgery reached host sink: %#v", host)
			}
		})
	}
}
