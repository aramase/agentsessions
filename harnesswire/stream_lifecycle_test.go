package harnesswire

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
)

// Controlled Recv avoids goroutine-count heuristics: the second frame is known
// to be forwarded against a full result queue, and pump return is observable.
type pumpStream struct {
	v1.Harness_ConnectServer
	ctx      context.Context
	frames   chan *v1.ControllerFrame
	received chan struct{}
}

func (s *pumpStream) Context() context.Context { return s.ctx }
func (s *pumpStream) Recv() (*v1.ControllerFrame, error) {
	select {
	case frame, ok := <-s.frames:
		if !ok {
			return nil, io.EOF
		}
		s.received <- struct{}{}
		return frame, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func TestReceivePumpBackpressureShutdown(t *testing.T) {
	for _, byContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "handler return", true: "context canceled"}[byContext], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &pumpStream{ctx: ctx, frames: make(chan *v1.ControllerFrame, 2), received: make(chan struct{}, 2)}
			stream.frames <- &v1.ControllerFrame{ExecutionId: "one"}
			stream.frames <- &v1.ControllerFrame{ExecutionId: "two"}
			results := make(chan *v1.ControllerFrame, 1)
			done := make(chan struct{})
			stopped := make(chan struct{})
			go func() { receiveFrames(stream, results, done); close(stopped) }()
			for range 2 {
				select {
				case <-stream.received:
				case <-time.After(time.Second):
					t.Fatal("pump did not receive the backpressured frame")
				}
			}
			if byContext {
				cancel()
			} else {
				close(done)
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("receive pump leaked on a full forwarding queue")
			}
		})
	}
}

func TestReceivePumpBlockedRecvCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &pumpStream{ctx: ctx, frames: make(chan *v1.ControllerFrame), received: make(chan struct{})}
	stopped := make(chan struct{})
	go func() { receiveFrames(stream, make(chan *v1.ControllerFrame, 1), make(chan struct{})); close(stopped) }()
	cancel() // gRPC owns this cancellation when Connect returns
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("receive pump did not exit canceled Recv")
	}
}

// A ready frame must not win over an already canceled invocation (select alone
// picks nondeterministically), or a decided reply may escape after cancellation.
func TestAwaitResultCanceledWithReadyFrame(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 100 {
		results := make(chan *v1.ControllerFrame, 1)
		results <- receiptFrameForLifecycle()
		s := &streamSink{results: results, executionID: "exec"}
		if _, err := s.awaitResult(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("ready canceled result = %v", err)
		}
	}
}
func receiptFrameForLifecycle() *v1.ControllerFrame {
	return &v1.ControllerFrame{ExecutionId: "exec", Frame: &v1.ControllerFrame_Tool{Tool: &v1.ToolResult{Id: "call"}}}
}

func TestStreamSinkClosedAwaitRejectsReadyFrame(t *testing.T) {
	done := make(chan struct{})
	close(done)
	results := make(chan *v1.ControllerFrame, 1)
	results <- receiptFrameForLifecycle()
	s := &streamSink{results: results, executionID: "exec", done: done}
	for range 100 {
		if _, err := s.awaitResult(t.Context()); !errors.Is(err, errSinkClosed) {
			t.Fatalf("ready result after Run closure = %v", err)
		}
	}
}

func TestAwaitResultClosedDoesNotHideCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	close(done)
	s := &streamSink{done: done}
	if _, err := s.awaitResult(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled and closed await = %v, want cancellation cause", err)
	}
}

func TestStreamSinkOperativeFailuresStaySticky(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.EOF, errors.New("invalid control frame"), errors.Join(errSinkClosed, context.Canceled)} {
		s := &streamSink{}
		if err := s.fail(cause); !errors.Is(err, cause) || !errors.Is(s.failure, cause) {
			t.Fatalf("operative failure = %v, sticky cause = %v, want %v", err, s.failure, cause)
		}
		if err := s.check(t.Context()); !errors.Is(err, cause) {
			t.Fatalf("later effect refusal = %v, want original %v", err, cause)
		}
	}
}

type parkSendFailure struct {
	v1.Harness_ConnectClient
	cause error
}

func (s parkSendFailure) Send(*v1.ControllerFrame) error { return s.cause }
func (parkSendFailure) Recv() (*v1.Event, error)         { panic("Recv after failed park Send") }

func TestParkExchangePreservesSendFailure(t *testing.T) {
	cause := errors.New("transport write failed")
	err := exchangePark(t.Context(), parkSendFailure{cause: cause}, &api.Start{SessionUID: "session", ExecutionID: "exec"}, api.ApprovalRef{ExecutionID: "exec", ToolCallID: "call", RequestSeq: 17})
	if !errors.Is(err, cause) || errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("failed park Send = %v, want original transport cause", err)
	}
}

func TestStreamSinkInvalidApprovalStaysSealed(t *testing.T) {
	results := make(chan *v1.ControllerFrame, 1)
	results <- &v1.ControllerFrame{Session: "foreign", ExecutionId: "exec", Frame: &v1.ControllerFrame_Park{Park: &v1.ApprovalRef{ExecutionId: "exec", ToolCallId: "call", RequestSeq: 17}}}
	server := &fakeConnectServer{}
	sink := &streamSink{stream: server, results: results, executionID: "exec", session: "session"}
	_, err := sink.ToolCall(t.Context(), api.ToolCall{ID: "call", Mediation: api.MediationRequiresApproval})
	if err == nil {
		t.Fatal("foreign-session park accepted")
	}
	if lateErr := sink.Output(t.Context(), "late"); lateErr != err {
		t.Fatalf("sticky refusal = %v, want %v", lateErr, err)
	}
	if len(server.sent) != 1 || server.sent[0].GetKind() != v1.EventKind_EVENT_TOOL_CALL {
		t.Fatalf("late effect escaped: %v", server.sent)
	}
}
