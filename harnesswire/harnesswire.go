// Package harnesswire bridges the in-process api.Harness SPI and the out-of-process Harness.Connect
// gRPC stream, in both directions:
//
//   - Server wraps an api.Harness as a v1.HarnessServer: it runs the harness with a streaming
//     EventSink that emits each nondeterministic op as an Event on the wire and blocks for the
//     host's ControllerFrame reply. The harness never invokes the model itself — the load-bearing
//     rule holds across a process boundary.
//   - ClientHarness wraps a v1.HarnessClient as an api.Harness: its Run drives the Connect stream
//     and translates each Event into a call on the host's sink (which invokes-and-records live or
//     serves-from-journal on replay), sending the result back. So the controller and its sinks are
//     reused UNCHANGED — the remote harness looks exactly like a local one.
//
// The model REQUEST (messages) rides on the wire ModelCall so the host can invoke live; the host
// records only input_hash (Option A).
package harnesswire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/wire"
)

// ---- Server: api.Harness -> v1.HarnessServer ----

// Server serves an api.Harness over the Harness gRPC service.
type Server struct {
	v1.UnimplementedHarnessServer
	harness api.Harness
}

// NewServer wraps h as a HarnessServer.
func NewServer(h api.Harness) *Server { return &Server{harness: h} }

// Describe returns the harness's static contract.
func (s *Server) Describe(ctx context.Context, _ *v1.DescribeRequest) (*v1.HarnessDescriptor, error) {
	d, err := s.harness.Describe(ctx)
	if err != nil {
		return nil, err
	}
	return DescriptorToProto(d), nil
}

// Connect drives one execution, ending with END or a transport-only PARKED acknowledgement.
func (s *Server) Connect(stream v1.Harness_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return errors.New("harnesswire: first ControllerFrame must be Start")
	}
	executionID := first.GetExecutionId()
	if executionID == "" {
		return errors.New("harnesswire: Start frame requires execution_id")
	}

	results := make(chan *v1.ControllerFrame, 1)
	done := make(chan struct{})
	go receiveFrames(stream, results, done)

	sink := &streamSink{stream: stream, results: results, executionID: executionID, session: first.GetSession(), done: done}
	runStart := startFromProto(start)
	runStart.ExecutionID = executionID
	runStart.SessionUID = first.GetSession()
	runErr := s.harness.Run(stream.Context(), runStart, sink)
	// Interrupt an in-flight await before taking the effect lock. Never wait for
	// Recv: gRPC cancels the stream context after this handler returns.
	close(done)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.closed = true
	if sink.parked != nil {
		if runErr != nil && !isParkUnwind(runErr, sink.parked.Ref) {
			return runErr
		}
		return stream.Send(&v1.Event{
			ExecutionId: executionID,
			Kind:        v1.EventKind_EVENT_PARKED,
			Body: &v1.Event_Parked{Parked: &v1.ApprovalRef{
				ExecutionId: sink.parked.Ref.ExecutionID,
				ToolCallId:  sink.parked.Ref.ToolCallID,
				RequestSeq:  sink.parked.Ref.RequestSeq,
			}},
		})
	}
	if sink.failure != nil {
		runErr = sink.failure
	}
	if runErr != nil {
		_ = stream.Send(&v1.Event{
			ExecutionId: executionID,
			Kind:        v1.EventKind_EVENT_END,
			Body: &v1.Event_End{End: &v1.HarnessEnd{
				State: "FAILED",
				Error: &v1.Error{Description: runErr.Error()},
			}},
		})
		return runErr
	}
	return stream.Send(&v1.Event{
		ExecutionId: executionID,
		Kind:        v1.EventKind_EVENT_END,
		Body:        &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}},
	})
}

// streamSink is the harness-side EventSink: each op becomes an Event on the wire; Model/ToolCall
// then block for the host's reply frame.
type streamSink struct {
	stream      v1.Harness_ConnectServer
	results     <-chan *v1.ControllerFrame
	executionID string
	session     string
	done        <-chan struct{}
	// Serialize sends and request/reply pairs. Park and protocol failures stay
	// sticky even when a harness handles (or ignores) a sink error.
	mu      sync.Mutex
	closed  bool
	parked  *api.ApprovalParkedError
	failure error
}

func receiveFrames(stream v1.Harness_ConnectServer, results chan<- *v1.ControllerFrame, done <-chan struct{}) {
	defer close(results)
	for {
		select {
		case <-done:
			return
		case <-stream.Context().Done():
			return
		default:
		}
		frame, err := stream.Recv()
		if err != nil {
			return
		}
		select {
		case results <- frame:
		case <-done:
			return
		case <-stream.Context().Done():
			return
		}
	}
}

var errSinkClosed = errors.New("harnesswire: sink closed")

// check is called with mu held; it never acquires the effect lock recursively.
func (s *streamSink) check(ctx context.Context) error {
	if s.parked != nil {
		return s.parked
	}
	if s.failure != nil {
		return s.failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errSinkClosed
	}
	select {
	case <-s.done:
		return errSinkClosed
	default:
		return nil
	}
}

func (s *streamSink) fail(err error) error {
	// Run closure rejects background operations without changing Run's outcome.
	// Only the exact private shutdown signal is synthetic; operative causes stay sticky.
	if err == errSinkClosed {
		return err
	}
	if s.failure == nil {
		s.failure = err
	}
	return s.failure
}

// awaitResult blocks for the host's reply frame, honoring cancellation. The ctx arm is what stops a
// harness hanging forever on a host that never replies: without it a dead or wedged controller
// leaves the harness parked on a channel receive with no way out.
func (s *streamSink) awaitResult(ctx context.Context) (*v1.ControllerFrame, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	select {
	case frame, ok := <-s.results:
		if err := s.check(ctx); err != nil {
			return nil, err
		}
		if !ok {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		if frame.GetExecutionId() != s.executionID {
			return nil, fmt.Errorf("harnesswire: result execution_id mismatch: got %q, want %q",
				frame.GetExecutionId(), s.executionID)
		}
		return frame, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errSinkClosed
	}
}

func (s *streamSink) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return api.ModelResponse{}, err
	}
	choice, err := wire.ToolChoiceToProto(req.ToolChoice)
	if err != nil {
		return api.ModelResponse{}, fmt.Errorf("harnesswire: model call: %w", err)
	}
	id := newID()
	ev := &v1.Event{
		ExecutionId: s.executionID,
		Kind:        v1.EventKind_EVENT_MODEL_CALL,
		Body: &v1.Event_Model{Model: &v1.ModelCall{
			Model:      req.Model,
			Params:     req.Params,
			Id:         id,
			Messages:   messagesToProto(req.Messages),
			Tools:      wire.ToolDefinitionsToProto(req.Tools),
			ToolChoice: choice,
		}},
	}
	if err := s.stream.Send(ev); err != nil {
		return api.ModelResponse{}, err
	}
	frame, err := s.awaitResult(ctx)
	if err != nil {
		return api.ModelResponse{}, err
	}
	mr := frame.GetModel()
	if mr == nil {
		return api.ModelResponse{}, errors.New("harnesswire: expected a ModelResult frame")
	}
	// Correlate the reply to the call we emitted: a mismatched model_call_id means the stream
	// delivered the wrong reply (reordering/loss), so fail loud rather than mis-serve it.
	if mr.GetModelCallId() != id {
		return api.ModelResponse{}, fmt.Errorf("harnesswire: model result correlation mismatch: got %q, want %q", mr.GetModelCallId(), id)
	}
	var msg api.Message
	if m := wire.MessageFromProto(mr.GetMessage()); m != nil {
		msg = *m
	}
	return api.ModelResponse{Message: msg}, nil
}

func (s *streamSink) Output(ctx context.Context, delta string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.stream.Send(&v1.Event{
		ExecutionId: s.executionID,
		Kind:        v1.EventKind_EVENT_OUTPUT,
		Body:        &v1.Event_Message{Message: wire.MessageToProto(api.TextMessage("assistant", delta))},
	})
}

// ToolCall mediates a CONTROLLER_MEDIATED tool over the wire: it emits the call as an
// EVENT_TOOL_CALL (carrying args + idempotency key) and blocks for the host's ToolResult frame. The
// host — not the harness — executes and records the tool (record-before-effect, §3), exactly as it
// mediates a model call, so the load-bearing rule holds across the process boundary.
func (s *streamSink) ToolCall(ctx context.Context, tc api.ToolCall) (api.ToolResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return api.ToolResult{}, err
	}
	call := tc
	if err := s.stream.Send(&v1.Event{
		ExecutionId: s.executionID,
		Kind:        v1.EventKind_EVENT_TOOL_CALL,
		Body:        &v1.Event_Tool{Tool: wire.ToolCallToProto(&call)},
	}); err != nil {
		return api.ToolResult{}, err
	}
	frame, err := s.awaitResult(ctx)
	if err != nil {
		return api.ToolResult{}, s.fail(err)
	}
	if frame.GetSession() != s.session {
		return api.ToolResult{}, s.fail(errors.New("harnesswire: tool result session mismatch"))
	}
	var approval *api.ApprovalResult
	if call.Mediation == api.MediationRequiresApproval {
		if s.session == "" || call.ID == "" {
			return api.ToolResult{}, s.fail(errors.New("harnesswire: approval requires session and call id"))
		}
		if park := frame.GetPark(); park != nil {
			if park.GetExecutionId() != s.executionID || park.GetToolCallId() != call.ID || park.GetRequestSeq() <= 0 {
				return api.ToolResult{}, s.fail(errors.New("harnesswire: park correlation mismatch"))
			}
			s.parked = &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: s.executionID, ToolCallID: call.ID, RequestSeq: park.GetRequestSeq()}}
			return api.ToolResult{}, s.parked
		}
		decision := frame.GetApproval()
		if decision == nil || decision.GetToolCallId() != call.ID || decision.GetRequestSeq() <= 0 {
			return api.ToolResult{}, s.fail(errors.New("harnesswire: expected correlated approval before ToolResult"))
		}
		approval = &api.ApprovalResult{ToolCallID: decision.GetToolCallId(), Approved: decision.GetApproved(), Reason: decision.GetReason(), RequestSeq: decision.GetRequestSeq()}
		frame, err = s.awaitResult(ctx)
		if err != nil {
			return api.ToolResult{}, s.fail(err)
		}
		if frame.GetSession() != s.session {
			return api.ToolResult{}, s.fail(errors.New("harnesswire: tool result session mismatch"))
		}
	}
	tr := frame.GetTool()
	if tr == nil {
		return api.ToolResult{}, s.fail(errors.New("harnesswire: expected a ToolResult frame"))
	}
	res := wire.ToolResultFromProto(tr)
	if res.ID != call.ID {
		return api.ToolResult{}, s.fail(fmt.Errorf("harnesswire: tool result correlation mismatch: got %q, want %q", res.ID, call.ID))
	}
	if approval != nil {
		if err := validateApprovalReceipt(call.ID, approval, *res); err != nil {
			return api.ToolResult{}, s.fail(err)
		}
		res.Approval = approval
	} else if err := rejectApprovalReport(*res); err != nil {
		return api.ToolResult{}, s.fail(err)
	}
	return *res, nil
}

// Report records the result of a tool the harness executed in-sandbox (IN_HARNESS_REPORTED): it
// emits an EVENT_TOOL_RESULT the host records, mirroring the in-process liveSink.Report.
func (s *streamSink) Report(ctx context.Context, tr api.ToolResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := rejectApprovalReport(tr); err != nil {
		return s.fail(err)
	}
	res := tr
	return s.stream.Send(&v1.Event{
		ExecutionId: s.executionID,
		Kind:        v1.EventKind_EVENT_TOOL_RESULT,
		Body:        &v1.Event_Result{Result: wire.ToolResultToProto(&res)},
	})
}
func (s *streamSink) Usage(ctx context.Context, u api.Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.stream.Send(&v1.Event{ExecutionId: s.executionID, Kind: v1.EventKind_EVENT_USAGE, Body: &v1.Event_Usage{Usage: &v1.Usage{
		Model: u.Model, InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, ReasoningTokens: u.ReasoningTokens,
	}}})
}

// ---- Client: v1.HarnessClient -> api.Harness ----

// ClientHarness makes a remote harness (reached via HarnessClient) look like a local api.Harness,
// so the controller drives it exactly as an in-process one.
type ClientHarness struct {
	client v1.HarnessClient
}

// NewClientHarness wraps client as an api.Harness.
func NewClientHarness(client v1.HarnessClient) *ClientHarness { return &ClientHarness{client: client} }

// Describe fetches the remote harness's contract.
func (h *ClientHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	d, err := h.client.Describe(ctx, &v1.DescribeRequest{})
	if err != nil {
		return api.Descriptor{}, err
	}
	return descriptorFromProto(d), nil
}

// Run drives the remote harness over Connect, translating each emitted Event into a host-mediated
// sink call and sending the result back. The sink (live or replay) is the controller's — the host
// still mediates the model over the wire.
func (h *ClientHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // tear the Connect stream down (and the server's recv loop) when Run returns
	if start == nil || start.ExecutionID == "" {
		return errors.New("harnesswire: Start requires execution_id")
	}
	stream, err := h.client.Connect(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.ControllerFrame{
		Session:     start.SessionUID,
		ExecutionId: start.ExecutionID,
		Frame:       &v1.ControllerFrame_Start{Start: startToProto(start)},
	}); err != nil {
		return err
	}
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if ev.GetExecutionId() != start.ExecutionID {
			return fmt.Errorf("harnesswire: event execution_id mismatch: got %q, want %q",
				ev.GetExecutionId(), start.ExecutionID)
		}
		switch ev.GetKind() {
		case v1.EventKind_EVENT_MODEL_CALL:
			mc := ev.GetModel()
			choice, err := wire.ToolChoiceFromProto(mc.GetToolChoice())
			if err != nil {
				return fmt.Errorf("harnesswire: model call %q: %w", mc.GetId(), err)
			}
			resp, err := sink.Model(ctx, api.ModelRequest{
				Model:      mc.GetModel(),
				Params:     mc.GetParams(),
				Messages:   messagesFromProto(mc.GetMessages()),
				Tools:      wire.ToolDefinitionsFromProto(mc.GetTools()),
				ToolChoice: choice,
			})
			if err != nil {
				return err
			}
			if err := stream.Send(&v1.ControllerFrame{
				Session:     start.SessionUID,
				ExecutionId: start.ExecutionID,
				Frame: &v1.ControllerFrame_Model{Model: &v1.ModelResult{
					Message:     wire.MessageToProto(&resp.Message),
					ModelCallId: mc.GetId(),
				}},
			}); err != nil {
				return err
			}
		case v1.EventKind_EVENT_OUTPUT:
			if msg := wire.MessageFromProto(ev.GetMessage()); msg != nil {
				if err := sink.Output(ctx, msg.Text()); err != nil {
					return err
				}
			}
		case v1.EventKind_EVENT_TOOL_CALL:
			tc := wire.ToolCallFromProto(ev.GetTool())
			if tc == nil {
				return errors.New("harnesswire: EVENT_TOOL_CALL missing its payload")
			}
			if tc.Mediation == api.MediationRequiresApproval && (start.SessionUID == "" || tc.ID == "") {
				return errors.New("harnesswire: approval requires session and call id")
			}
			res, err := sink.ToolCall(ctx, *tc)
			if err != nil {
				var parked *api.ApprovalParkedError
				if errors.As(err, &parked) && parked != nil {
					if tc.Mediation != api.MediationRequiresApproval || start.SessionUID == "" || tc.ID == "" || parked.Ref.ExecutionID != start.ExecutionID || parked.Ref.ToolCallID != tc.ID || parked.Ref.RequestSeq <= 0 {
						return errors.New("harnesswire: invalid host park correlation")
					}
					if handoffErr := exchangePark(ctx, stream, start, parked.Ref); handoffErr != nil {
						return handoffErr
					}
					return err
				}
				if errors.Is(err, api.ErrApprovalParked) {
					return errors.New("harnesswire: host park missing correlation")
				}
				return err
			}
			if tc.Mediation == api.MediationRequiresApproval {
				if err := validateApprovalReceipt(tc.ID, res.Approval, res); err != nil {
					return err
				}
				decision := *res.Approval
				if err := stream.Send(&v1.ControllerFrame{
					Session: start.SessionUID, ExecutionId: start.ExecutionID,
					Frame: &v1.ControllerFrame_Approval{Approval: &v1.ApprovalResult{
						ToolCallId: decision.ToolCallID, RequestSeq: decision.RequestSeq,
						Approved: decision.Approved, Reason: decision.Reason,
					}},
				}); err != nil {
					return err
				}
			} else if err := rejectApprovalReport(res); err != nil {
				return err
			}
			if err := stream.Send(&v1.ControllerFrame{
				Session:     start.SessionUID,
				ExecutionId: start.ExecutionID,
				Frame:       &v1.ControllerFrame_Tool{Tool: wire.ToolResultToProto(&res)},
			}); err != nil {
				return err
			}
		case v1.EventKind_EVENT_TOOL_RESULT:
			if tr := wire.ToolResultFromProto(ev.GetResult()); tr != nil {
				if err := rejectApprovalReport(*tr); err != nil {
					return err
				}
				if err := sink.Report(ctx, *tr); err != nil {
					return err
				}
			}
		case v1.EventKind_EVENT_USAGE:
			if u := ev.GetUsage(); u != nil {
				if err := sink.Usage(ctx, api.Usage{
					Model:           u.GetModel(),
					InputTokens:     u.GetInputTokens(),
					OutputTokens:    u.GetOutputTokens(),
					ReasoningTokens: u.GetReasoningTokens(),
				}); err != nil {
					return err
				}
			}
		case v1.EventKind_EVENT_APPROVAL_REQUEST, v1.EventKind_EVENT_APPROVAL_RESULT, v1.EventKind_EVENT_EXECUTION_START, v1.EventKind_EVENT_PARKED:
			return fmt.Errorf("harnesswire: harness emitted host-owned control event %s", ev.GetKind())
		case v1.EventKind_EVENT_END:
			return endError(ev.GetEnd())
		}
	}
}

// A wrapped park is a normal unwind, but a joined independent Run failure must
// not be hidden by finding the park somewhere in its error tree.
func isParkUnwind(err error, ref api.ApprovalRef) bool {
	var parked *api.ApprovalParkedError
	if !errors.As(err, &parked) || parked == nil || parked.Ref != ref {
		return false
	}
	for err != nil {
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			for _, cause := range joined.Unwrap() {
				if !isParkUnwind(cause, ref) {
					return false
				}
			}
			return true
		}
		err = errors.Unwrap(err)
	}
	return true
}

// exchangePark accepts only the transport acknowledgement followed by actual
// clean closure. Nothing received here is dispatched to the journal sink.
func exchangePark(ctx context.Context, stream v1.Harness_ConnectClient, start *api.Start, ref api.ApprovalRef) error {
	if err := stream.Send(&v1.ControllerFrame{
		Session: start.SessionUID, ExecutionId: start.ExecutionID,
		Frame: &v1.ControllerFrame_Park{Park: &v1.ApprovalRef{
			ExecutionId: ref.ExecutionID, ToolCallId: ref.ToolCallID, RequestSeq: ref.RequestSeq,
		}},
	}); err != nil {
		return err
	}
	ack, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("harnesswire: park acknowledgement: %w", err)
	}
	park := ack.GetParked()
	if ack.GetKind() != v1.EventKind_EVENT_PARKED || ack.GetExecutionId() != start.ExecutionID || park == nil || park.GetExecutionId() != ref.ExecutionID || park.GetToolCallId() != ref.ToolCallID || park.GetRequestSeq() != ref.RequestSeq {
		return errors.New("harnesswire: park acknowledgement correlation mismatch")
	}
	if ev, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("harnesswire: expected stream closure after park acknowledgement, got %s", ev.GetKind())
	}
	return ctx.Err()
}

func validateApprovalReceipt(callID string, decision *api.ApprovalResult, receipt api.ToolResult) error {
	if decision == nil || callID == "" || decision.ToolCallID != callID || decision.RequestSeq <= 0 {
		return errors.New("harnesswire: invalid approval correlation")
	}
	if receipt.ID != callID {
		return errors.New("harnesswire: tool result correlation mismatch")
	}
	if receipt.ApprovalRequestSeq != decision.RequestSeq || receipt.ApprovalDecisionSeq <= 0 {
		return errors.New("harnesswire: invalid approval receipt sequences")
	}
	if !decision.Approved {
		if receipt.Code == api.ToolResultCodeApprovalDenied && receipt.IsError {
			return nil
		}
	} else if receipt.Code == api.ToolResultCodeUnspecified && !receipt.IsError || receipt.Code == api.ToolResultCodeExecutorError && receipt.IsError {
		return nil
	}
	return errors.New("harnesswire: approval receipt status conflicts with decision")
}

func rejectApprovalReport(receipt api.ToolResult) error {
	if receipt.Approval != nil || receipt.ApprovalRequestSeq != 0 || receipt.ApprovalDecisionSeq != 0 || receipt.Code == api.ToolResultCodeApprovalDenied {
		return errors.New("harnesswire: harness report or ordinary reply contains host-owned approval receipt")
	}
	return nil
}

// endError maps a terminal HarnessEnd into a Go error. COMPLETED is the only success; a FAILED (or
// any other non-completed) end MUST surface so the controller records EVENT_ERROR instead of
// EVENT_END{COMPLETED}. Without this, a failed remote harness would be journaled as a successful
// turn — the failure would be silently lost across the process boundary.
func endError(end *v1.HarnessEnd) error {
	if end.GetState() == "COMPLETED" {
		return nil
	}
	if e := end.GetError(); e != nil && e.GetDescription() != "" {
		return fmt.Errorf("harnesswire: remote harness ended %s: %s", end.GetState(), e.GetDescription())
	}
	return fmt.Errorf("harnesswire: remote harness ended %s", end.GetState())
}

// ---- conversions ----

func messagesToProto(ms []api.Message) []*v1.Message {
	if len(ms) == 0 {
		return nil
	}
	out := make([]*v1.Message, len(ms))
	for i := range ms {
		out[i] = wire.MessageToProto(&ms[i])
	}
	return out
}

func messagesFromProto(ps []*v1.Message) []api.Message {
	if len(ps) == 0 {
		return nil
	}
	out := make([]api.Message, 0, len(ps))
	for _, p := range ps {
		if m := wire.MessageFromProto(p); m != nil {
			out = append(out, *m)
		}
	}
	return out
}

func startToProto(s *api.Start) *v1.Start {
	if s == nil {
		return &v1.Start{}
	}
	out := &v1.Start{Config: s.Config, ResumeFromSeq: s.ResumeFromSeq, Inputs: messagesToProto(s.Inputs)}
	if len(s.History) > 0 {
		out.History = make([]*v1.Event, len(s.History))
		for i := range s.History {
			out.History[i] = wire.EventToProto(s.History[i])
		}
	}
	return out
}

func startFromProto(p *v1.Start) *api.Start {
	out := &api.Start{Config: p.GetConfig(), ResumeFromSeq: p.GetResumeFromSeq(), Inputs: messagesFromProto(p.GetInputs())}
	if len(p.GetHistory()) > 0 {
		out.History = make([]api.Event, 0, len(p.GetHistory()))
		for _, e := range p.GetHistory() {
			out.History = append(out.History, wire.EventFromProto(e))
		}
	}
	return out
}

// DescriptorToProto renders a harness descriptor as the wire type Describe returns.
func DescriptorToProto(d api.Descriptor) *v1.HarnessDescriptor {
	out := &v1.HarnessDescriptor{
		Id:           d.ID,
		Version:      d.Version,
		Models:       d.Models,
		Capabilities: capabilitiesToProto(d.Capabilities),
	}
	for _, tool := range d.Tools {
		out.Tools = append(out.Tools, wire.ToolSpecToProto(tool))
	}
	return out
}

func descriptorFromProto(p *v1.HarnessDescriptor) api.Descriptor {
	out := api.Descriptor{
		ID:           p.GetId(),
		Version:      p.GetVersion(),
		Models:       p.GetModels(),
		Capabilities: capabilitiesFromProto(p.GetCapabilities()),
	}
	for _, tool := range p.GetTools() {
		out.Tools = append(out.Tools, wire.ToolSpecFromProto(tool))
	}
	return out
}

func capabilitiesToProto(c api.Capabilities) *v1.Capabilities {
	r := v1.Resumability_RESUMABILITY_STATELESS_REPLAY
	if c.Resumability == api.ResumabilityRequiresMemorySnapshot {
		r = v1.Resumability_RESUMABILITY_REQUIRES_MEMORY_SNAPSHOT
	}
	return &v1.Capabilities{Resumability: r, ForkSafe: c.ForkSafe, RequiresGpu: c.RequiresGPU, Streaming: c.Streaming, ReasoningReplay: c.ReasoningReplay}
}

func capabilitiesFromProto(p *v1.Capabilities) api.Capabilities {
	res := api.ResumabilityStatelessReplay
	if p.GetResumability() == v1.Resumability_RESUMABILITY_REQUIRES_MEMORY_SNAPSHOT {
		res = api.ResumabilityRequiresMemorySnapshot
	}
	return api.Capabilities{Resumability: res, ForkSafe: p.GetForkSafe(), RequiresGPU: p.GetRequiresGpu(), Streaming: p.GetStreaming(), ReasoningReplay: p.GetReasoningReplay()}
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
