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
	return descriptorToProto(d), nil
}

// Connect drives one execution: it reads the Start frame, runs the harness with a streaming sink,
// and terminates the stream with an END event.
func (s *Server) Connect(stream v1.Harness_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return errors.New("harnesswire: first ControllerFrame must be Start")
	}

	results := make(chan *v1.ControllerFrame, 1)
	go func() {
		for {
			f, err := stream.Recv()
			if err != nil {
				close(results)
				return
			}
			results <- f
		}
	}()

	sink := &streamSink{stream: stream, results: results}
	if err := s.harness.Run(stream.Context(), startFromProto(start), sink); err != nil {
		_ = stream.Send(&v1.Event{Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "FAILED", Error: &v1.Error{Description: err.Error()}}}})
		return err
	}
	return stream.Send(&v1.Event{Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}}})
}

// streamSink is the harness-side EventSink: each op becomes an Event on the wire; Model/ToolCall
// then block for the host's reply frame.
type streamSink struct {
	stream  v1.Harness_ConnectServer
	results <-chan *v1.ControllerFrame
}

func (s *streamSink) Model(req api.ModelRequest) (api.ModelResponse, error) {
	ev := &v1.Event{
		Kind: v1.EventKind_EVENT_MODEL_CALL,
		Body: &v1.Event_Model{Model: &v1.ModelCall{
			Model:    req.Model,
			Params:   req.Params,
			Id:       newID(),
			Messages: messagesToProto(req.Messages),
		}},
	}
	if err := s.stream.Send(ev); err != nil {
		return api.ModelResponse{}, err
	}
	frame, ok := <-s.results
	if !ok {
		return api.ModelResponse{}, io.EOF
	}
	mr := frame.GetModel()
	if mr == nil {
		return api.ModelResponse{}, errors.New("harnesswire: expected a ModelResult frame")
	}
	var msg api.Message
	if m := wire.MessageFromProto(mr.GetMessage()); m != nil {
		msg = *m
	}
	return api.ModelResponse{Message: msg}, nil
}

func (s *streamSink) Output(delta string) error {
	return s.stream.Send(&v1.Event{
		Kind: v1.EventKind_EVENT_OUTPUT,
		Body: &v1.Event_Message{Message: wire.MessageToProto(api.TextMessage("assistant", delta))},
	})
}

// ToolCall/Report over the wire arrive with the controller-side tool executor (tool-idempotency
// calibration); not yet exercised by the echo harness.
func (s *streamSink) ToolCall(api.ToolCall) (api.ToolResult, error) {
	return api.ToolResult{}, errors.New("harnesswire: tool mediation over the wire not implemented (M0)")
}
func (s *streamSink) Report(api.ToolResult) error {
	return errors.New("harnesswire: tool reporting over the wire not implemented (M0)")
}
func (s *streamSink) Usage(u api.Usage) error {
	return s.stream.Send(&v1.Event{Kind: v1.EventKind_EVENT_USAGE, Body: &v1.Event_Usage{Usage: &v1.Usage{
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
	stream, err := h.client.Connect(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.ControllerFrame{Frame: &v1.ControllerFrame_Start{Start: startToProto(start)}}); err != nil {
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
		switch ev.GetKind() {
		case v1.EventKind_EVENT_MODEL_CALL:
			mc := ev.GetModel()
			resp, err := sink.Model(api.ModelRequest{
				Model:    mc.GetModel(),
				Params:   mc.GetParams(),
				Messages: messagesFromProto(mc.GetMessages()),
			})
			if err != nil {
				return err
			}
			if err := stream.Send(&v1.ControllerFrame{Frame: &v1.ControllerFrame_Model{Model: &v1.ModelResult{
				Message:     wire.MessageToProto(&resp.Message),
				ModelCallId: mc.GetId(),
			}}}); err != nil {
				return err
			}
		case v1.EventKind_EVENT_OUTPUT:
			if msg := wire.MessageFromProto(ev.GetMessage()); msg != nil {
				if err := sink.Output(msg.Text()); err != nil {
					return err
				}
			}
		case v1.EventKind_EVENT_USAGE:
			if u := ev.GetUsage(); u != nil {
				_ = sink.Usage(api.Usage{Model: u.GetModel(), InputTokens: u.GetInputTokens(), OutputTokens: u.GetOutputTokens(), ReasoningTokens: u.GetReasoningTokens()})
			}
		case v1.EventKind_EVENT_END:
			return endError(ev.GetEnd())
		}
	}
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

func descriptorToProto(d api.Descriptor) *v1.HarnessDescriptor {
	return &v1.HarnessDescriptor{
		Id:           d.ID,
		Models:       d.Models,
		Capabilities: capabilitiesToProto(d.Capabilities),
	}
}

func descriptorFromProto(p *v1.HarnessDescriptor) api.Descriptor {
	return api.Descriptor{
		ID:           p.GetId(),
		Models:       p.GetModels(),
		Capabilities: capabilitiesFromProto(p.GetCapabilities()),
	}
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
