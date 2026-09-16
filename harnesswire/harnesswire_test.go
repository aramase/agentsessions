package harnesswire

import (
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
)

// fakeConnectServer is a minimal Harness_ConnectServer that records Sends; Model/ToolCall only send
// and read the results channel, so the other stream methods are never called.
type fakeConnectServer struct {
	v1.Harness_ConnectServer
	sent []*v1.Event
}

func (f *fakeConnectServer) Send(e *v1.Event) error {
	f.sent = append(f.sent, e)
	return nil
}

// A ModelResult whose model_call_id does not match the emitted ModelCall.id must fail loud, not be
// mis-served — the correlation check defends against a reordered/mismatched reply on the stream.
func TestStreamSinkModelCorrelationMismatch(t *testing.T) {
	results := make(chan *v1.ControllerFrame, 1)
	server := &fakeConnectServer{}
	s := &streamSink{stream: server, results: results, executionID: "exec-1"}
	results <- &v1.ControllerFrame{
		ExecutionId: "exec-1",
		Frame:       &v1.ControllerFrame_Model{Model: &v1.ModelResult{ModelCallId: "not-the-emitted-id"}},
	}
	if _, err := s.Model(t.Context(), api.ModelRequest{Model: "m"}); err == nil || !strings.Contains(err.Error(), "correlation mismatch") {
		t.Fatalf("want a model correlation mismatch error, got %v", err)
	}
	if got := server.sent[0].GetExecutionId(); got != "exec-1" {
		t.Fatalf("model event execution ID = %q, want exec-1", got)
	}
}

// A ToolResult whose id does not match the emitted ToolCall.id must fail loud.
func TestStreamSinkToolCorrelationMismatch(t *testing.T) {
	results := make(chan *v1.ControllerFrame, 1)
	server := &fakeConnectServer{}
	s := &streamSink{stream: server, results: results, executionID: "exec-1"}
	results <- &v1.ControllerFrame{
		ExecutionId: "exec-1",
		Frame:       &v1.ControllerFrame_Tool{Tool: &v1.ToolResult{Id: "wrong"}},
	}
	if _, err := s.ToolCall(t.Context(), api.ToolCall{ID: "t1", Tool: "charge"}); err == nil || !strings.Contains(err.Error(), "correlation mismatch") {
		t.Fatalf("want a tool correlation mismatch error, got %v", err)
	}
	if got := server.sent[0].GetExecutionId(); got != "exec-1" {
		t.Fatalf("tool event execution ID = %q, want exec-1", got)
	}
}

func TestStreamSinkExecutionIDMismatch(t *testing.T) {
	tests := []struct {
		name      string
		resultID  string
		operation func(*streamSink) error
	}{
		{
			name:     "model wrong ID",
			resultID: "other-execution",
			operation: func(s *streamSink) error {
				_, err := s.Model(t.Context(), api.ModelRequest{Model: "m"})
				return err
			},
		},
		{
			name:     "model empty ID",
			resultID: "",
			operation: func(s *streamSink) error {
				_, err := s.Model(t.Context(), api.ModelRequest{Model: "m"})
				return err
			},
		},
		{
			name:     "tool wrong ID",
			resultID: "other-execution",
			operation: func(s *streamSink) error {
				_, err := s.ToolCall(t.Context(), api.ToolCall{ID: "t1", Tool: "charge"})
				return err
			},
		},
		{
			name:     "tool empty ID",
			resultID: "",
			operation: func(s *streamSink) error {
				_, err := s.ToolCall(t.Context(), api.ToolCall{ID: "t1", Tool: "charge"})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan *v1.ControllerFrame, 1)
			s := &streamSink{
				stream:      &fakeConnectServer{},
				results:     results,
				executionID: "exec-1",
			}
			results <- &v1.ControllerFrame{
				ExecutionId: test.resultID,
				Frame:       &v1.ControllerFrame_Model{Model: &v1.ModelResult{}},
			}

			err := test.operation(s)
			if err == nil || !strings.Contains(err.Error(), "execution_id mismatch") {
				t.Fatalf("want execution ID mismatch, got %v", err)
			}
			if strings.Contains(err.Error(), "correlation mismatch") {
				t.Fatalf("call correlation ran before execution ID validation: %v", err)
			}
		})
	}
}
