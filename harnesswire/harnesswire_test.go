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
	s := &streamSink{stream: &fakeConnectServer{}, results: results}
	results <- &v1.ControllerFrame{Frame: &v1.ControllerFrame_Model{Model: &v1.ModelResult{ModelCallId: "not-the-emitted-id"}}}
	if _, err := s.Model(api.ModelRequest{Model: "m"}); err == nil || !strings.Contains(err.Error(), "correlation mismatch") {
		t.Fatalf("want a model correlation mismatch error, got %v", err)
	}
}

// A ToolResult whose id does not match the emitted ToolCall.id must fail loud.
func TestStreamSinkToolCorrelationMismatch(t *testing.T) {
	results := make(chan *v1.ControllerFrame, 1)
	s := &streamSink{stream: &fakeConnectServer{}, results: results}
	results <- &v1.ControllerFrame{Frame: &v1.ControllerFrame_Tool{Tool: &v1.ToolResult{Id: "wrong"}}}
	if _, err := s.ToolCall(api.ToolCall{ID: "t1", Tool: "charge"}); err == nil || !strings.Contains(err.Error(), "correlation mismatch") {
		t.Fatalf("want a tool correlation mismatch error, got %v", err)
	}
}
