package conformance_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
)

// wireHarness runs the echo harness in a gRPC Harness server (bufconn) and returns an api.Harness
// that drives it OUT OF PROCESS over Harness.Connect. Each Run opens a fresh Connect stream, so the
// same value serves both the live turn and the replay.
func wireHarness(t *testing.T) api.Harness { return wireHarnessFrom(t, echoagent.Harness{}) }

// wireHarnessFrom serves h over a bufconn Harness server and returns an api.Harness that drives it
// out of process, so a test can swap in a deliberately-failing harness.
func wireHarnessFrom(t *testing.T, h api.Harness) api.Harness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(h))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///harness",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
}

// failHarness fails its run without emitting anything; the Server terminates the stream with
// END{FAILED,error} and returns the status.
type failHarness struct{}

func (failHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "fail"}, nil
}
func (failHarness) Run(context.Context, *api.Start, api.EventSink) error {
	return errors.New("boom")
}

// TestWireFailedHarnessSurfacesError is the END-state correctness proof across the wire: a remote
// harness that FAILs is journaled as EVENT_ERROR, not EVENT_END{COMPLETED}. Before the fix,
// ClientHarness.Run returned nil on any END event, so the controller recorded the failed turn as a
// successful one and the failure was silently lost across the process boundary.
func TestWireFailedHarnessSurfacesError(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	log := s.Session("s")
	har := wireHarnessFrom(t, failHarness{})

	c, err := controller.New(log, (&countModel{}).call)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Exec(context.Background(), har, []api.Message{*api.TextMessage("user", "hi")}, 0)
	if err == nil {
		t.Fatal("exec of a failing remote harness returned nil; the failure was swallowed")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("surfaced error did not carry the harness failure %q: %v", "boom", err)
	}

	recs, rerr := log.Read(1)
	if rerr != nil {
		t.Fatal(rerr)
	}
	last := recs[len(recs)-1].Event
	if last.Kind == api.EventEnd {
		t.Fatalf("failed turn was journaled as END{%s}; want EVENT_ERROR", last.End.State)
	}
	if last.Kind != api.EventError {
		t.Fatalf("terminal event = %s; want ERROR", last.Kind)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after a failed turn: %v", err)
	}
}

// TestWireNondeterministicReplay is the load-bearing proof across a PROCESS BOUNDARY: with the
// harness driven out-of-process over Harness.Connect and a genuinely nondeterministic model, replay
// still reconstructs the recorded output byte-identically and invokes the model zero times (I1).
// The host mediates the model over the wire — the harness never invokes it — so the load-bearing
// rule holds remotely, exactly as in-process.
func TestWireNondeterministicReplay(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	log := s.Session("s")
	har := wireHarness(t)
	m := &countModel{}

	c, err := controller.New(log, m.call)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), har, []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatalf("wire exec: %v", err)
	}
	live, _ := c.Outputs()
	again, _ := m.call(t.Context(), api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", "hi")}})
	if len(live) != 1 || again.Message.Text() == live[0] {
		t.Fatalf("model not nondeterministic (live=%v again=%q)", live, again.Message.Text())
	}

	c2, err := controller.New(log, m.call)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := c2.Replay(context.Background(), har)
	if err != nil {
		t.Fatalf("wire replay: %v", err)
	}
	if !reflect.DeepEqual(live, replay) {
		t.Fatalf("wire replay served a fresh answer instead of the recorded one: %v vs %v", replay, live)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatalf("wire replay invoked the model %d times (I1)", c2.ModelInvocations())
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestWireControllerMediatedTool proves a CONTROLLER_MEDIATED tool is host-mediated across the
// process boundary: the out-of-process harness emits the tool call over Harness.Connect, the HOST
// executes it (its executor, not the harness) and records intent+result, and on replay the recorded
// result is served without re-executing — determinism holds for tools exactly as for model calls.
func TestWireControllerMediatedTool(t *testing.T) {
	s, _ := openFile(t)
	defer s.Close()
	log := s.Session("s")
	har := wireHarnessFrom(t, toolHarness{key: "k1"})
	tool := newIdempotentTool()

	c, err := controller.New(log, (&countModel{}).call, controller.WithToolExecutor(tool.exec))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), har, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatalf("wire exec: %v", err)
	}
	if tool.effects["k1"] != 1 {
		t.Fatalf("host executed the tool %d times, want 1", tool.effects["k1"])
	}
	recs, _ := log.Read(1)
	var calls, results int
	for _, r := range recs {
		switch r.Event.Kind {
		case api.EventToolCall:
			calls++
		case api.EventToolResult:
			results++
		}
	}
	if calls != 1 || results != 1 {
		t.Fatalf("journal has %d TOOL_CALL / %d TOOL_RESULT, want 1/1", calls, results)
	}

	c2, err := controller.New(log, (&countModel{}).call, controller.WithToolExecutor(tool.exec))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Replay(context.Background(), har); err != nil {
		t.Fatalf("wire replay: %v", err)
	}
	if tool.effects["k1"] != 1 {
		t.Fatalf("replay re-executed the tool: %d times, want 1 (I1)", tool.effects["k1"])
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
