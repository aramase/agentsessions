package conformance_test

import (
	"context"
	"net"
	"reflect"
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
func wireHarness(t *testing.T) api.Harness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(echoagent.Harness{}))
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
	again, _ := m.call(api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", "hi")}})
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
