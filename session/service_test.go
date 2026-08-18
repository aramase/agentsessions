package session_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

func newClient(t *testing.T) v1.SessionsClient {
	return newClientWith(t, local.New(echoagent.Harness{}))
}

func newClientWith(t *testing.T, backend placement.Backend) v1.SessionsClient {
	t.Helper()
	if c, ok := backend.(io.Closer); ok {
		t.Cleanup(func() { _ = c.Close() })
	}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(store, placement.New(backend, echoagent.Model)))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return v1.NewSessionsClient(conn)
}

func TestRequestFlowLogsAreCorrelated(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend := local.New(echoagent.Harness{}, local.WithLogger(logger))
	t.Cleanup(func() { _ = backend.Close() })

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(logger)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(logger)),
	)
	v1.RegisterSessionsServer(srv, session.NewService(
		store,
		placement.New(backend, echoagent.Model, placement.WithLogger(logger)),
		session.WithLogger(logger),
	))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := v1.NewSessionsClient(conn)
	stream, err := client.Exec(context.Background(), &v1.ExecRequest{
		Session: "observed-session",
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "do-not-log-this"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}

	if strings.Contains(output.String(), "do-not-log-this") {
		t.Fatalf("logs contain input contents: %s", output.String())
	}
	required := map[string]bool{
		"grpc/request":                     false,
		"session/exec":                     false,
		"placement/resolve_execution_path": false,
		"runtime.local/create_compute":     false,
		"controller/exec":                  false,
	}
	requestIDs := map[string]struct{}{}
	infoRecords := 0
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		component, _ := record["component"].(string)
		operation, _ := record["operation"].(string)
		if record["level"] == "INFO" {
			infoRecords++
		}
		if key := component + "/" + operation; record["phase"] == "finish" {
			if _, ok := required[key]; ok {
				required[key] = true
			}
		}
		if component == "controller" && record["session_uid"] != "observed-session" {
			t.Fatalf("controller record missing session correlation: %v", record)
		}
		if requestID, _ := record["request_id"].(string); requestID != "" {
			requestIDs[requestID] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for event, found := range required {
		if !found {
			t.Errorf("missing correlated event %s", event)
		}
	}
	if len(requestIDs) != 1 {
		t.Fatalf("request flow used %d request IDs, want 1: %v", len(requestIDs), requestIDs)
	}
	if infoRecords != 6 {
		t.Fatalf("request flow emitted %d INFO records, want 6", infoRecords)
	}

	output.Reset()
	stream, err = client.Exec(context.Background(), &v1.ExecRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty session: want InvalidArgument, got %v", err)
	}
	scanner = bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["level"] == "ERROR" {
			t.Fatalf("caller-caused InvalidArgument logged at ERROR: %v", record)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	output.Reset()
	stream, err = client.Exec(context.Background(), &v1.ExecRequest{
		Session:         "observed-session",
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", "retry"))},
		ExpectedLastSeq: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("stale cursor: want Aborted, got %v", err)
	}
	scanner = bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["level"] == "ERROR" {
			t.Fatalf("expected CAS conflict logged at ERROR: %v", record)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceCreatesRequestIDWithoutInterceptors(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend := local.New(echoagent.Harness{}, local.WithLogger(logger))
	t.Cleanup(func() { _ = backend.Close() })

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(
		store,
		placement.New(backend, echoagent.Model, placement.WithLogger(logger)),
		session.WithLogger(logger),
	))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := v1.NewSessionsClient(conn).Exec(context.Background(), &v1.ExecRequest{Session: "direct-service"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}

	required := map[string]bool{
		"session/exec":                 false,
		"placement/exec":               false,
		"runtime.local/create_compute": false,
		"controller/exec":              false,
	}
	requestIDs := map[string]struct{}{}
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		component, _ := record["component"].(string)
		operation, _ := record["operation"].(string)
		key := component + "/" + operation
		if _, ok := required[key]; !ok {
			continue
		}
		required[key] = true
		requestID, _ := record["request_id"].(string)
		if requestID == "" {
			t.Fatalf("%s missing request_id: %v", key, record)
		}
		requestIDs[requestID] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for event, found := range required {
		if !found {
			t.Errorf("missing event %s", event)
		}
	}
	if len(requestIDs) != 1 {
		t.Fatalf("direct Service flow used %d request IDs, want 1: %v", len(requestIDs), requestIDs)
	}
}

// memHarness declares REQUIRES_MEMORY_SNAPSHOT — unplaceable on the filesystem-only local backend.
type memHarness struct{}

func (memHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}, nil
}
func (memHarness) Run(context.Context, *api.Start, api.EventSink) error { return nil }

// TestExecUnplaceableIsFailedPrecondition proves the placement gate surfaces at the API: a
// REQUIRES_MEMORY_SNAPSHOT harness on the filesystem-only local backend is refused with
// codes.FailedPrecondition, distinct from the CAS/fence codes.Aborted.
func TestExecUnplaceableIsFailedPrecondition(t *testing.T) {
	c := newClientWith(t, local.New(memHarness{}))
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session: "s",
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unplaceable harness: want FailedPrecondition, got %v", err)
	}
}

func execOutputs(t *testing.T, c v1.SessionsClient, sess, input string, expected int64) []string {
	t.Helper()
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:         sess,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", input))},
		ExpectedLastSeq: expected,
	})
	if err != nil {
		t.Fatal(err)
	}
	var outs []string
	for {
		up, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("exec recv: %v", err)
		}
		if r := up.GetRecord(); r != nil && r.GetEvent().GetKind() == v1.EventKind_EVENT_OUTPUT {
			outs = append(outs, r.GetEvent().GetMessage().GetParts()[0].GetText().GetText())
		}
	}
	return outs
}

// TestSessionsServiceEndToEnd drives the whole client-facing surface over real gRPC (bufconn):
// create → exec → replay → fork → suspend → resume → continue, plus single-writer rejection.
func TestSessionsServiceEndToEnd(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	s, err := c.CreateSession(ctx, &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	sess := s.GetMetadata().GetUid()

	if outs := execOutputs(t, c, sess, "hi", 0); len(outs) != 1 || outs[0] != "echo:hi" {
		t.Fatalf("turn 1 outputs=%v", outs)
	}

	// replay re-delivers the 4 committed records of turn 1.
	rs, err := c.Replay(ctx, &v1.ReplayRequest{Session: sess})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		r, err := rs.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
		if r.GetContentHash() == "" {
			t.Fatalf("record seq=%d missing content_hash", r.GetSeq())
		}
	}
	if n != 4 {
		t.Fatalf("replay delivered %d records, want 4", n)
	}

	// fork at head → a child sharing the prefix.
	fr, err := c.Fork(ctx, &v1.ForkRequest{Session: sess, AtSeq: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.GetChildren()) != 1 {
		t.Fatalf("expected 1 child, got %d", len(fr.GetChildren()))
	}
	if child := fr.GetChildren()[0].GetMetadata().GetUid(); child == "" || child == sess {
		t.Fatalf("bad child uid %q", child)
	}

	// suspend → resume.
	if sr, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatal(err)
	} else if sr.GetComputeState() != v1.ComputeState_COMPUTE_COLD {
		t.Fatalf("suspend compute=%v want COLD", sr.GetComputeState())
	}
	if rr, err := c.Resume(ctx, &v1.ResumeRequest{Session: sess}); err != nil {
		t.Fatal(err)
	} else if rr.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
		t.Fatalf("resume compute=%v want LIVE", rr.GetComputeState())
	}

	// continue with turn 2 on the same durable session.
	cur, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: sess})
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, c, sess, "bye", cur.GetLastSeq()); len(outs) != 1 || outs[0] != "echo:bye" {
		t.Fatalf("turn 2 outputs=%v", outs)
	}

	// a stale expected_last_seq is rejected (single-writer CAS surfaces as Aborted).
	stream, err := c.Exec(ctx, &v1.ExecRequest{
		Session:         sess,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", "stale"))},
		ExpectedLastSeq: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale exec: want Aborted, got %v", err)
	}
}

// count is caller-supplied and sizes both an allocation and a provisioning loop, so it must be
// bounded at the edge. Rejecting it is InvalidArgument (the request is malformed), and the rejection
// must land before anything is allocated or provisioned.
func TestForkRejectsUnboundedChildCount(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	s, err := c.CreateSession(ctx, &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	sess := s.GetMetadata().GetUid()

	_, err = c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: session.MaxForkChildren + 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("fork count=%d err=%v want InvalidArgument", session.MaxForkChildren+1, err)
	}
	// The boundary itself stays valid: the guard must not be off by one.
	if _, err := c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: session.MaxForkChildren}); err != nil {
		t.Fatalf("fork at the documented maximum must be accepted: %v", err)
	}
}
