package session_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

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
	v1.RegisterSessionsServer(srv, session.NewService(store, echoRegistry(t, backend)))
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
		echoRegistry(t, backend, placement.WithLogger(logger)),
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
	uid := mustCreate(t, client)
	// Creating the session is its own request with its own id. Drop those records so the
	// assertions below describe the Exec flow alone.
	output.Reset()
	stream, err := client.Exec(context.Background(), &v1.ExecRequest{
		Session: uid,
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
		if component == "controller" && record["session_uid"] != uid {
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
	if err := drainExec(stream); err != nil {
		t.Fatalf("exec with no session: %v", err)
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
		Session:         uid,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", "retry"))},
		ExpectedLastSeq: proto.Int64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); status.Code(err) != codes.Aborted {
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
		echoRegistry(t, backend, placement.WithLogger(logger)),
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

	client := v1.NewSessionsClient(conn)
	stream, err := client.Exec(context.Background(), &v1.ExecRequest{Session: mustCreate(t, client)})
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
		Session: mustCreate(t, c),
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unplaceable harness: want FailedPrecondition, got %v", err)
	}
}

func execOutputs(t *testing.T, c v1.SessionsClient, sess, input string, expected int64) []string {
	t.Helper()
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:         sess,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", input))},
		ExpectedLastSeq: proto.Int64(expected),
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
		ExpectedLastSeq: proto.Int64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); status.Code(err) != codes.Aborted {
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

// mustCreate registers a session and returns its uid. Exec resolves the harness from the session's
// metadata row, so a session has to exist before it can be executed.
func mustCreate(t *testing.T, c v1.SessionsClient) string {
	t.Helper()
	sess, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return sess.GetMetadata().GetUid()
}

// A session records the harness it was created with, and Exec routes on that rather than on a
// single configured backend. Naming one the host does not serve is a caller mistake, so it is
// rejected instead of quietly running on whatever happens to be wired.
func TestCreateSessionRejectsUnknownHarness(t *testing.T) {
	c := newClient(t)
	_, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		Session: &v1.Session{Harness: "nope"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown harness at create: want InvalidArgument, got %v", err)
	}
}

// ExecRequest.harness is defined as "empty = session default", so a name the host does not serve
// must fail rather than fall back to the default.
func TestExecRejectsUnknownHarnessOverride(t *testing.T) {
	c := newClient(t)
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session: mustCreate(t, c),
		Harness: "nope",
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown harness override: want InvalidArgument, got %v", err)
	}
}

// Routing resolves the harness from the session's metadata row, so a session that was never
// created has nothing to route on and is reported as missing rather than run on a default.
func TestExecUnknownSessionIsNotFound(t *testing.T) {
	c := newClient(t)
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session: "sess-does-not-exist",
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown session: want NotFound, got %v", err)
	}
}

// drainExec reads an Exec stream to completion and returns its terminal error, or nil on a clean
// EOF. Every Exec stream opens with a session frame, so an execution error arrives after it; a
// caller that reads a single frame would see the session rather than the failure. Real clients loop
// until EOF, which is what this mirrors.
func drainExec(stream v1.Sessions_ExecClient) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Exec with no session creates one, so the common case is a single call rather than create, read
// the cursor, exec. The uid arrives on the stream.
func TestExecWithoutSessionCreatesOne(t *testing.T) {
	c := newClient(t)
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	var uid string
	var outputs int
	for {
		up, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if s := up.GetSession(); s != nil {
			uid = s.GetMetadata().GetUid()
		}
		if r := up.GetRecord(); r != nil && r.GetEvent().GetKind() == v1.EventKind_EVENT_OUTPUT {
			outputs++
		}
	}
	if uid == "" {
		t.Fatal("no session frame: the caller cannot learn the uid of the session created for it")
	}
	if outputs != 1 {
		t.Fatalf("outputs = %d, want 1", outputs)
	}
	// The created session must be a real, listable one, not an implicit log with no metadata row.
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatalf("session created by Exec is not retrievable: %v", err)
	}
	if got.GetHarness() == "" {
		t.Fatal("session created by Exec has no harness recorded")
	}
}

// Every Exec stream opens with the session, including one that names an existing session, so a
// client can rely on the frame rather than branching on whether it supplied a uid.
func TestExecAlwaysSendsSessionFrameFirst(t *testing.T) {
	c := newClient(t)
	uid := mustCreate(t, c)
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session: uid,
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetSession().GetMetadata().GetUid() != uid {
		t.Fatalf("first frame = %v, want the session %s", first, uid)
	}
}

// Leaving expected_last_seq unset appends at the head, so consecutive turns need no cursor
// bookkeeping. Setting it keeps the strict check, which TestSessionsServiceEndToEnd covers.
func TestExecWithoutCASAppendsAtHead(t *testing.T) {
	c := newClient(t)
	uid := mustCreate(t, c)
	for i := 0; i < 3; i++ {
		stream, err := c.Exec(context.Background(), &v1.ExecRequest{
			Session: uid,
			Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "turn"))},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := drainExec(stream); err != nil {
			t.Fatalf("turn %d without a cursor: %v", i+1, err)
		}
	}
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLastSeq() != 12 { // 3 turns x (INPUT, MODEL_CALL, OUTPUT, END)
		t.Fatalf("last_seq = %d after 3 turns, want 12", got.GetLastSeq())
	}
}

// expected_last_seq = 0 is a real assertion ("this session has no events"), not an absent value.
// Treating 0 as unset would make that unsayable.
func TestExecExplicitZeroCASIsEnforced(t *testing.T) {
	c := newClient(t)
	uid := mustCreate(t, c)
	first, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:         uid,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", "one"))},
		ExpectedLastSeq: proto.Int64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(first); err != nil {
		t.Fatalf("first turn with expected_last_seq=0: %v", err)
	}
	// The session now has events, so the same assertion must be refused.
	second, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:         uid,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", "two"))},
		ExpectedLastSeq: proto.Int64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(second); status.Code(err) != codes.Aborted {
		t.Fatalf("expected_last_seq=0 on a non-empty session: want Aborted, got %v", err)
	}
}

// Metadata a caller attaches must survive the round trip. Accepting it and dropping it is worse
// than rejecting it, because the caller cannot tell the difference.
func TestCreateSessionPersistsMetadata(t *testing.T) {
	c := newClient(t)
	created, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		Session: &v1.Session{
			Metadata:    &v1.ResourceMetadata{Name: "triage"},
			Labels:      map[string]string{"team": "platform", "tier": "gold"},
			Annotations: map[string]string{"note": "from the issue tracker"},
			Origin:      &v1.Origin{Source: "github", Subject: "issue/42", Uri: "https://example.invalid/42"},
			Identity:    &v1.IdentityRef{Principal: "user:alice", Issuer: "https://issuer.invalid", Subject: "alice"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{Uid: created.GetMetadata().GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLabels()["team"] != "platform" || got.GetLabels()["tier"] != "gold" {
		t.Fatalf("labels = %v", got.GetLabels())
	}
	if got.GetAnnotations()["note"] != "from the issue tracker" {
		t.Fatalf("annotations = %v", got.GetAnnotations())
	}
	if got.GetOrigin().GetSource() != "github" || got.GetOrigin().GetSubject() != "issue/42" {
		t.Fatalf("origin = %v", got.GetOrigin())
	}
	// Identity is recorded as provenance, never enforced. Storing it is what makes a session say
	// who it was created for; see docs/security.md.
	if got.GetIdentity().GetPrincipal() != "user:alice" || got.GetIdentity().GetIssuer() != "https://issuer.invalid" {
		t.Fatalf("identity = %v", got.GetIdentity())
	}
}

// A session with no metadata must report none rather than empty maps, so "nothing was attached"
// stays distinguishable from "an empty map was attached".
func TestSessionWithoutMetadataReportsNone(t *testing.T) {
	c := newClient(t)
	created, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{Uid: created.GetMetadata().GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetLabels()) != 0 || len(got.GetAnnotations()) != 0 {
		t.Fatalf("labels %v annotations %v, want both empty", got.GetLabels(), got.GetAnnotations())
	}
	if got.GetOrigin() != nil || got.GetIdentity() != nil {
		t.Fatalf("origin %v identity %v, want both unset", got.GetOrigin(), got.GetIdentity())
	}
}

// Metadata must survive a listing too, not just a direct read: a caller that lists sessions to find
// one by label needs the label to be there.
func TestListSessionsCarriesMetadata(t *testing.T) {
	c := newClient(t)
	if _, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		Session: &v1.Session{Labels: map[string]string{"team": "platform"}},
	}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListSessions(context.Background(), &v1.ListSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetSessions()) != 1 {
		t.Fatalf("listed %d sessions, want 1", len(list.GetSessions()))
	}
	if list.GetSessions()[0].GetLabels()["team"] != "platform" {
		t.Fatalf("listing dropped labels: %v", list.GetSessions()[0].GetLabels())
	}
}

// A fork inherits the context it shares with its parent and takes the rest from the request: labels
// are how a caller tells branches apart, so inheriting them would make a fan-out indistinguishable.
func TestForkMetadataInheritance(t *testing.T) {
	c := newClient(t)
	parent, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		Session: &v1.Session{
			Labels:      map[string]string{"branch": "parent"},
			Annotations: map[string]string{"note": "shared"},
			Origin:      &v1.Origin{Source: "github"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	uid := parent.GetMetadata().GetUid()
	execOutputs(t, c, uid, "hi", 0)

	resp, err := c.Fork(context.Background(), &v1.ForkRequest{
		Session: uid,
		Count:   1,
		Labels:  map[string]string{"branch": "child"},
	})
	if err != nil {
		t.Fatal(err)
	}
	child := resp.GetChildren()[0]
	if child.GetLabels()["branch"] != "child" {
		t.Fatalf("child labels = %v, want the request's", child.GetLabels())
	}
	if child.GetAnnotations()["note"] != "shared" {
		t.Fatalf("child annotations = %v, want the parent's", child.GetAnnotations())
	}
	if child.GetOrigin().GetSource() != "github" {
		t.Fatalf("child origin = %v, want the parent's", child.GetOrigin())
	}
}

// exec_state reports the execution axis. A session that has never run is PENDING; running a turn
// moves it off that.
func TestExecStateReflectsWhetherTheSessionHasRun(t *testing.T) {
	c := newClient(t)
	created, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	uid := created.GetMetadata().GetUid()
	if created.GetExecState() != v1.ExecState_EXEC_PENDING {
		t.Fatalf("new session exec_state = %v, want EXEC_PENDING", created.GetExecState())
	}

	execOutputs(t, c, uid, "hi", 0)
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetExecState() != v1.ExecState_EXEC_COMPLETED {
		t.Fatalf("after a turn exec_state = %v, want EXEC_COMPLETED", got.GetExecState())
	}
}

// A deadline in the past must cancel the turn rather than be ignored. This is what the ctx threaded
// through the harness SPI is for.
func TestExecHonoursDeadline(t *testing.T) {
	c := newClient(t)
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:      mustCreate(t, c),
		Inputs:       []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
		DeadlineUnix: time.Now().Add(-time.Minute).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); err == nil {
		t.Fatal("an expired deadline was ignored")
	}
}

// configHarness records the Start it was handed, so a test can prove the per-execution values a
// caller supplied actually reach the harness instead of stopping at the service.
type configHarness struct{ got *api.Start }

func (configHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "echo", Models: []string{"echo"}}, nil
}

func (h *configHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	*h.got = *s
	_, err := sink.Model(ctx, api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", "x")}})
	return err
}

// config and resume_from_seq are opaque to the host and meaningful only to the harness, so the test
// that they are honored is that they arrive there.
func TestExecPassesConfigAndCursorToHarness(t *testing.T) {
	var got api.Start
	c := newClientWith(t, local.New(&configHarness{got: &got}))

	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session:       mustCreate(t, c),
		Inputs:        []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
		Config:        []byte(`{"temperature":0}`),
		ResumeFromSeq: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); err != nil {
		t.Fatal(err)
	}
	if string(got.Config) != `{"temperature":0}` {
		t.Fatalf("harness saw config %q, want the request's", got.Config)
	}
	if got.ResumeFromSeq != 7 {
		t.Fatalf("harness saw resume_from_seq %d, want 7", got.ResumeFromSeq)
	}
}
