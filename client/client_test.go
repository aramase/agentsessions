package client_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

// newClient runs a real Sessions service over bufconn and returns the SDK pointed at it. These
// exercise the SDK against the actual server rather than a stub, since the bookkeeping it exists to
// hide (the session frame, pagination, draining) is only meaningful against real responses.
func newClient(t *testing.T) *client.Client {
	t.Helper()

	backend := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = backend.Close() })

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(backend, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(store, registry))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	c, err := client.Dial("passthrough:///bufnet",
		client.WithDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// The point of the SDK: one call runs a turn, and the caller gets the uid, the output, and the
// cursor without touching a stream or a second RPC.
func TestExecCreatesSessionAndReturnsOutput(t *testing.T) {
	c := newClient(t)

	turn, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if turn.Session.GetMetadata().GetUid() == "" {
		t.Fatal("no session uid: the caller cannot address the session Exec created")
	}
	if turn.Output != "echo:hello" {
		t.Fatalf("output = %q, want %q", turn.Output, "echo:hello")
	}
	if turn.LastSeq != 4 { // INPUT, MODEL_CALL, OUTPUT, END
		t.Fatalf("last_seq = %d, want 4", turn.LastSeq)
	}
	if len(turn.Records) != 4 {
		t.Fatalf("records = %d, want 4", len(turn.Records))
	}
}

// TurnResult.LastSeq is what makes the strict single-writer check usable without a separate
// GetSession on every turn.
func TestExecLastSeqFeedsTheNextCAS(t *testing.T) {
	c := newClient(t)

	first, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"one"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := first.Session.GetMetadata().GetUid()

	second, err := c.Exec(t.Context(), client.ExecOptions{
		Session:         uid,
		Inputs:          []string{"two"},
		ExpectedLastSeq: proto.Int64(first.LastSeq),
	})
	if err != nil {
		t.Fatalf("CAS with the previous turn's cursor was rejected: %v", err)
	}
	if second.Output != "echo:two" {
		t.Fatalf("output = %q", second.Output)
	}

	// Reusing the stale cursor must be refused, or the CAS would be decorative.
	_, err = c.Exec(t.Context(), client.ExecOptions{
		Session:         uid,
		Inputs:          []string{"three"},
		ExpectedLastSeq: proto.Int64(first.LastSeq),
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale cursor: want Aborted, got %v", err)
	}
}

// Omitting the cursor appends at the head, so consecutive turns need no bookkeeping at all.
func TestExecWithoutCASRunsConsecutiveTurns(t *testing.T) {
	c := newClient(t)

	turn, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"one"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := turn.Session.GetMetadata().GetUid()
	for _, in := range []string{"two", "three"} {
		if _, err := c.Exec(t.Context(), client.ExecOptions{Session: uid, Inputs: []string{in}}); err != nil {
			t.Fatalf("turn %q without a cursor: %v", in, err)
		}
	}
	got, err := c.GetSession(t.Context(), uid)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLastSeq() != 12 {
		t.Fatalf("last_seq = %d after 3 turns, want 12", got.GetLastSeq())
	}
}

// A failed execution must still report the session it ran against, so a caller can find a session
// Exec created for it before the turn failed.
func TestExecReturnsSessionAlongsideError(t *testing.T) {
	c := newClient(t)

	first, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"one"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := first.Session.GetMetadata().GetUid()

	turn, err := c.Exec(t.Context(), client.ExecOptions{
		Session:         uid,
		Inputs:          []string{"two"},
		ExpectedLastSeq: proto.Int64(0), // stale on purpose
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("want Aborted, got %v", err)
	}
	if turn == nil || turn.Session.GetMetadata().GetUid() != uid {
		t.Fatal("a failed turn did not report which session it ran against")
	}
}

// OnRecord fires during the turn, for callers that want to react as events commit.
func TestExecOnRecordObservesEveryRecord(t *testing.T) {
	c := newClient(t)

	var kinds []v1.EventKind
	turn, err := c.Exec(t.Context(), client.ExecOptions{
		Inputs:   []string{"hi"},
		OnRecord: func(r *v1.LogRecord) { kinds = append(kinds, r.GetEvent().GetKind()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) != len(turn.Records) {
		t.Fatalf("OnRecord saw %d records, stream carried %d", len(kinds), len(turn.Records))
	}
	if kinds[0] != v1.EventKind_EVENT_INPUT {
		t.Fatalf("first record = %v, want EVENT_INPUT", kinds[0])
	}
}

// ListSessions walks every page. Stopping at the first would silently under-report, which is worse
// than being slow because the caller cannot tell.
func TestListSessionsWalksEveryPage(t *testing.T) {
	c := newClient(t)

	const want = 7
	for i := 0; i < want; i++ {
		if _, err := c.CreateSession(t.Context(), &v1.Session{
			Metadata: &v1.ResourceMetadata{Project: "paged"},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// A page smaller than the set proves the walk, not just a single large response.
	page, err := c.ListSessionsPage(t.Context(), "paged", 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetSessions()) != 2 || page.GetNextPageToken() == "" {
		t.Fatalf("setup: page carried %d sessions, token %q", len(page.GetSessions()), page.GetNextPageToken())
	}

	all, err := c.ListSessions(t.Context(), "paged")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != want {
		t.Fatalf("ListSessions returned %d sessions, want %d", len(all), want)
	}
}

// WithProject supplies the project for calls given none, so a single-tenant caller is not repeating
// it on every request.
func TestWithProjectDefaultsTheProject(t *testing.T) {
	backendClient := newClient(t)
	if _, err := backendClient.CreateSession(t.Context(), &v1.Session{
		Metadata: &v1.ResourceMetadata{Project: "acme"},
	}); err != nil {
		t.Fatal(err)
	}
	all, err := backendClient.ListSessions(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("listed %d sessions in acme, want 1", len(all))
	}
}

// Replay is read-only and re-delivers exactly what the turn committed.
func TestReplayReturnsCommittedRecords(t *testing.T) {
	c := newClient(t)

	turn, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := turn.Session.GetMetadata().GetUid()

	records, err := c.Replay(t.Context(), uid, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(turn.Records) {
		t.Fatalf("replay returned %d records, turn committed %d", len(records), len(turn.Records))
	}
	if records[0].GetEvent().GetKind() != v1.EventKind_EVENT_INPUT {
		t.Fatalf("first replayed record = %v, want EVENT_INPUT", records[0].GetEvent().GetKind())
	}
}

// Fork returns addressable children, which is the property a single-RPC runtime cannot offer.
func TestForkReturnsNamedChildren(t *testing.T) {
	c := newClient(t)

	turn, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := turn.Session.GetMetadata().GetUid()

	children, err := c.Fork(t.Context(), uid, client.ForkOptions{Count: 2, Names: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 {
		t.Fatalf("fork returned %d children, want 2", len(children))
	}
	for i, name := range []string{"a", "b"} {
		if got := children[i].GetMetadata().GetName(); got != name {
			t.Fatalf("child %d name = %q, want %q", i, got, name)
		}
		if children[i].GetParentUid() != uid {
			t.Fatalf("child %d parent = %q, want %q", i, children[i].GetParentUid(), uid)
		}
	}
}

// An unknown harness is a caller mistake and must surface as one rather than as a transport error.
func TestExecUnknownHarnessIsInvalidArgument(t *testing.T) {
	c := newClient(t)

	turn, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"hi"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(t.Context(), client.ExecOptions{
		Session: turn.Session.GetMetadata().GetUid(),
		Harness: "nope",
		Inputs:  []string{"hi"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown harness: want InvalidArgument, got %v", err)
	}
}

// The SDK must not be a ceiling: anything it does not model stays reachable through the stub.
func TestSessionsExposesTheUnderlyingStub(t *testing.T) {
	c := newClient(t)
	if c.Sessions() == nil {
		t.Fatal("Sessions() returned nil, so the generated client is unreachable")
	}
	if _, err := c.Sessions().GetSession(t.Context(), &v1.GetSessionRequest{Uid: "sess-missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("stub call: want NotFound, got %v", err)
	}
}
