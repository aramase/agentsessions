package session_test

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

// serve wires a Sessions service over the given store so a test can reopen the same journal and
// prove a listing survives a restart, which the shared newClient helper cannot express.
func serve(t *testing.T, store *sqlitelog.Store, opts ...session.Option) v1.SessionsClient {
	t.Helper()
	return serveWith(t, store, local.New(echoagent.Harness{}), opts...)
}

func serveWith(t *testing.T, store *sqlitelog.Store, backend placement.Backend, opts ...session.Option) v1.SessionsClient {
	t.Helper()
	if c, ok := backend.(io.Closer); ok {
		t.Cleanup(func() { _ = c.Close() })
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(store, echoRegistry(t, backend), opts...))
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
	return v1.NewSessionsClient(conn)
}

func openStore(t *testing.T, path string, opts ...sqlitelog.Option) *sqlitelog.Store {
	t.Helper()
	store, err := sqlitelog.Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func uids(sessions []*v1.Session) []string {
	out := make([]string, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, s.GetMetadata().GetUid())
	}
	return out
}

// CreateSession used to drop the request entirely, minting a bare UID. The spec a caller sends is
// the only place project, name, harness, and model come from, so losing it made every session
// indistinguishable.
func TestCreateSessionPersistsRequestedMetadata(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	created, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{
			Metadata: &v1.ResourceMetadata{Project: "acme", Name: "nightly-triage"},
			Harness:  "echo",
			Model:    "echo-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := created.GetMetadata().GetProject(); got != "acme" {
		t.Errorf("project = %q, want acme", got)
	}
	if got := created.GetMetadata().GetName(); got != "nightly-triage" {
		t.Errorf("name = %q, want nightly-triage", got)
	}
	if got := created.GetModel(); got != "echo-1" {
		t.Errorf("model = %q, want echo-1", got)
	}
	if created.GetMetadata().GetCreateTime() == nil {
		t.Error("create_time is unset, want it stamped at creation")
	}

	got, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: created.GetMetadata().GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetMetadata().GetProject() != "acme" || got.GetMetadata().GetName() != "nightly-triage" {
		t.Errorf("get returned %+v, want the stored metadata", got.GetMetadata())
	}
	if got.GetModel() != "echo-1" {
		t.Errorf("get model = %q, want echo-1", got.GetModel())
	}
}

// A session with no events and no compute must still appear. This is the whole point of persisting
// metadata at create time rather than materializing a session on first Exec.
func TestListSessionsIncludesSessionWithNoEvents(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	created, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := uids(resp.GetSessions()); len(got) != 1 || got[0] != created.GetMetadata().GetUid() {
		t.Fatalf("list = %v, want [%s]", got, created.GetMetadata().GetUid())
	}
	if s := resp.GetSessions()[0]; s.GetLastSeq() != 0 {
		t.Errorf("last_seq = %d, want 0 for a session with no events", s.GetLastSeq())
	}
	if s := resp.GetSessions()[0]; s.GetComputeState() != v1.ComputeState_COMPUTE_NONE {
		t.Errorf("compute_state = %v, want COMPUTE_NONE", s.GetComputeState())
	}
}

// project is an exact-match filter, never a wildcard: a caller must not be able to enumerate
// another tenant's sessions by asking for a project it does not own.
func TestListSessionsFiltersByProject(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	for _, project := range []string{"acme", "acme", "globex"} {
		if _, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
			Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: project}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		project string
		want    int
	}{{"acme", 2}, {"globex", 1}, {"nobody", 0}} {
		resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: tc.project})
		if err != nil {
			t.Fatalf("list %q: %v", tc.project, err)
		}
		if got := len(resp.GetSessions()); got != tc.want {
			t.Errorf("project %q returned %d sessions, want %d", tc.project, got, tc.want)
		}
	}
}

// The pod-restart criterion: a listing is read from the journal, so it must be complete against a
// store reopened from the same file by a different service instance.
func TestListSessionsSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	ctx := context.Background()

	first := openStore(t, path)
	created, err := serve(t, first).CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme", Name: "survivor"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	resp, err := serve(t, openStore(t, path)).ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := uids(resp.GetSessions()); len(got) != 1 || got[0] != created.GetMetadata().GetUid() {
		t.Fatalf("after restart list = %v, want [%s]", got, created.GetMetadata().GetUid())
	}
	if got := resp.GetSessions()[0].GetMetadata().GetName(); got != "survivor" {
		t.Errorf("name = %q, want it to survive the restart", got)
	}
}

// Fork lineage used to be visible only in the ForkResponse and lost on the next Get. Children also
// inherit the parent's project, so a fan-out stays inside the tenant that owns the parent instead
// of landing in the service default.
func TestForkChildrenCarryLineageAndProject(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{
			Metadata: &v1.ResourceMetadata{Project: "acme", Name: "base"},
			Model:    "echo-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	parentUID := parent.GetMetadata().GetUid()
	forked, err := client.Fork(ctx, &v1.ForkRequest{Session: parentUID, Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(forked.GetChildren()) != 2 {
		t.Fatalf("children = %d, want 2", len(forked.GetChildren()))
	}
	for _, child := range forked.GetChildren() {
		got, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: child.GetMetadata().GetUid()})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetParentUid() != parentUID {
			t.Errorf("child %s parent_uid = %q, want %q", child.GetMetadata().GetUid(), got.GetParentUid(), parentUID)
		}
		if got.GetMetadata().GetProject() != "acme" {
			t.Errorf("child project = %q, want it inherited as acme", got.GetMetadata().GetProject())
		}
		if got.GetModel() != "echo-1" {
			t.Errorf("child model = %q, want it inherited as echo-1", got.GetModel())
		}
	}
	resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(resp.GetSessions()); got != 3 {
		t.Fatalf("list returned %d sessions, want the parent plus 2 children", got)
	}
}

// Placer.forkSource refuses an unplaceable fork with no side effects at all. Registering children
// before the fan-out would turn that into a listing full of phantom sessions the caller was never
// given UIDs for and, with DeleteSession unimplemented, cannot remove.
func TestRefusedForkLeavesNoPhantomSessions(t *testing.T) {
	store := openStore(t, ":memory:")
	client := serveWith(t, store, local.New(memHarness{}))
	ctx := context.Background()

	parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A memory-snapshot harness can only fork at the log head, so a historical at_seq is refused
	// before anything is provisioned.
	_, err = client.Fork(ctx, &v1.ForkRequest{
		Session: parent.GetMetadata().GetUid(), AtSeq: 5, Count: 4,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err: %v)", status.Code(err), err)
	}

	resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := uids(resp.GetSessions()); len(got) != 1 || got[0] != parent.GetMetadata().GetUid() {
		t.Fatalf("list = %v, want just the parent; a refused fork must leave no children", got)
	}
}

// Walking pages must visit every session exactly once. The loop also guards against a token that
// never terminates, which is the failure mode a paginated API most easily hides.
func TestListSessionsPagesThroughEverySession(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	want := map[string]bool{}
	for i := 0; i < 7; i++ {
		created, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
			Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		want[created.GetMetadata().GetUid()] = true
	}

	seen := map[string]bool{}
	token := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{
			Project: "acme", PageSize: 2, PageToken: token,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range resp.GetSessions() {
			uid := s.GetMetadata().GetUid()
			if seen[uid] {
				t.Fatalf("session %s returned twice", uid)
			}
			seen[uid] = true
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("saw %d sessions, want %d", len(seen), len(want))
	}
	for uid := range want {
		if !seen[uid] {
			t.Errorf("session %s was never returned", uid)
		}
	}
}

// A malformed cursor is rejected rather than silently restarting from the top, which would show up
// as a caller quietly re-reading page one forever.
func TestListSessionsRejectsBadPageToken(t *testing.T) {
	client := newClient(t)
	_, err := client.ListSessions(context.Background(), &v1.ListSessionsRequest{
		Project: "acme", PageToken: "not-a-cursor",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err: %v)", status.Code(err), err)
	}
}

// An unknown UID is a caller error, not a host fault, so it must not be reported as Internal.
func TestGetSessionUnknownUIDIsNotFound(t *testing.T) {
	client := newClient(t)
	_, err := client.GetSession(context.Background(), &v1.GetSessionRequest{Uid: "sess-does-not-exist"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err: %v)", status.Code(err), err)
	}
}

// Suspend and Resume report the state the store will also report, so a client that acts on the
// response and a client that polls the listing never disagree.
func TestSuspendResumeAgreeWithListing(t *testing.T) {
	store := openStore(t, ":memory:")
	client := serve(t, store)
	ctx := context.Background()

	created, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	uid := created.GetMetadata().GetUid()

	for _, tc := range []struct {
		name string
		call func() (*v1.Session, error)
		want v1.ComputeState
	}{
		{"suspend", func() (*v1.Session, error) {
			return client.Suspend(ctx, &v1.SuspendRequest{Session: uid})
		}, v1.ComputeState_COMPUTE_COLD},
		{"resume", func() (*v1.Session, error) {
			return client.Resume(ctx, &v1.ResumeRequest{Session: uid})
		}, v1.ComputeState_COMPUTE_LIVE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := tc.call()
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetComputeState() != tc.want {
				t.Errorf("response compute_state = %v, want %v", resp.GetComputeState(), tc.want)
			}
			listed, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
			if err != nil {
				t.Fatal(err)
			}
			if len(listed.GetSessions()) != 1 {
				t.Fatalf("list returned %d sessions, want 1", len(listed.GetSessions()))
			}
			if got := listed.GetSessions()[0].GetComputeState(); got != tc.want {
				t.Errorf("listed compute_state = %v, want %v", got, tc.want)
			}
		})
	}
}

// A session created without a project must not be stored under an empty one: ListSessions filters
// on exact match, so a project-less row would be unreachable by every real caller.
func TestCreateSessionDefaultsProject(t *testing.T) {
	store := openStore(t, ":memory:", sqlitelog.WithDefaultProject("house"))
	client := serve(t, store, session.WithDefaultProject("house"))
	ctx := context.Background()

	created, err := client.CreateSession(ctx, &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := created.GetMetadata().GetProject(); got != "house" {
		t.Fatalf("project = %q, want house", got)
	}
	resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := uids(resp.GetSessions()); len(got) != 1 || got[0] != created.GetMetadata().GetUid() {
		t.Fatalf("list with no project = %v, want the default project's sessions", got)
	}
}

// A fork inherits the workload but not the label. Copying the parent's name made a listing report
// N+1 rows all claiming to be the same session, distinguishable only by uid.
func TestForkChildrenDoNotInheritTheParentName(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{
			Metadata: &v1.ResourceMetadata{Project: "acme", Name: "planning agent"},
			Model:    "echo-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := client.Fork(ctx, &v1.ForkRequest{Session: parent.GetMetadata().GetUid(), Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range forked.GetChildren() {
		got, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: child.GetMetadata().GetUid()})
		if err != nil {
			t.Fatal(err)
		}
		if name := got.GetMetadata().GetName(); name != "" {
			t.Errorf("child name = %q, want it empty rather than a copy of the parent's", name)
		}
		// The workload still follows the parent; only the label is dropped.
		if got.GetModel() != "echo-1" {
			t.Errorf("child model = %q, want it inherited as echo-1", got.GetModel())
		}
	}
}

// Naming children is the caller's job, so the names it supplies must land on the children in the
// order they are returned.
func TestForkAppliesCallerSuppliedChildNames(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme", Name: "base"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"optimistic branch", "pessimistic branch"}
	forked, err := client.Fork(ctx, &v1.ForkRequest{
		Session:    parent.GetMetadata().GetUid(),
		Count:      2,
		ChildNames: want,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, child := range forked.GetChildren() {
		if got := child.GetMetadata().GetName(); got != want[i] {
			t.Errorf("child %d name = %q, want %q", i, got, want[i])
		}
		// Re-read so this proves the name was persisted, not just echoed in the response.
		stored, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: child.GetMetadata().GetUid()})
		if err != nil {
			t.Fatal(err)
		}
		if got := stored.GetMetadata().GetName(); got != want[i] {
			t.Errorf("child %d name after reload = %q, want %q", i, got, want[i])
		}
	}
}

// A name/count mismatch is rejected before anything is provisioned, so a caller never ends up with
// children it could not name.
func TestForkRejectsChildNameCountMismatch(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{
		Session: &v1.Session{Metadata: &v1.ResourceMetadata{Project: "acme", Name: "base"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	parentUID := parent.GetMetadata().GetUid()
	_, err = client.Fork(ctx, &v1.ForkRequest{
		Session:    parentUID,
		Count:      3,
		ChildNames: []string{"only one"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("fork with mismatched child_names = %v, want InvalidArgument", err)
	}
	// Nothing was provisioned, so the parent is still the only session in the project.
	resp, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(resp.GetSessions()); got != 1 {
		t.Fatalf("list returned %d sessions, want only the parent", got)
	}
}

// echoRegistry wraps a backend as the single-harness registry the tests drive. Harness routing has
// its own tests in package placement; these exercise the service through it.
func echoRegistry(t *testing.T, b placement.Backend, opts ...placement.Option) *placement.Registry {
	t.Helper()
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(b, echoagent.Model, opts...),
	})
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	return r
}
