package sqlitelog_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/sqlitelog"
)

func mustOpen(t *testing.T, path string, opts ...sqlitelog.Option) *sqlitelog.Store {
	t.Helper()
	s, err := sqlitelog.Open(path, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func put(t *testing.T, s *sqlitelog.Store, m sqlitelog.SessionMeta) {
	t.Helper()
	if err := s.PutSession(m); err != nil {
		t.Fatalf("put session %q: %v", m.UID, err)
	}
}

func uids(page sqlitelog.SessionPage) []string {
	out := make([]string, 0, len(page.Sessions))
	for _, s := range page.Sessions {
		out = append(out, s.UID)
	}
	return out
}

func TestPutSessionRoundTrips(t *testing.T) {
	s := mustOpen(t, ":memory:")
	created := time.Unix(1700000000, 0)
	put(t, s, sqlitelog.SessionMeta{
		UID: "sess-a", Project: "acme", Name: "first", Harness: "echo", Model: "gpt-x",
		ParentUID: "sess-parent", ForkSeq: 7, CreatedAt: created,
	})

	got, err := s.SessionInfo("sess-a")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	if got.Project != "acme" || got.Name != "first" || got.Harness != "echo" || got.Model != "gpt-x" {
		t.Fatalf("metadata not round-tripped: %+v", got)
	}
	if got.ParentUID != "sess-parent" || got.ForkSeq != 7 {
		t.Fatalf("fork lineage not round-tripped: %+v", got)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("created_at = %v, want %v", got.CreatedAt, created)
	}
	// A session exists before it has events: that is the whole point of storing metadata.
	if got.LastSeq != 0 || got.ComputeState != api.ComputeNone {
		t.Fatalf("fresh session should have no log and no incarnation: %+v", got)
	}
}

func TestPutSessionDefaultsProject(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a"})

	got, err := s.SessionInfo("sess-a")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	// A project-less row would be invisible to every listing, which all filter on an exact project.
	if got.Project != sqlitelog.DefaultProject {
		t.Fatalf("project = %q, want %q", got.Project, sqlitelog.DefaultProject)
	}
}

func TestSessionInfoUnknownSession(t *testing.T) {
	s := mustOpen(t, ":memory:")
	if _, err := s.SessionInfo("sess-missing"); !errors.Is(err, sqlitelog.ErrSessionNotFound) {
		t.Fatalf("err = %v, want ErrSessionNotFound", err)
	}
}

// A session with no events must still be listed. This is the criterion a listing derived from the
// event table cannot meet, and the reason metadata is persisted at creation.
func TestListSessionsIncludesSessionWithNoEvents(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "sess-fresh", Project: "acme"})

	page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := uids(page); len(got) != 1 || got[0] != "sess-fresh" {
		t.Fatalf("sessions = %v, want [sess-fresh]", got)
	}
	if page.NextPageToken != "" {
		t.Fatalf("single page should not offer a cursor, got %q", page.NextPageToken)
	}
}

func TestListSessionsFiltersByProject(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme"})
	put(t, s, sqlitelog.SessionMeta{UID: "sess-b", Project: "other"})

	page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := uids(page); len(got) != 1 || got[0] != "sess-a" {
		t.Fatalf("sessions = %v, want only acme's [sess-a]", got)
	}
}

func TestListSessionsOrdersNewestFirst(t *testing.T) {
	s := mustOpen(t, ":memory:")
	base := time.Unix(1700000000, 0)
	put(t, s, sqlitelog.SessionMeta{UID: "sess-old", Project: "acme", CreatedAt: base})
	put(t, s, sqlitelog.SessionMeta{UID: "sess-new", Project: "acme", CreatedAt: base.Add(time.Hour)})

	page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"sess-new", "sess-old"}
	got := uids(page)
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("sessions = %v, want %v", got, want)
	}
}

func TestListSessionsPaginates(t *testing.T) {
	s := mustOpen(t, ":memory:")
	base := time.Unix(1700000000, 0)
	for i, uid := range []string{"sess-1", "sess-2", "sess-3", "sess-4", "sess-5"} {
		put(t, s, sqlitelog.SessionMeta{
			UID: uid, Project: "acme", CreatedAt: base.Add(time.Duration(i) * time.Minute),
		})
	}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme", PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(page.Sessions) > 2 {
			t.Fatalf("page returned %d sessions, want at most 2", len(page.Sessions))
		}
		seen = append(seen, uids(page)...)
		token = page.NextPageToken
		if token == "" {
			break
		}
	}
	want := []string{"sess-5", "sess-4", "sess-3", "sess-2", "sess-1"}
	if len(seen) != len(want) {
		t.Fatalf("saw %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("saw %v, want %v", seen, want)
		}
	}
}

// Keyset paging exists for this case. A client refreshing a session list pages through it while new
// sessions are being created; an offset would shift every later page and re-deliver a row.
func TestListSessionsPagingIsStableAcrossInserts(t *testing.T) {
	s := mustOpen(t, ":memory:")
	base := time.Unix(1700000000, 0)
	for i, uid := range []string{"sess-1", "sess-2", "sess-3", "sess-4"} {
		put(t, s, sqlitelog.SessionMeta{
			UID: uid, Project: "acme", CreatedAt: base.Add(time.Duration(i) * time.Minute),
		})
	}

	first, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme", PageSize: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// A session newer than everything already returned arrives between the two pages.
	put(t, s, sqlitelog.SessionMeta{UID: "sess-new", Project: "acme", CreatedAt: base.Add(time.Hour)})

	second, err := s.ListSessions(sqlitelog.ListOptions{
		Project: "acme", PageSize: 2, PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	want := []string{"sess-2", "sess-1"}
	if got := uids(second); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("page 2 = %v, want %v (no skip, no repeat)", got, want)
	}
}

func TestListSessionsRejectsInvalidPageToken(t *testing.T) {
	s := mustOpen(t, ":memory:")
	for _, token := range []string{"not-base64!!", "bm90LWEtY3Vyc29y"} {
		_, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme", PageToken: token})
		if !errors.Is(err, sqlitelog.ErrInvalidPageToken) {
			t.Fatalf("token %q: err = %v, want ErrInvalidPageToken", token, err)
		}
	}
}

func TestListSessionsClampsPageSize(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme"})
	// An oversized ask is clamped, not refused.
	page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme", PageSize: sqlitelog.MaxPageSize * 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(page.Sessions))
	}
}

// The projection is maintained inside Append's transaction, so committing a lifecycle event and
// observing its compute state cannot come apart.
func TestComputeStateProjection(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind api.LifecycleKind
		want api.ComputeState
	}{
		{"suspend is cold", api.LifecycleSuspend, api.ComputeCold},
		{"resume is live", api.LifecycleResume, api.ComputeLive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := mustOpen(t, ":memory:")
			put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme"})
			log := s.Session("sess-a")
			if _, err := log.Append(0, 0, api.Event{
				Kind:      api.EventLifecycle,
				Lifecycle: &api.Lifecycle{Kind: tc.kind},
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
			got, err := s.SessionInfo("sess-a")
			if err != nil {
				t.Fatalf("session info: %v", err)
			}
			if got.ComputeState != tc.want {
				t.Fatalf("compute state = %q, want %q", got.ComputeState, tc.want)
			}
			if got.LastSeq != 1 {
				t.Fatalf("last_seq = %d, want 1", got.LastSeq)
			}
		})
	}
}

// controller.Fork copies the parent's prefix through child.Append, so those copied records would
// otherwise leave a child looking live when nothing is running for it. The FORK marker is always
// the child's last event, and it settles the child at COLD: forked from a snapshot, no compute of
// its own, placed on first Exec. This is what makes the demo UI offer Resume on a fresh child.
func TestForkLandsChildCold(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "child", Project: "acme"})
	log := s.Session("child")
	// The copied prefix ends in an ordinary record, which on its own reads as live.
	if _, err := log.Append(0, 0, api.Event{
		Kind: api.EventOutput, Message: api.TextMessage("assistant", "copied"),
	}); err != nil {
		t.Fatalf("append copied prefix: %v", err)
	}
	if got, err := s.SessionInfo("child"); err != nil {
		t.Fatalf("session info: %v", err)
	} else if got.ComputeState != api.ComputeLive {
		t.Fatalf("precondition: compute state = %q, want %q", got.ComputeState, api.ComputeLive)
	}

	if _, err := log.Append(1, 0, api.Event{
		Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork},
	}); err != nil {
		t.Fatalf("append fork: %v", err)
	}
	got, err := s.SessionInfo("child")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	if got.ComputeState != api.ComputeCold {
		t.Fatalf("compute state = %q, want %q", got.ComputeState, api.ComputeCold)
	}
}

// Re-registering a session must not rewind its identity: the fence belongs to the incarnation
// sequence and the projection to the log, neither of which PutSession owns. Fork depends on this,
// because controller.Fork fences the child log before the service writes the child's metadata.
func TestPutSessionPreservesFenceAndProjection(t *testing.T) {
	s := mustOpen(t, ":memory:")
	log := s.Session("sess-a")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatalf("new fence: %v", err)
	}
	if _, err := log.Append(0, fence, api.Event{
		Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	created := time.Unix(1700000000, 0)
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme", CreatedAt: created})
	// The second registration carries no CreatedAt, which is the shape Fork and CreateSession
	// actually use. It must update the metadata without restamping the session as new.
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme", Name: "renamed"})

	got, err := s.SessionInfo("sess-a")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	if got.ComputeState != api.ComputeCold {
		t.Fatalf("compute state = %q, want it preserved as %q", got.ComputeState, api.ComputeCold)
	}
	if got.Name != "renamed" {
		t.Fatalf("name = %q, want the update applied", got.Name)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("created_at = %v, want the original %v", got.CreatedAt, created)
	}
	next, err := log.NewFence()
	if err != nil {
		t.Fatalf("new fence: %v", err)
	}
	if next <= fence {
		t.Fatalf("fence went backwards: %d then %d", fence, next)
	}
}

// A session driven straight through the log, with no PutSession, must still be listable. This is
// the agentnode and direct-Exec shape, and it is the case a listing built only from registered
// metadata would silently drop.
func TestUnregisteredSessionIsStillListable(t *testing.T) {
	s := mustOpen(t, ":memory:", sqlitelog.WithDefaultProject("acme"))
	log := s.Session("sess-raw")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatalf("new fence: %v", err)
	}
	if _, err := log.Append(0, fence, api.Event{
		Kind: api.EventOutput, Message: api.TextMessage("assistant", "hi"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	page, err := s.ListSessions(sqlitelog.ListOptions{Project: "acme"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].UID != "sess-raw" {
		t.Fatalf("list = %+v, want just sess-raw", page.Sessions)
	}
	if page.Sessions[0].LastSeq != 1 {
		t.Fatalf("last seq = %d, want 1", page.Sessions[0].LastSeq)
	}
}

// A turn's own records are what make a session look live. Placer.Exec appends no RESUME of its
// own, so without this a session that has only ever been Exec'd would sit at NONE and the demo UI
// would never offer Suspend on it.
func TestExecRecordsMakeASessionLive(t *testing.T) {
	s := mustOpen(t, ":memory:")
	put(t, s, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme"})
	log := s.Session("sess-a")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatalf("new fence: %v", err)
	}
	if _, err := log.Append(0, fence, api.Event{
		Kind: api.EventInput, Message: api.TextMessage("user", "hi"),
	}); err != nil {
		t.Fatalf("append input: %v", err)
	}
	got, err := s.SessionInfo("sess-a")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	if got.ComputeState != api.ComputeLive {
		t.Fatalf("compute state = %q, want %q", got.ComputeState, api.ComputeLive)
	}
}

// A lifecycle marker that says nothing about the incarnation must leave the projection alone rather
// than reset it to NONE.
func TestUnrelatedLifecycleDoesNotResetComputeState(t *testing.T) {
	s := mustOpen(t, ":memory:")
	log := s.Session("sess-a")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatalf("new fence: %v", err)
	}
	if _, err := log.Append(0, fence, api.Event{
		Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend},
	}); err != nil {
		t.Fatalf("append suspend: %v", err)
	}
	if _, err := log.Append(1, fence, api.Event{
		Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleBaseline},
	}); err != nil {
		t.Fatalf("append baseline: %v", err)
	}
	got, err := s.SessionInfo("sess-a")
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	if got.ComputeState != api.ComputeCold {
		t.Fatalf("compute state = %q, want it left at %q", got.ComputeState, api.ComputeCold)
	}
}

func TestListSessionsSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s1 := mustOpen(t, path)
	put(t, s1, sqlitelog.SessionMeta{UID: "sess-a", Project: "acme", Harness: "echo"})
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := mustOpen(t, path) // fresh pod
	page, err := s2.ListSessions(sqlitelog.ListOptions{Project: "acme"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := uids(page); len(got) != 1 || got[0] != "sess-a" {
		t.Fatalf("sessions = %v, want [sess-a] after restart", got)
	}
	if page.Sessions[0].Harness != "echo" {
		t.Fatalf("harness = %q, want echo", page.Sessions[0].Harness)
	}
}

// TestOpenRejectsObsoleteSchema covers a database written before `sessions` carried metadata
// columns. CREATE TABLE IF NOT EXISTS cannot add a column to an existing table, so without the
// check such a database opens and then fails every metadata query with a bare "no such column".
func TestOpenRejectsObsoleteSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obsolete.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open obsolete db: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE sessions (session TEXT PRIMARY KEY, fence INTEGER NOT NULL DEFAULT 0)`,
		`INSERT INTO sessions(session, fence) VALUES('sess-ran', 1)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("obsolete schema: %v", err)
		}
	}
	db.Close()

	s, err := sqlitelog.Open(path)
	if err == nil {
		s.Close()
		t.Fatal("Open accepted a database with the pre-metadata sessions table")
	}
	if !errors.Is(err, sqlitelog.ErrObsoleteSchema) {
		t.Fatalf("error = %v, want ErrObsoleteSchema", err)
	}
	if !strings.Contains(err.Error(), "delete it") {
		t.Fatalf("error %q does not tell the operator what to do", err)
	}
}
