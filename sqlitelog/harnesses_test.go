package sqlitelog_test

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// testdata/v0.1.2.db was written by the v0.1.2 release, not by this build: `agentctl exec`, then
// `agentctl suspend`, then `agentctl fork -names child-a` against an embedded journal, built from the
// v0.1.2 tag, followed by a WAL checkpoint. It holds a parent and a forked child on the echo
// harness, eleven events in all, stamped schema version 1.
const v012Fixture = "testdata/v0.1.2.db"

const (
	v012Parent = "sess-43c1f32622cd7ac118bed408"
	v012Child  = "sess-78ec0aced8e04aa38d6eac9f"
)

func copyFixture(t *testing.T, src string) string {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	dst := filepath.Join(t.TempDir(), filepath.Base(src))
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return dst
}

// schemaOf lists every table and index with the SQL that created it, which is the whole shape a
// migration is responsible for.
func schemaOf(t *testing.T, path string) [][3]string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT type, name, COALESCE(sql, '') FROM sqlite_master ORDER BY type, name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][3]string
	for rows.Next() {
		var r [3]string
		if err := rows.Scan(&r[0], &r[1], &r[2]); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func hasObject(shape [][3]string, typ, name string) bool {
	for _, r := range shape {
		if r[0] == typ && r[1] == name {
			return true
		}
	}
	return false
}

// A database written by v0.1.2 opens, migrates to the current version, keeps every session and an
// intact hash chain, and can take registrations, except under a name its sessions run on.
func TestOpenMigratesV012Database(t *testing.T) {
	path := copyFixture(t, v012Fixture)
	if got := readUserVersion(t, path); got != 1 {
		t.Fatalf("fixture user_version = %d, want 1", got)
	}
	if hasObject(schemaOf(t, path), "table", "harnesses") {
		t.Fatal("fixture already has a harnesses table")
	}

	s := mustOpen(t, path)
	if got := readUserVersion(t, path); got != sqlitelog.SchemaVersion {
		t.Fatalf("user_version = %d after Open, want %d", got, sqlitelog.SchemaVersion)
	}
	for _, uid := range []string{v012Parent, v012Child} {
		info, err := s.SessionInfo(uid)
		if err != nil {
			t.Fatalf("session %s after migration: %v", uid, err)
		}
		if info.Harness != "echo" || info.LastSeq == 0 {
			t.Fatalf("session %s: harness %q last_seq %d", uid, info.Harness, info.LastSeq)
		}
		if err := s.Session(uid).Verify(); err != nil {
			t.Fatalf("session %s chain after migration: %v", uid, err)
		}
	}
	if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: "h", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Fatalf("register on a migrated database: %v", err)
	}
	// Every version 1 session ran on a static harness, so the migration reserved its name.
	if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: "echo", UID: "u", Spec: "{}", SpecDigest: "d"}); !errors.Is(err, sqlitelog.ErrHarnessNameReserved) {
		t.Fatalf("register a v1 session's harness on a migrated database: %v, want ErrHarnessNameReserved", err)
	}
}

// appendV1Event appends ev to a version 1 database the way v0.1.x appends it: the next seq, chained
// to the head's hash, at the session's fence. It writes through SQL because Open would migrate.
func appendV1Event(t *testing.T, path, session string, ev api.Event) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var (
		seq   int64
		prev  string
		fence int64
	)
	if err := db.QueryRow(`SELECT seq, hash FROM events WHERE session = ? ORDER BY seq DESC LIMIT 1`, session).Scan(&seq, &prev); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT fence FROM sessions WHERE session = ?`, session).Scan(&fence); err != nil {
		t.Fatal(err)
	}
	seq++
	hash, err := canon.HashRecord(prev, seq, ev)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := proto.Marshal(wire.EventToProto(ev))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events(session, seq, prev_hash, hash, fence, event) VALUES(?,?,?,?,?,?)`, session, seq, prev, hash, fence, blob); err != nil {
		t.Fatal(err)
	}
}

// A version 1 turn can override the session's harness, and Resume re-runs a pending turn on the
// harness its EXECUTION_START marker recorded. The migration reserves that name too, though no
// session stores it, so no registration can take over the pending turn.
func TestMigrationReservesHarnessRecordedOnExecutionStart(t *testing.T) {
	path := copyFixture(t, v012Fixture)
	zero := int64(0)
	appendV1Event(t, path, v012Parent, api.Event{
		ExecutionID:    "pending-override",
		Kind:           api.EventExecutionStart,
		ExecutionStart: &api.ExecutionStart{InputCount: &zero, Harness: "alias-b", HarnessVersion: "v1"},
	})
	// An unversioned marker with no harness name is a legacy turn on the session's harness.
	appendV1Event(t, path, v012Child, api.Event{
		ExecutionID:    "legacy",
		Kind:           api.EventExecutionStart,
		ExecutionStart: &api.ExecutionStart{InputCount: &zero},
	})
	if got := readUserVersion(t, path); got != 1 {
		t.Fatalf("user_version = %d before Open, want 1", got)
	}

	s := mustOpen(t, path)
	for _, uid := range []string{v012Parent, v012Child} {
		if err := s.Session(uid).Verify(); err != nil {
			t.Fatalf("session %s chain after migration: %v", uid, err)
		}
	}
	for _, name := range []string{"alias-b", "echo"} {
		if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: name, UID: "u", Spec: "{}", SpecDigest: "d"}); !errors.Is(err, sqlitelog.ErrHarnessNameReserved) {
			t.Fatalf("register %s on a migrated database: %v, want ErrHarnessNameReserved", name, err)
		}
	}
	// The empty legacy name is not reserved as a name of its own.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var empty int
	if err := db.QueryRow(`SELECT COUNT(*) FROM reserved_harness_names WHERE name = ''`).Scan(&empty); err != nil || empty != 0 {
		t.Fatalf("empty name reserved: count %d, %v", empty, err)
	}
}

// An event the rung cannot decode fails the migration and leaves the database at version 1, since
// the journal cannot be shown free of a harness name a registration would take over.
func TestMigrationRefusesUndecodableEvent(t *testing.T) {
	path := copyFixture(t, v012Fixture)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE events SET event = x'ff' WHERE session = ? AND seq = 1`, v012Parent); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if s, err := sqlitelog.Open(path); err == nil {
		s.Close()
		t.Fatal("Open migrated a database with an undecodable event")
	}
	if got := readUserVersion(t, path); got != 1 {
		t.Fatalf("user_version = %d after a failed migration, want 1", got)
	}
	if hasObject(schemaOf(t, path), "table", "harnesses") {
		t.Fatal("a failed migration left the harnesses table behind")
	}
}

// A migrated database and a fresh one have the same shape. The base schema is frozen and every
// change is a rung, so a fresh database runs the same rungs and cannot drift from a migrated one.
func TestMigratedAndFreshDatabasesMatch(t *testing.T) {
	migrated := copyFixture(t, v012Fixture)
	mustOpen(t, migrated).Close()
	fresh := filepath.Join(t.TempDir(), "fresh.db")
	mustOpen(t, fresh).Close()

	got, want := schemaOf(t, migrated), schemaOf(t, fresh)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("migrated schema differs from fresh:\nmigrated %v\nfresh    %v", got, want)
	}
	for _, obj := range [][2]string{{"table", "harnesses"}, {"table", "reserved_harness_names"}, {"index", "sessions_harness"}} {
		if !hasObject(want, obj[0], obj[1]) {
			t.Fatalf("schema has no %s %s", obj[0], obj[1])
		}
	}
}

// A migrated database refuses to open under the version 1 rule. v0.1.0 through v0.1.2 refuse any
// database stamped above 1, so a downgrade fails closed instead of a v1 host appending to a journal
// whose harness retirements it cannot see. This pins the stamp that rule reads; the rule itself is
// TestOpenRejectsNewerSchema.
func TestMigratedDatabaseIsNewerThanVersion1(t *testing.T) {
	path := copyFixture(t, v012Fixture)
	mustOpen(t, path).Close()
	if got := readUserVersion(t, path); got <= 1 {
		t.Fatalf("user_version = %d after migration; a v1 build would open it", got)
	}
}

// No build stamps a negative version, so Open refuses one instead of indexing the migrations with it,
// and leaves the database as it was.
func TestOpenRejectsNegativeSchemaVersion(t *testing.T) {
	for _, v := range []int{-1, -2} {
		path := copyFixture(t, v012Fixture)
		setUserVersion(t, path, v)
		s, err := sqlitelog.Open(path)
		if err == nil {
			s.Close()
			t.Fatalf("Open accepted a database stamped v%d", v)
		}
		if !errors.Is(err, sqlitelog.ErrUnsupportedSchema) {
			t.Fatalf("v%d: error = %v, want ErrUnsupportedSchema", v, err)
		}
		if got := readUserVersion(t, path); got != v {
			t.Fatalf("user_version = %d after a refused Open, want %d", got, v)
		}
		if hasObject(schemaOf(t, path), "table", "harnesses") {
			t.Fatalf("v%d: a refused Open migrated the database", v)
		}
	}
}

// A rung that fails leaves the database at its old version with nothing half applied, so the next
// Open retries the whole step.
func TestFailedMigrationLeavesVersionUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// A version 1 stamp on a sessions table with no harness column: the 1 -> 2 rung's index fails.
	if _, err := db.Exec(`CREATE TABLE sessions (session TEXT PRIMARY KEY); PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if s, err := sqlitelog.Open(path); err == nil {
		s.Close()
		t.Fatal("Open migrated a database the rung cannot apply to")
	}
	if got := readUserVersion(t, path); got != 1 {
		t.Fatalf("user_version = %d after a failed migration, want 1", got)
	}
	if hasObject(schemaOf(t, path), "table", "harnesses") {
		t.Fatal("a failed migration left the harnesses table behind")
	}
}

func register(t *testing.T, s *sqlitelog.Store, name, uid, digest string) (sqlitelog.HarnessRecord, sqlitelog.RegisterResult) {
	t.Helper()
	rec, res, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: name, UID: uid, Spec: `{"spec":"` + digest + `"}`, SpecDigest: digest})
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return rec, res
}

func TestRegisterHarnessOutcomes(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "h.db"))

	first, res := register(t, s, "h", "uid-1", "d1")
	if res != sqlitelog.HarnessCreated || first.State != sqlitelog.HarnessActive || first.UID != "uid-1" {
		t.Fatalf("first register: %v %+v", res, first)
	}
	again, res := register(t, s, "h", "uid-2", "d1")
	if res != sqlitelog.HarnessUnchanged || again.UID != "uid-1" || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("repeat register: %v %+v; want UNCHANGED keeping uid-1", res, again)
	}
	if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: "h", UID: "uid-3", Spec: "{}", SpecDigest: "d2"}); !errors.Is(err, sqlitelog.ErrHarnessSpecConflict) {
		t.Fatalf("different spec: %v, want ErrHarnessSpecConflict", err)
	}

	retired, err := s.RetireHarness("h", "superseded")
	if err != nil || retired.State != sqlitelog.HarnessRetired || retired.RetireReason != "superseded" || retired.RetiredAt.IsZero() {
		t.Fatalf("retire: %+v, %v", retired, err)
	}
	twice, err := s.RetireHarness("h", "another reason")
	if err != nil || twice.RetireReason != "superseded" || !twice.RetiredAt.Equal(retired.RetiredAt) {
		t.Fatalf("second retire rewrote history: %+v, %v", twice, err)
	}
	if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: "h", UID: "uid-3", Spec: "{}", SpecDigest: "d2"}); !errors.Is(err, sqlitelog.ErrHarnessSpecConflict) {
		t.Fatalf("different spec on a retired name: %v, want ErrHarnessSpecConflict", err)
	}

	back, res := register(t, s, "h", "uid-4", "d1")
	if res != sqlitelog.HarnessReactivated || back.State != sqlitelog.HarnessActive || back.UID != "uid-1" || back.RetireReason != "" || !back.RetiredAt.IsZero() {
		t.Fatalf("re-register retired: %v %+v; want REACTIVATED with retire fields cleared", res, back)
	}

	if _, err := s.RetireHarness("missing", ""); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatalf("retire unknown: %v, want ErrHarnessNotFound", err)
	}
	if _, err := s.Harness("missing"); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatalf("get unknown: %v, want ErrHarnessNotFound", err)
	}
}

// Registrations survive a restart, retire state included.
func TestHarnessesPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	s := mustOpen(t, path)
	register(t, s, "h", "uid-1", "d1")
	if _, err := s.RetireHarness("h", "done"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	rec, err := mustOpen(t, path).Harness("h")
	if err != nil || rec.State != sqlitelog.HarnessRetired || rec.RetireReason != "done" || rec.UID != "uid-1" {
		t.Fatalf("after reopen: %+v, %v", rec, err)
	}
}

func TestListHarnessesPagesInNameOrder(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "h.db"))
	for _, n := range []string{"c", "a", "d", "b"} {
		register(t, s, n, "uid-"+n, "d-"+n)
	}
	if _, err := s.RetireHarness("b", ""); err != nil {
		t.Fatal(err)
	}
	names := func(opts sqlitelog.HarnessListOptions) []string {
		t.Helper()
		recs, err := s.ListHarnesses(opts)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range recs {
			out = append(out, r.Name)
		}
		return out
	}
	if got := names(sqlitelog.HarnessListOptions{Limit: 10}); !reflect.DeepEqual(got, []string{"a", "c", "d"}) {
		t.Fatalf("active = %v", got)
	}
	if got := names(sqlitelog.HarnessListOptions{Limit: 2, IncludeRetired: true}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("first page = %v", got)
	}
	if got := names(sqlitelog.HarnessListOptions{After: "b", Limit: 2, IncludeRetired: true}); !reflect.DeepEqual(got, []string{"c", "d"}) {
		t.Fatalf("second page = %v", got)
	}
	if _, err := s.ListHarnesses(sqlitelog.HarnessListOptions{}); err == nil {
		t.Fatal("a zero limit was accepted")
	}
}

func TestPutSessionOnActiveHarness(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "h.db"))
	register(t, s, "reg", "uid-1", "d1")

	if err := s.PutSessionOnActiveHarness(sqlitelog.SessionMeta{UID: "s-static", Harness: "echo"}, false); err != nil {
		t.Fatalf("static harness: %v", err)
	}
	if err := s.PutSessionOnActiveHarness(sqlitelog.SessionMeta{UID: "s-reg", Harness: "reg"}, true); err != nil {
		t.Fatalf("active registered harness: %v", err)
	}
	if err := s.PutSessionOnActiveHarness(sqlitelog.SessionMeta{UID: "s-missing", Harness: "nope"}, true); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatalf("registered harness with no row: %v, want ErrHarnessNotFound", err)
	}
	if _, err := s.RetireHarness("reg", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSessionOnActiveHarness(sqlitelog.SessionMeta{UID: "s-late", Harness: "reg"}, true); !errors.Is(err, sqlitelog.ErrHarnessRetired) {
		t.Fatalf("retired harness: %v, want ErrHarnessRetired", err)
	}
	// A name the caller serves as static is refused if it has a row at all: another host
	// registered it, so it means two harnesses. "reg" is retired by now; "active" is not.
	register(t, s, "active", "uid-2", "d2")
	for _, name := range []string{"reg", "active"} {
		if err := s.PutSessionOnActiveHarness(sqlitelog.SessionMeta{UID: "s-static-" + name, Harness: name}, false); !errors.Is(err, sqlitelog.ErrHarnessNameCollision) {
			t.Fatalf("static harness with a %s row: %v, want ErrHarnessNameCollision", name, err)
		}
	}
	for _, uid := range []string{"s-missing", "s-late", "s-static-reg", "s-static-active"} {
		if _, err := s.SessionInfo(uid); !errors.Is(err, sqlitelog.ErrSessionNotFound) {
			t.Fatalf("refused session %s was written: %v", uid, err)
		}
	}
}

// A reserved name can never be registered, and reserving a registered name fails with nothing
// reserved, so a name is one or the other.
func TestReserveHarnessNames(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "h.db"))
	register(t, s, "reg", "uid-1", "d1")
	if _, err := s.RetireHarness("reg", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveHarnessNames([]string{"echo", "reg"}); !errors.Is(err, sqlitelog.ErrHarnessNameCollision) {
		t.Fatalf("reserve a retired registration's name: %v, want ErrHarnessNameCollision", err)
	}
	// The failed call reserved nothing, echo included.
	register(t, s, "echo", "uid-2", "d2")

	if err := s.ReserveHarnessNames([]string{"chat", "solo"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveHarnessNames([]string{"chat"}); err != nil {
		t.Fatalf("reserve again: %v", err)
	}
	for _, name := range []string{"chat", "solo"} {
		if _, _, err := s.RegisterHarness(sqlitelog.HarnessRecord{Name: name, UID: "u", Spec: "{}", SpecDigest: "d"}); !errors.Is(err, sqlitelog.ErrHarnessNameReserved) {
			t.Fatalf("register reserved %q: %v, want ErrHarnessNameReserved", name, err)
		}
		if _, err := s.Harness(name); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
			t.Fatalf("refused registration %q was stored: %v", name, err)
		}
	}
	if err := s.ReserveHarnessNames([]string{""}); err == nil {
		t.Fatal("reserved an empty name")
	}
}
