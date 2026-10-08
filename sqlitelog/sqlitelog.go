// Package sqlitelog is the persistent, single-writer event log backing agentsessions on durable
// storage. It implements the same append-only, hash-chained, CAS + fencing semantics as the
// in-memory eventlog.Log, but survives process death — the property that makes "suspend on one
// pod, resume on another" real. content_hash is computed once at append via package canon
// (RFC 8785 JCS over proto3-JSON) and stored; reads return it verbatim and never re-canonicalize.
// A single database holds many sessions (a parent plus its forked children). It uses the pure-Go
// modernc.org/sqlite driver (no cgo), so it runs in a plain container image. See determinism
// contract §1, §5, §7.
package sqlitelog

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	sqlite "modernc.org/sqlite"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/wire"
)

// ErrTampered indicates the stored hash-chain failed verification: a prev_hash link or a
// content_hash does not match what canon re-derives. It is deliberately distinct from an internal
// canonicalize/decode failure (a bug or storage corruption), which is returned wrapped and does
// NOT satisfy errors.Is(err, ErrTampered) — different severities, per the integrity contract.
var ErrTampered = errors.New("sqlitelog: hash-chain verification failed (tamper-evident)")

// sqliteConstraint is SQLITE_CONSTRAINT — the primary result code (low byte) for any constraint
// violation. Extended codes such as SQLITE_CONSTRAINT_PRIMARYKEY (1555) share this low byte.
const sqliteConstraint = 19

// isConstraint reports whether err is a sqlite constraint violation (e.g. a PRIMARY KEY clash).
func isConstraint(err error) bool {
	var serr *sqlite.Error
	return errors.As(err, &serr) && serr.Code()&0xff == sqliteConstraint
}

// schema is the version 1 shape, exactly as v0.1.x created it. It is frozen: a later shape is a
// rung in migrations, never an edit here, so every database reaches the current shape by the same
// steps whether it was created by v0.1.0 or a moment ago.
const schema = `
CREATE TABLE IF NOT EXISTS sessions (
  session       TEXT PRIMARY KEY,
  fence         INTEGER NOT NULL DEFAULT 0,
  project       TEXT    NOT NULL DEFAULT '',
  name          TEXT    NOT NULL DEFAULT '',
  harness       TEXT    NOT NULL DEFAULT '',
  model         TEXT    NOT NULL DEFAULT '',
  parent_uid    TEXT    NOT NULL DEFAULT '',
  fork_seq      INTEGER NOT NULL DEFAULT 0,
  compute_state TEXT    NOT NULL DEFAULT '',
  created_at    INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT 0,
  -- Caller-supplied metadata, stored as proto3-JSON. These are opaque to the store: it round-trips
  -- them so a session reports what its creator attached, and nothing here is interpreted or
  -- enforced. identity in particular is provenance, not authorization -- see docs/security.md.
  labels        TEXT    NOT NULL DEFAULT '',
  annotations   TEXT    NOT NULL DEFAULT '',
  origin        TEXT    NOT NULL DEFAULT '',
  identity      TEXT    NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS events (
  session   TEXT    NOT NULL,
  seq       INTEGER NOT NULL,
  prev_hash TEXT    NOT NULL,
  hash      TEXT    NOT NULL,
  fence     INTEGER NOT NULL,
  event     BLOB    NOT NULL,
  PRIMARY KEY (session, seq)
);
CREATE INDEX IF NOT EXISTS sessions_project_created
  ON sessions(project, created_at DESC, session);`

// SchemaVersion is the schema shape this build writes, stamped into PRAGMA user_version so a
// build can identify a database without inspecting its columns. Version 1 is what v0.1.0 through
// v0.1.2 shipped. Version 2 adds the harness registry.
const SchemaVersion = 2

// migrations[i] moves a database from version i+1 to i+2, inside the migration transaction. The
// array length is tied to SchemaVersion, so bumping one without the other does not compile.
var migrations = [SchemaVersion - 1]func(*sql.Tx) error{
	migrateV1ToV2,
}

// v1ToV2 is the SQL half of the 1 -> 2 rung: harnesses registered through the HarnessRegistry API.
// Static harnesses (built in, or configured when the host starts) are never stored here. A row is
// immutable except for state and the retire fields. reserved_harness_names holds every name a host
// sharing the database serves, or may serve, as a static harness; a name in it can never be
// registered. The sessions(harness) index serves the per-harness session lookups a registry needs,
// and is what a later delete checks before removing a row.
const v1ToV2 = `CREATE TABLE IF NOT EXISTS harnesses (
  name          TEXT    PRIMARY KEY,
  uid           TEXT    NOT NULL,
  spec          TEXT    NOT NULL, -- canonical proto3-JSON of HarnessSpec
  spec_digest   TEXT    NOT NULL,
  state         TEXT    NOT NULL, -- 'active' | 'retired'
  retire_reason TEXT    NOT NULL DEFAULT '',
  retired_at    INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reserved_harness_names (
  name          TEXT    PRIMARY KEY,
  reserved_at   INTEGER NOT NULL
);
INSERT OR IGNORE INTO reserved_harness_names(name, reserved_at)
  SELECT DISTINCT harness, CAST(strftime('%s', 'now') AS INTEGER) * 1000000000
  FROM sessions WHERE harness != '';
CREATE INDEX IF NOT EXISTS sessions_harness ON sessions(harness);`

// migrateV1ToV2 creates the registry tables and reserves every harness name a version 1 journal
// uses. Version 1 had no registry, so each of those names was static on some host. A session's
// stored harness is one such name; the harness recorded on an EXECUTION_START marker is another,
// because a turn may override the session's harness and Resume re-runs a pending turn on the name
// it recorded. Both are reserved before the transaction that creates the table commits, so no
// registration can take over a harness an old session or turn ran on, even one made by a host that
// does not serve it. A marker that does not decode fails the rung: the journal cannot be shown safe
// to register against, and the database stays at version 1.
func migrateV1ToV2(tx *sql.Tx) error {
	if _, err := tx.Exec(v1ToV2); err != nil {
		return err
	}
	// Collect first and insert after: the read cursor is closed before the transaction writes.
	rows, err := tx.Query(`SELECT session, seq, event FROM events ORDER BY session, seq`)
	if err != nil {
		return err
	}
	names := map[string]struct{}{}
	for rows.Next() {
		var (
			session string
			seq     int64
			blob    []byte
		)
		if err := rows.Scan(&session, &seq, &blob); err != nil {
			rows.Close()
			return err
		}
		ev, err := decode(blob)
		if err != nil {
			rows.Close()
			return fmt.Errorf("decode session %s seq %d: %w", session, seq, err)
		}
		if ev.Kind == api.EventExecutionStart && ev.ExecutionStart != nil && ev.ExecutionStart.Harness != "" {
			names[ev.ExecutionStart.Harness] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	for name := range names {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO reserved_harness_names(name, reserved_at) VALUES(?, ?)`, name, now); err != nil {
			return err
		}
	}
	return nil
}

// ErrUnsupportedSchema reports a database written by a build with a newer schema.
var ErrUnsupportedSchema = errors.New("sqlitelog: unsupported database schema")

// migrate brings the database to SchemaVersion, or refuses it.
//
// A database stamped newer than this build is refused before anything is written to it. It may
// have tables or invariants this build does not know about, and appending to a hash-chained log
// under those conditions risks corrupting the chain. The same rule is what makes a downgrade fail
// closed: v0.1.x refuses a version 2 database, which is the point, because a downgraded host would
// otherwise silently forget which harnesses were retired.
//
// Zero means unstamped: a fresh database, or one written before the stamp existed. Both get the
// version 1 schema and then every rung. The schema, the rungs and the new stamp commit in one
// transaction, so a crash leaves the database at its old version rather than half migrated, and an
// opener racing another one re-reads the stamp under the write lock and finds nothing left to do.
func migrate(db *sql.DB) error {
	v, err := userVersion(db)
	if err != nil {
		return err
	}
	if err := checkVersion(v); err != nil || v == SchemaVersion {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sqlitelog: migrate: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if v, err = userVersion(tx); err != nil {
		return err
	}
	if err := checkVersion(v); err != nil || v == SchemaVersion {
		return err
	}
	if v == 0 {
		if _, err := tx.Exec(schema); err != nil {
			return fmt.Errorf("sqlitelog: schema: %w", err)
		}
		v = 1
	}
	for ; v < SchemaVersion; v++ {
		if err := migrations[v-1](tx); err != nil {
			return fmt.Errorf("sqlitelog: migrate v%d to v%d: %w", v, v+1, err)
		}
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion)); err != nil {
		return fmt.Errorf("sqlitelog: stamp schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitelog: migrate: %w", err)
	}
	return nil
}

func checkVersion(v int) error {
	if v < 0 {
		// No build stamps a negative version, so the database was not written by one.
		return fmt.Errorf("%w: database is stamped v%d", ErrUnsupportedSchema, v)
	}
	if v > SchemaVersion {
		return fmt.Errorf("%w: database is v%d, this build understands v%d", ErrUnsupportedSchema, v, SchemaVersion)
	}
	return nil
}

type queryRower interface {
	QueryRow(query string, args ...any) *sql.Row
}

func userVersion(db queryRower) (int, error) {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlitelog: read schema version: %w", err)
	}
	return v, nil
}

// DefaultProject is the tenant a session lands in when none is supplied. A session is never stored
// with an empty project: a listing filters on an exact project, so a project-less row would be
// invisible to every caller that asks for a real one.
const DefaultProject = "default"

type options struct{ defaultProject string }

// Option configures a Store at Open.
type Option func(*options)

// WithDefaultProject sets the tenant used for sessions created without one.
func WithDefaultProject(project string) Option {
	return func(o *options) {
		if project != "" {
			o.defaultProject = project
		}
	}
}

// Store is a durable, multi-session event log over a single sqlite database.
type Store struct {
	db             *sql.DB
	defaultProject string
}

// Open opens (creating if needed) the sqlite database at path and migrates it to SchemaVersion. Use
// ":memory:" for an ephemeral store. For a file-backed store it uses BEGIN IMMEDIATE (via the
// _txlock DSN) so cross-process writers take the write lock upfront and serialize cleanly instead
// of deadlocking on a lock upgrade (SQLITE_BUSY), and synchronous=FULL so every committed
// transaction is durable across power loss, not just process death.
//
// Durability boundary: a *committed* append survives process death and power loss. A crash
// mid-append (before commit) drops only that uncommitted event, which is re-driven on resume
// (determinism contract I3/I4). TestPersistenceAcrossReopen proves the clean-restart case.
func Open(path string, opts ...Option) (*Store, error) {
	cfg := options{defaultProject: DefaultProject}
	for _, opt := range opts {
		opt(&cfg)
	}
	dsn := path
	if path != ":memory:" && !strings.Contains(path, "?") && !strings.HasPrefix(path, "file:") {
		dsn = "file:" + path + "?_txlock=immediate"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: guarantees a ":memory:" DB is a single shared instance (a pooled second
	// connection would otherwise get its own empty DB), and serializes the single writer.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(busyTimeoutPragma); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlitelog: %q: %w", busyTimeoutPragma, err)
	}
	if err := setWALMode(db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec("PRAGMA synchronous=FULL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlitelog: %q: %w", "PRAGMA synchronous=FULL", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, defaultProject: cfg.defaultProject}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

const (
	// busyTimeoutPragma makes ordinary statements wait for a contended write lock instead of
	// failing immediately.
	busyTimeoutPragma = "PRAGMA busy_timeout=5000"
	// walSwitchBudget bounds setWALMode's own retry loop, matching busyTimeoutPragma.
	walSwitchBudget  = 5 * time.Second
	walSwitchBackoff = 2 * time.Millisecond
)

// setWALMode puts the database in WAL journaling mode.
//
// busy_timeout does not cover this statement. Sqlite does not run the busy handler for the
// journal-mode switch, which needs a database-wide exclusive lock and returns SQLITE_BUSY the
// moment another connection holds the write lock. Measured against a held write transaction: the
// switch fails after ~90µs, while a plain INSERT on the same connection waits the full 5s timeout.
// Concurrent openers of one database therefore need explicit handling here.
//
// Reading the current mode takes no lock at all, so an already-WAL database — every reopen after
// the first, which is the case that matters for a restarting pod — skips the exclusive lock
// entirely. Only openers racing to initialize a fresh database can still collide, bounded by the
// winner's schema creation, so retry that within the same budget ordinary statements get.
func setWALMode(db *sql.DB) error {
	deadline := time.Now().Add(walSwitchBudget)
	for delay := walSwitchBackoff; ; delay *= 2 {
		var mode string
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
			return fmt.Errorf("sqlitelog: read journal mode: %w", err)
		}
		if strings.EqualFold(mode, "wal") {
			return nil
		}
		// An in-memory database cannot be WAL and reports "memory"; the switch is a no-op there
		// rather than an error, so this returns on the first attempt.
		_, err := db.Exec("PRAGMA journal_mode=WAL")
		if err == nil {
			return nil
		}
		if remaining := time.Until(deadline); remaining <= 0 {
			return fmt.Errorf("sqlitelog: %q: %w", "PRAGMA journal_mode=WAL", err)
		} else if delay > remaining {
			delay = remaining
		}
		time.Sleep(delay)
	}
}

// Session returns a handle to one session's append-only chain.
func (s *Store) Session(uid string) *Log { return &Log{store: s, session: uid} }

// Log is one session's durable, single-writer, hash-chained event log.
type Log struct {
	store   *Store
	session string
}

// Log satisfies the controller's error-returning event-log contract.
var _ eventlog.Store = (*Log)(nil)

// Head returns the seq of the last committed record (0 if empty).
func (l *Log) Head() (int64, error) {
	var head int64
	err := l.store.db.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = ?`, l.session,
	).Scan(&head)
	return head, err
}

// NewFence advances and returns this session's fencing token. A new incarnation calls it to take
// over; appends carrying an older token are rejected with eventlog.ErrFenced.
func (l *Log) NewFence() (int64, error) {
	var fence int64
	now := time.Now().UnixNano()
	err := l.store.db.QueryRow(
		`INSERT INTO sessions(session, fence, project, created_at, updated_at)
		 VALUES(?, 1, ?, ?, ?)
		 ON CONFLICT(session) DO UPDATE SET
		   fence      = sessions.fence + 1,
		   project    = CASE WHEN sessions.project = '' THEN excluded.project ELSE sessions.project END,
		   created_at = CASE WHEN sessions.created_at = 0 THEN excluded.created_at ELSE sessions.created_at END,
		   updated_at = excluded.updated_at
		 RETURNING fence`, l.session, l.store.defaultProject, now, now,
	).Scan(&fence)
	return fence, err
}

// Append commits ev as the next record iff expectedLastSeq == head (single-writer CAS) and fence
// is current, inside one transaction. It assigns seq, computes content_hash once via canon, and
// stores it alongside the wire-encoded event.
func (l *Log) Append(expectedLastSeq, fence int64, ev api.Event) (eventlog.Record, error) {
	tx, err := l.store.db.Begin()
	if err != nil {
		return eventlog.Record{}, err
	}
	defer tx.Rollback()

	var head int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = ?`, l.session,
	).Scan(&head); err != nil {
		return eventlog.Record{}, err
	}
	if expectedLastSeq != head {
		return eventlog.Record{}, eventlog.ErrConflict
	}

	var curFence int64
	if err := tx.QueryRow(
		`SELECT fence FROM sessions WHERE session = ?`, l.session,
	).Scan(&curFence); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return eventlog.Record{}, err
	}
	if fence < curFence {
		return eventlog.Record{}, eventlog.ErrFenced
	}

	prev := ""
	if head > 0 {
		if err := tx.QueryRow(
			`SELECT hash FROM events WHERE session = ? AND seq = ?`, l.session, head,
		).Scan(&prev); err != nil {
			return eventlog.Record{}, err
		}
	}
	seq := head + 1
	hash, err := canon.HashRecord(prev, seq, ev)
	if err != nil {
		return eventlog.Record{}, fmt.Errorf("sqlitelog: canonicalize seq %d: %w", seq, err)
	}
	blob, err := proto.Marshal(wire.EventToProto(ev))
	if err != nil {
		return eventlog.Record{}, fmt.Errorf("sqlitelog: marshal seq %d: %w", seq, err)
	}
	if _, err := tx.Exec(
		`INSERT INTO events(session, seq, prev_hash, hash, fence, event) VALUES(?,?,?,?,?,?)`,
		l.session, seq, prev, hash, fence, blob,
	); err != nil {
		// A PRIMARY KEY(session, seq) violation means another writer already committed this seq —
		// it advanced the log out from under us. That is exactly a single-writer CAS conflict, so
		// map it. Defense-in-depth for the cross-process race that MaxOpenConns cannot serialize
		// (BEGIN IMMEDIATE closes most of the window; this closes the rest).
		if isConstraint(err) {
			return eventlog.Record{}, eventlog.ErrConflict
		}
		return eventlog.Record{}, err
	}
	if err := touchSession(tx, l.session, l.store.defaultProject, ev); err != nil {
		return eventlog.Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return eventlog.Record{}, err
	}
	return eventlog.Record{Seq: seq, PrevHash: prev, Hash: hash, Fence: fence, Event: ev}, nil
}

// touchSession keeps the session's metadata row in step with its log, inside Append's transaction.
//
// It does two things. First, it guarantees the row exists: Append does not require one, so a
// session driven straight through the log — a direct Exec, a fork child, a caller that never took a
// fence — would otherwise have events but no metadata and be invisible to a listing. Second, when
// the committed event is a lifecycle transition, it advances the compute-state projection.
//
// The projection exists because events.event is an opaque blob that no query can filter on
// lifecycle kind, so a listing cannot fold the log itself. Running in the same transaction that
// commits the event is what keeps it from drifting: there is no window in which the log records a
// suspend that the projection missed, and every existing writer gets it for free because they all
// reach the log through Append.
//
// On conflict it touches only what it owns. compute_state moves only for an event that implies one,
// so a marker that says nothing about the incarnation cannot reset a suspended session to NONE;
// project and created_at are filled only when still unset, so an explicit PutSession is never
// overwritten by later log activity.
func touchSession(tx *sql.Tx, session, defaultProject string, ev api.Event) error {
	state, moves := eventComputeState(ev)
	if !moves {
		state = api.ComputeNone
	}
	now := time.Now().UnixNano()
	_, err := tx.Exec(
		`INSERT INTO sessions(session, fence, project, compute_state, created_at, updated_at)
		 VALUES(?, 0, ?, ?, ?, ?)
		 ON CONFLICT(session) DO UPDATE SET
		   compute_state = CASE WHEN ? THEN excluded.compute_state ELSE sessions.compute_state END,
		   project       = CASE WHEN sessions.project = '' THEN excluded.project ELSE sessions.project END,
		   created_at    = CASE WHEN sessions.created_at = 0 THEN excluded.created_at ELSE sessions.created_at END,
		   updated_at    = excluded.updated_at`,
		session, defaultProject, string(state), now, now, moves,
	)
	if err != nil {
		return fmt.Errorf("sqlitelog: touch session %q: %w", session, err)
	}
	return nil
}

// Read returns all records with seq >= fromSeq, in order. It returns the stored hashes verbatim
// and does not re-canonicalize — that is Verify's job (reviewer note: never re-hash on read).
func (l *Log) Read(fromSeq int64) ([]eventlog.Record, error) {
	rows, err := l.store.db.Query(
		`SELECT seq, prev_hash, hash, fence, event FROM events
		 WHERE session = ? AND seq >= ? ORDER BY seq`, l.session, fromSeq,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventlog.Record
	for rows.Next() {
		var (
			seq, fence     int64
			prevHash, hash string
			blob           []byte
		)
		if err := rows.Scan(&seq, &prevHash, &hash, &fence, &blob); err != nil {
			return nil, err
		}
		ev, err := decode(blob)
		if err != nil {
			return nil, fmt.Errorf("sqlitelog: decode seq %d: %w", seq, err)
		}
		out = append(out, eventlog.Record{Seq: seq, PrevHash: prevHash, Hash: hash, Fence: fence, Event: ev})
	}
	return out, rows.Err()
}

// Snapshot returns a copy of the full chain (used to seed a fork).
func (l *Log) Snapshot() ([]eventlog.Record, error) { return l.Read(1) }

// Verify walks the chain and re-derives each content_hash via canon. It returns ErrTampered if a
// prev_hash link or a content_hash does not match (an integrity violation), or a wrapped error
// for an internal decode/canonicalize failure (a bug/corruption) — keeping the two severities
// distinct so a caller can alert on tamper without conflating it with an operational fault.
func (l *Log) Verify() error {
	recs, err := l.Read(1)
	if err != nil {
		return err
	}
	prev := ""
	for _, r := range recs {
		if r.PrevHash != prev {
			return fmt.Errorf("%w: prev_hash mismatch at seq %d", ErrTampered, r.Seq)
		}
		want, err := canon.HashRecord(prev, r.Seq, r.Event)
		if err != nil {
			return fmt.Errorf("sqlitelog: canonicalize seq %d: %w", r.Seq, err)
		}
		if r.Hash != want {
			return fmt.Errorf("%w: content_hash mismatch at seq %d", ErrTampered, r.Seq)
		}
		prev = r.Hash
	}
	return nil
}

func decode(blob []byte) (api.Event, error) {
	pb := &v1.Event{}
	if err := proto.Unmarshal(blob, pb); err != nil {
		return api.Event{}, err
	}
	return wire.EventFromProto(pb), nil
}
