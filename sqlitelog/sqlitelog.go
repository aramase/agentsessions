// Package sqlitelog is the persistent, single-writer event log backing agentsessions on durable
// storage. It implements the same append-only, hash-chained, CAS + fencing semantics as the
// in-memory eventlog.Log, but survives process death — the property that makes "suspend on one
// pod, resume on another" real. content_hash is computed once at append via package canon
// (RFC 8785 JCS over proto3-JSON) and stored; reads return it verbatim and never re-canonicalize.
// A single database holds many sessions (a parent plus its forked children). It uses the pure-Go
// modernc.org/sqlite driver (no cgo), so it runs in a plain container image. ax's own event log
// is likewise sqlite/postgres-backed. See determinism contract §1, §5, §7.
package sqlitelog

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

const schema = `
CREATE TABLE IF NOT EXISTS sessions (
  session TEXT PRIMARY KEY,
  fence   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
  session   TEXT    NOT NULL,
  seq       INTEGER NOT NULL,
  prev_hash TEXT    NOT NULL,
  hash      TEXT    NOT NULL,
  fence     INTEGER NOT NULL,
  event     BLOB    NOT NULL,
  PRIMARY KEY (session, seq)
);`

// Store is a durable, multi-session event log over a single sqlite database.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the sqlite database at path and applies the schema. Use
// ":memory:" for an ephemeral store. For a file-backed store it uses BEGIN IMMEDIATE (via the
// _txlock DSN) so cross-process writers take the write lock upfront and serialize cleanly instead
// of deadlocking on a lock upgrade (SQLITE_BUSY), and synchronous=FULL so every committed
// transaction is durable across power loss, not just process death.
//
// Durability boundary: a *committed* append survives process death and power loss. A crash
// mid-append (before commit) drops only that uncommitted event, which is re-driven on resume
// (determinism contract I3/I4). TestPersistenceAcrossReopen proves the clean-restart case.
func Open(path string) (*Store, error) {
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
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000", // set first so the WAL-mode switch below waits for the lock
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlitelog: %q: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlitelog: schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// Session returns a handle to one session's append-only chain.
func (s *Store) Session(uid string) *Log { return &Log{store: s, session: uid} }

// Log is one session's durable, single-writer, hash-chained event log.
type Log struct {
	store   *Store
	session string
}

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
	err := l.store.db.QueryRow(
		`INSERT INTO sessions(session, fence) VALUES(?, 1)
		 ON CONFLICT(session) DO UPDATE SET fence = fence + 1
		 RETURNING fence`, l.session,
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
	if err := tx.Commit(); err != nil {
		return eventlog.Record{}, err
	}
	return eventlog.Record{Seq: seq, PrevHash: prev, Hash: hash, Fence: fence, Event: ev}, nil
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
