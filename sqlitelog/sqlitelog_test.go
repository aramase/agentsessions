package sqlitelog_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

func sampleEvents() []api.Event {
	return []api.Event{
		{Kind: api.EventInput, Message: api.TextMessage("user", "drive")},
		{Kind: api.EventModelCall, ModelCall: &api.ModelCall{Model: "echo", InputHash: "h", ID: "mc1"}},
		{Kind: api.EventOutput, Message: api.TextMessage("assistant", "hello")},
		{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	}
}

// appendAll takes a fresh fence and appends events in order, returning the committed records.
func appendAll(t *testing.T, l *sqlitelog.Log, evs []api.Event) []eventlog.Record {
	t.Helper()
	fence, err := l.NewFence()
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	var recs []eventlog.Record
	for i, ev := range evs {
		rec, err := l.Append(int64(i), fence, ev)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		recs = append(recs, rec)
	}
	return recs
}

// TestPersistenceAcrossReopen is the load-bearing property: a session written by one process
// survives that process dying (Close) and is intact + verifiable when a fresh process reopens the
// database. This is what makes suspend-on-pod-A / resume-on-pod-B real.
func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.db")
	evs := sampleEvents()

	s1, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, s1.Session("sess-1"), evs)
	if err := s1.Close(); err != nil { // simulate pod death
		t.Fatal(err)
	}

	s2, err := sqlitelog.Open(path) // fresh pod
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	log := s2.Session("sess-1")

	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != int64(len(evs)) {
		t.Fatalf("head after reopen = %d, want %d", head, len(evs))
	}
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(evs) {
		t.Fatalf("read %d records, want %d", len(recs), len(evs))
	}
	if got := recs[2].Event.Message.Text(); got != "hello" {
		t.Fatalf("event content lost across reopen: %q", got)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
}

func TestSingleWriterCAS(t *testing.T) {
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	l := s.Session("s")
	fence, _ := l.NewFence()
	if _, err := l.Append(0, fence, sampleEvents()[0]); err != nil {
		t.Fatal(err)
	}
	// stale expected_last_seq (0, but head is now 1) must be rejected.
	if _, err := l.Append(0, fence, sampleEvents()[1]); !errors.Is(err, eventlog.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestFencing(t *testing.T) {
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	l := s.Session("s")
	old, _ := l.NewFence()   // 1
	newer, _ := l.NewFence() // 2 supersedes old
	if newer <= old {
		t.Fatalf("fence did not advance: %d -> %d", old, newer)
	}
	// a zombie incarnation holding the old fence can no longer write.
	if _, err := l.Append(0, old, sampleEvents()[0]); !errors.Is(err, eventlog.ErrFenced) {
		t.Fatalf("want ErrFenced, got %v", err)
	}
}

// TestHashMatchesCanon is the cross-backend guard: the persistent log's stored content_hash must
// equal canon.HashRecord — the same value the in-memory log produces — so both backends yield the
// same tamper-evident chain and a resume across backends stays byte-identical.
func TestHashMatchesCanon(t *testing.T) {
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recs := appendAll(t, s.Session("s"), sampleEvents())
	prev := ""
	for _, r := range recs {
		want, err := canon.HashRecord(prev, r.Seq, r.Event)
		if err != nil {
			t.Fatal(err)
		}
		if r.Hash != want {
			t.Fatalf("seq %d stored hash != canon: %s != %s", r.Seq, r.Hash, want)
		}
		prev = r.Hash
	}
}

// TestTamperDetected simulates an operator editing the database directly and asserts Verify flags
// it as ErrTampered (an integrity violation), distinct from an internal/operational error.
func TestTamperDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.db")
	s, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, s.Session("s"), sampleEvents())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE events SET hash = 'tampered' WHERE session = 's' AND seq = 2`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	s2, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.Session("s").Verify(); !errors.Is(err, sqlitelog.ErrTampered) {
		t.Fatalf("want ErrTampered, got %v", err)
	}
}

func TestMultiSessionIsolation(t *testing.T) {
	s, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, b := s.Session("a"), s.Session("b")
	appendAll(t, a, sampleEvents()[:2])
	appendAll(t, b, sampleEvents()[:3])
	ha, _ := a.Head()
	hb, _ := b.Head()
	if ha != 2 || hb != 3 {
		t.Fatalf("session isolation broken: a=%d b=%d", ha, hb)
	}
	if err := a.Verify(); err != nil {
		t.Fatalf("verify a: %v", err)
	}
	if err := b.Verify(); err != nil {
		t.Fatalf("verify b: %v", err)
	}
}

// TestNoRawErrorUnderContention runs independent Stores (distinct "pods") against the same file
// and races them to commit seq 1. The single-writer contract must hold cross-process: exactly one
// append succeeds, and every rejection maps to a typed eventlog error (ErrConflict/ErrFenced) —
// never a raw sqlite BUSY or UNIQUE leak. This exercises BEGIN IMMEDIATE plus the UNIQUE→ErrConflict
// mapping, the two hardenings that make the contract hold where MaxOpenConns(1) cannot serialize.
func TestNoRawErrorUnderContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.db")
	const writers = 8

	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	var failures []error

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sqlitelog.Open(path)
			if err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
				return
			}
			defer s.Close()
			l := s.Session("race")
			fence, err := l.NewFence()
			if err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
				return
			}
			_, err = l.Append(0, fence, sampleEvents()[0])
			mu.Lock()
			if err == nil {
				successes++
			} else {
				failures = append(failures, err)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("want exactly 1 successful append across %d writers, got %d", writers, successes)
	}
	for _, err := range failures {
		if !errors.Is(err, eventlog.ErrConflict) && !errors.Is(err, eventlog.ErrFenced) {
			t.Fatalf("raw error leaked (want ErrConflict/ErrFenced): %v", err)
		}
	}
}

// TestOpenWaitsOutAConcurrentInitializer pins the retry in setWALMode. Sqlite does not run the
// busy handler for PRAGMA journal_mode, so a second opener racing the process that is creating the
// schema used to fail instantly with SQLITE_BUSY instead of waiting like every other statement.
func TestOpenWaitsOutAConcurrentInitializer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contended.db")

	// Stand in for an opener that has created the schema and still holds the write lock, on a
	// database that is not in WAL mode yet.
	holder, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	holder.SetMaxOpenConns(1)
	if _, err := holder.Exec(`CREATE TABLE sessions (
	  session    TEXT PRIMARY KEY,
	  fence      INTEGER NOT NULL DEFAULT 0,
	  project    TEXT    NOT NULL DEFAULT '',
	  created_at INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatalf("holder schema: %v", err)
	}
	tx, err := holder.Begin()
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO sessions(session, fence) VALUES ('held', 1)`); err != nil {
		t.Fatalf("holder write: %v", err)
	}

	opened := make(chan error, 1)
	go func() {
		s, err := sqlitelog.Open(path)
		if err == nil {
			t.Cleanup(func() { s.Close() })
		}
		opened <- err
	}()

	// Long enough that an unretried journal_mode switch (measured at ~90µs) has certainly run.
	select {
	case err := <-opened:
		t.Fatalf("Open returned while the write lock was held: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("holder commit: %v", err)
	}
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("Open after the write lock was released: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Open did not return after the write lock was released")
	}
}
