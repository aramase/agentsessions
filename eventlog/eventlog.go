// Package eventlog is the reference in-memory event log for the agentsessions walking
// skeleton. It realizes three parts of the durable-log & replay contract:
//
//   - single-writer enforcement via compare-and-swap on the log head (expected_last_seq);
//   - an incarnation fencing token that rejects writes from a superseded incarnation;
//   - a per-record hash-chain (tamper-evident; becomes a hash-tree at a fork).
//
// content_hash is the language-neutral value from package canon (RFC 8785 JCS over
// proto3-JSON), so the chain is verifiable by any implementation — not only this Go host — and
// the integrity definition lives in exactly one place. A sqlite/Durable Task backend implements
// the same shape later. See agentsessions-replay-determinism-contract.md (§1, §5, §7).
package eventlog

import (
	"errors"
	"strconv"
	"sync"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
)

// ErrConflict is returned when Append's expectedLastSeq does not equal the log head
// (another writer advanced the log). This is the single-writer CAS.
var ErrConflict = errors.New("eventlog: seq conflict (single-writer compare-and-swap failed)")

// ErrFenced is returned when Append is called with a fencing token older than the
// current one — a superseded (zombie) incarnation trying to write.
var ErrFenced = errors.New("eventlog: stale fencing token (incarnation superseded)")

// Record is one committed log entry: the payload plus ordering, integrity, and fencing
// metadata. Hash = SHA-256(prevHash || canonical(payload)); a fork sets a child's first
// prevHash to the parent record's Hash, turning the chain into a tree.
type Record struct {
	Seq      int64
	PrevHash string
	Hash     string
	Fence    int64
	Event    api.Event
}

// Log is an append-only, single-writer event log with a hash-chain and fencing.
type Log struct {
	mu      sync.Mutex
	records []Record
	fence   int64
}

// New returns an empty log.
func New() *Log { return &Log{} }

// NewFrom returns a log seeded with a prefix (used to realize a replay-fork: the child
// starts from a copy of the parent's records up to the fork point).
func NewFrom(prefix []Record) *Log {
	cp := make([]Record, len(prefix))
	copy(cp, prefix)
	return &Log{records: cp}
}

// Head returns the seq of the last committed record (0 if empty).
func (l *Log) Head() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.headLocked()
}

func (l *Log) headLocked() int64 {
	if len(l.records) == 0 {
		return 0
	}
	return l.records[len(l.records)-1].Seq
}

// NewFence advances and returns the current fencing token. A new incarnation calls this
// to take over; older incarnations holding a smaller token can no longer Append.
func (l *Log) NewFence() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fence++
	return l.fence
}

// Append commits ev as the next record iff expectedLastSeq == head (CAS) and fence is
// current. It assigns Seq and links the hash-chain. The caller must not rely on ev.Seq.
func (l *Log) Append(expectedLastSeq, fence int64, ev api.Event) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if expectedLastSeq != l.headLocked() {
		return Record{}, ErrConflict
	}
	if fence < l.fence {
		return Record{}, ErrFenced
	}

	prev := ""
	if n := len(l.records); n > 0 {
		prev = l.records[n-1].Hash
	}
	seq := l.headLocked() + 1
	hash, err := canon.HashRecord(prev, seq, ev)
	if err != nil {
		return Record{}, err
	}
	rec := Record{
		Seq:      seq,
		PrevHash: prev,
		Hash:     hash,
		Fence:    fence,
		Event:    ev,
	}
	l.records = append(l.records, rec)
	return rec, nil
}

// Read returns a copy of all records with Seq >= fromSeq, in order. Replay reads from a
// baseline (or 0) up to the resume point.
func (l *Log) Read(fromSeq int64) []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Record, 0, len(l.records))
	for _, r := range l.records {
		if r.Seq >= fromSeq {
			out = append(out, r)
		}
	}
	return out
}

// Snapshot returns a copy of the full record slice (used to seed a fork).
func (l *Log) Snapshot() []Record { return l.Read(1) }

// Verify walks the chain and returns an error if any link is broken.
func (l *Log) Verify() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := ""
	for i, r := range l.records {
		if r.PrevHash != prev {
			return errors.New("eventlog: broken chain at index " + strconv.Itoa(i) + " (prev_hash mismatch)")
		}
		want, err := canon.HashRecord(prev, r.Seq, r.Event)
		if err != nil {
			return err
		}
		if r.Hash != want {
			return errors.New("eventlog: content hash mismatch at index " + strconv.Itoa(i))
		}
		prev = r.Hash
	}
	return nil
}
