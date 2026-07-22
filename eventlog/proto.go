package eventlog

import (
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/wire"
)

// RecordToProto wraps a committed Record as a wire LogRecord: the host-assigned envelope
// (seq, prev_hash, content_hash, fence) plus the hash-free Event. content_hash is the
// language-neutral hash from package canon (carried verbatim; not recomputed here).
func RecordToProto(r Record) *v1.LogRecord {
	return &v1.LogRecord{
		Seq:         r.Seq,
		PrevHash:    r.PrevHash,
		ContentHash: r.Hash,
		Fence:       r.Fence,
		Event:       wire.EventToProto(r.Event),
	}
}

// RecordFromProto converts a wire LogRecord back to a Record.
func RecordFromProto(p *v1.LogRecord) Record {
	if p == nil {
		return Record{}
	}
	return Record{
		Seq:      p.GetSeq(),
		PrevHash: p.GetPrevHash(),
		Hash:     p.GetContentHash(),
		Fence:    p.GetFence(),
		Event:    wire.EventFromProto(p.GetEvent()),
	}
}
