package eventlog_test

import (
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/eventlog"
)

// TestHashMatchesCanon is the guard that keeps the log's content_hash and the canonical spec
// from diverging again: the hash the log stores MUST equal canon.HashRecord for the same
// (prev_hash, seq, event). Without this, eventlog could silently fall back to a non-neutral hash
// and the "any implementation can verify the chain" property would quietly break.
func TestHashMatchesCanon(t *testing.T) {
	l := eventlog.New()
	fence := l.NewFence()
	events := []api.Event{
		{Kind: api.EventInput, Message: api.TextMessage("user", "drive")},
		{Kind: api.EventOutput, Message: api.TextMessage("assistant", "ok")},
		{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	}
	prev := ""
	for i, ev := range events {
		rec, err := l.Append(int64(i), fence, ev)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		want, err := canon.HashRecord(prev, rec.Seq, ev)
		if err != nil {
			t.Fatalf("canon %d: %v", i, err)
		}
		if rec.Hash != want {
			t.Fatalf("record %d hash != canon.HashRecord: %s != %s", i, rec.Hash, want)
		}
		prev = rec.Hash
	}
	if err := l.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
