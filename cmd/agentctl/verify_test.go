package main

import (
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/wire"
)

func TestVerifyRecords(t *testing.T) {
	records := makeRecords(t,
		api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hello")},
		api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", "hi")},
	)
	if err := verifyRecords(records, 2); err != nil {
		t.Fatal(err)
	}

	tampered := makeRecords(t,
		api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hello")},
	)
	tampered[0].ContentHash = "tampered"
	if err := verifyRecords(tampered, 1); err == nil || !strings.Contains(err.Error(), "content_hash mismatch") {
		t.Fatalf("error = %v, want content hash failure", err)
	}

	gap := makeRecords(t, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hello")})
	gap[0].Seq = 2
	if err := verifyRecords(gap, 1); err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("error = %v, want sequence failure", err)
	}

	if err := verifyRecords(records[:1], 2); err == nil || !strings.Contains(err.Error(), "session metadata reports 2") {
		t.Fatalf("error = %v, want truncated journal failure", err)
	}
	if err := verifyRecords(nil, 0); err != nil {
		t.Fatalf("empty existing journal: %v", err)
	}
}

func makeRecords(t *testing.T, events ...api.Event) []*v1.LogRecord {
	t.Helper()
	records := make([]*v1.LogRecord, 0, len(events))
	prev := ""
	for i, event := range events {
		seq := int64(i + 1)
		hash, err := canon.HashRecord(prev, seq, event)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, &v1.LogRecord{
			Seq:         seq,
			PrevHash:    prev,
			ContentHash: hash,
			Event:       wire.EventToProto(event),
		})
		prev = hash
	}
	return records
}
