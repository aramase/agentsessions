package canon_test

import (
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
)

// goldenEvent is a fixed event whose canonical hash is published below as an interop golden
// vector. Any independent implementation of "JCS over proto3-JSON" (see determinism contract §7)
// MUST reproduce goldenHash for this exact event — that is the proof the tamper-evident chain is
// verifiable by a non-Go auditor. Do not change this event without updating the vector.
func goldenEvent() api.Event {
	return api.Event{
		ExecutionID:   "exec-1",
		SchemaVersion: 1,
		Timestamp:     time.Unix(1_700_000_000, 0).UTC(), // 2023-11-14T22:13:20Z
		Kind:          api.EventOutput,
		Message: &api.Message{Role: "assistant", Parts: []api.Part{
			{Text: &api.TextPart{Text: "hello"}},
		}},
		Actor: api.IdentityRef{Principal: "agent://a", Issuer: "entra", Subject: "sub-1"},
	}
}

// goldenHash is content_hash for goldenEvent() at prev_hash="" seq=1. Published interop vector:
// an independent JCS-over-proto3-JSON implementation must reproduce this exact value.
const goldenHash = "551bd146050c8d630b0c3b999a4445f3792a470db9bca443d8d4a67706283fcc"

func TestGoldenVector(t *testing.T) {
	got, err := canon.HashRecord("", 1, goldenEvent())
	if err != nil {
		t.Fatalf("HashRecord: %v", err)
	}
	if got != goldenHash {
		t.Fatalf("golden vector mismatch:\n got:  %s\n want: %s\n"+
			"(if the canonical encoding changed intentionally, update goldenHash AND the "+
			"interop vector shared with other implementations)", got, goldenHash)
	}
}

// TestDeterministic guards the load-bearing property: protojson deliberately randomizes
// whitespace/field order across runs, and JCS must wash that out so the hash is stable.
func TestDeterministic(t *testing.T) {
	ev := goldenEvent()
	first, err := canon.Record("", 1, ev)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		b, err := canon.Record("", 1, ev)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != string(first) {
			t.Fatalf("canonical bytes not deterministic at iter %d", i)
		}
	}
}

// TestChainBinding checks that prev_hash and seq are bound into content_hash, so a child fork
// (prev_hash = parent hash, new seq) yields a different, stable hash — the hash-tree property.
func TestChainBinding(t *testing.T) {
	parent, err := canon.HashRecord("", 1, goldenEvent())
	if err != nil {
		t.Fatal(err)
	}
	child, err := canon.HashRecord(parent, 2, goldenEvent())
	if err != nil {
		t.Fatal(err)
	}
	if child == parent {
		t.Fatal("child hash must differ from parent (prev_hash + seq are bound in)")
	}
	// same inputs must reproduce the same child hash
	again, err := canon.HashRecord(parent, 2, goldenEvent())
	if err != nil {
		t.Fatal(err)
	}
	if again != child {
		t.Fatalf("child hash not stable: %s != %s", again, child)
	}
}
