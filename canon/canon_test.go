package canon_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/wire"
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

func TestExecutionStartCanonicalBytes(t *testing.T) {
	event := api.Event{
		ExecutionID: "exec-config", Kind: api.EventExecutionStart,
		ExecutionStart: &api.ExecutionStart{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740993, InputCount: proto.Int64(2)},
	}
	got, err := canon.Record("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	// Opaque bytes use proto3-JSON base64; the int64 cursor must not lose precision via a JSON number.
	want := `{"event":{"execution_id":"exec-config","execution_start":{"config":"AP8gCgk=","input_count":"2","resume_from_seq":"9007199254740993"},"kind":"EVENT_EXECUTION_START"},"prev_hash":"","seq":"1"}`
	if string(got) != want {
		t.Fatalf("canonical execution start = %s, want %s", got, want)
	}
	roundTrip, err := canon.Record("", 1, wire.EventFromProto(wire.EventToProto(event)))
	if err != nil || string(roundTrip) != want {
		t.Fatalf("wire canonical bytes = %s, %v", roundTrip, err)
	}
	original, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "aa1f42a26b7a227836d32d31608ebb69c59f3b1c57eadd8537b6faf88a01c790"
	if original != wantHash {
		t.Fatalf("execution start hash = %s, want %s (also update hack/verify_chain.py)", original, wantHash)
	}
	for _, changed := range []api.ExecutionStart{
		{Config: []byte{0, 255, ' ', '\n'}, ResumeFromSeq: 9007199254740993, InputCount: proto.Int64(2)},
		{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740992, InputCount: proto.Int64(2)},
		{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740993, InputCount: proto.Int64(1)},
		{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740993},
	} {
		event.ExecutionStart = &changed
		hash, err := canon.HashRecord("", 1, event)
		if err != nil {
			t.Fatal(err)
		}
		if hash == original {
			t.Fatal("execution config/cursor/input count is not bound into the hash")
		}
	}
}

func TestExecutionStartCanonicalInputCountPresence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count *int64
		want  string
	}{
		{"absent", nil, `{"event":{"execution_start":{},"kind":"EVENT_EXECUTION_START"},"prev_hash":"","seq":"1"}`},
		{"zero", proto.Int64(0), `{"event":{"execution_start":{"input_count":"0"},"kind":"EVENT_EXECUTION_START"},"prev_hash":"","seq":"1"}`},
		{"large", proto.Int64(9007199254740993), `{"event":{"execution_start":{"input_count":"9007199254740993"},"kind":"EVENT_EXECUTION_START"},"prev_hash":"","seq":"1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := api.Event{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: tc.count}}
			got, err := canon.Record("", 1, event)
			if err != nil || string(got) != tc.want {
				t.Fatalf("canonical count presence = %s, %v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestDefaultExecutionStartGoldenVectors(t *testing.T) {
	for _, tc := range []struct {
		count int64
		hash  string
	}{
		{0, "b09cbff45d5a30990c3b72be8b06af6bc509d5a29a849aab5fd42153ef10ec65"},
		{2, "f06c25ee5b3673ee23b8f91883c7f1c6c5682d94986caf5bc067908a61b2ac4f"},
	} {
		event := api.Event{ExecutionID: "exec-default", Kind: api.EventExecutionStart,
			ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(tc.count)}}
		got, err := canon.HashRecord("", 1, event)
		if err != nil || got != tc.hash {
			t.Fatalf("default start count %d hash = %s, %v; want %s (also update hack/verify_chain.py)", tc.count, got, err, tc.hash)
		}
	}
}

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
