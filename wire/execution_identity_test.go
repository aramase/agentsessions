package wire_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/canon"
	"github.com/agent-primitives/sessions/wire"
)

func TestExecutionStartHarnessIdentityRoundTrip(t *testing.T) {
	for _, version := range []string{"", "replay-v1"} {
		t.Run(version, func(t *testing.T) {
			count := int64(1)
			start := &api.ExecutionStart{Config: []byte("config"), ResumeFromSeq: 7, InputCount: &count, Harness: "registered-alias", HarnessVersion: version}
			event := api.Event{ExecutionID: "run", Kind: api.EventExecutionStart, ExecutionStart: start}
			encoded := wire.EventToProto(event)
			if got := encoded.GetExecutionStart(); got.GetHarness() != start.Harness || got.GetHarnessVersion() != version {
				t.Fatalf("wire start = %#v", got)
			}
			if got := wire.EventFromProto(encoded).ExecutionStart; !reflect.DeepEqual(got, start) {
				t.Fatalf("round-trip start = %#v, want %#v", got, start)
			}
		})
	}
}

func TestExecutionStartIdentityFieldsAffectHashButPreserveLegacyHash(t *testing.T) {
	count := int64(1)
	start := api.ExecutionStart{Config: []byte("config"), ResumeFromSeq: 7, InputCount: &count}
	event := api.Event{ExecutionID: "run", Kind: api.EventExecutionStart, ExecutionStart: &start}
	legacy, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	// Captured on main before either identity field existed; absent strings must stay omitted.
	const oldHash = "62c7fde67689b1bd48d9c4ba13f86755afbbe5920429f785a82220249f06eeb8"
	if legacy != oldHash {
		t.Fatalf("legacy marker hash changed: %s", legacy)
	}
	canonical, err := canon.Event(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "harness") {
		t.Fatalf("absent legacy identity emitted: %s", canonical)
	}
	start.Harness = "alias"
	withName, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	if withName == legacy {
		t.Fatal("harness name missing from hash")
	}
	start.HarnessVersion = "v1"
	withVersion, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	if withVersion == withName {
		t.Fatal("harness version missing from hash")
	}
	start.HarnessVersion = "v2"
	changedVersion, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	if changedVersion == withVersion {
		t.Fatal("different version reused hash")
	}
}

// The compute instance a memory-tier turn ran on survives the wire and is bound into the record's
// hash, so it cannot be rewritten without breaking the chain. An empty one is omitted, so a start
// marker that records none hashes as it did before the field existed.
func TestExecutionStartComputeInstanceRoundTripAndHash(t *testing.T) {
	count := int64(1)
	start := &api.ExecutionStart{InputCount: &count, Harness: "mem", ComputeInstance: "0b5c3c56-6d5e-4f0e-9b1a-2f4d8c1e7a90"}
	event := api.Event{ExecutionID: "run", Kind: api.EventExecutionStart, ExecutionStart: start}
	encoded := wire.EventToProto(event)
	if got := encoded.GetExecutionStart().GetComputeInstance(); got != start.ComputeInstance {
		t.Fatalf("wire compute_instance = %q, want %q", got, start.ComputeInstance)
	}
	if got := wire.EventFromProto(encoded).ExecutionStart; !reflect.DeepEqual(got, start) {
		t.Fatalf("round-trip start = %#v, want %#v", got, start)
	}
	canonical, err := canon.Event(event)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(canonical), `"compute_instance":"0b5c3c56-6d5e-4f0e-9b1a-2f4d8c1e7a90"`) {
		t.Fatalf("canonical bytes omit the compute instance: %s", canonical)
	}
	with, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	start.ComputeInstance = "7f1e2d3c-4b5a-4968-8776-655443322110"
	other, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	start.ComputeInstance = ""
	without, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	if with == other || with == without {
		t.Fatal("compute instance is not bound into the record hash")
	}
	canonical, err = canon.Event(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "compute_instance") {
		t.Fatalf("absent compute instance emitted: %s", canonical)
	}
}
