package wire_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/wire"
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
