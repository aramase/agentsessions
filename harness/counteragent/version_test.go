package counteragent

import "testing"

func TestStableReplayVersion(t *testing.T) {
	desc, err := (&Harness{}).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if desc.Version != "1" {
		t.Fatalf("replay version = %q, want 1", desc.Version)
	}
}
