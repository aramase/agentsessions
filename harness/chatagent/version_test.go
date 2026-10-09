package chatagent_test

import (
	"github.com/aramase/agentsessions/harness/chatagent"
	"testing"
)

func TestStableReplayVersion(t *testing.T) {
	for _, model := range []string{"first", "second"} {
		desc, err := (chatagent.Harness{Model: model}).Describe(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if desc.Version != "1" {
			t.Fatalf("replay version = %q, want 1", desc.Version)
		}
	}
}
