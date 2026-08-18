package echoagent_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/sqlitelog"
)

// TestEchoAdvanceReplay closes the loop for the exported echo harness: a live turn echoes the input,
// and a fresh controller replays the journal byte-identically with zero model invocations — the
// same property the pod demo shows across an actual pod restart.
func TestEchoAdvanceReplay(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	c, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Advance(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "ping")}, 0); err != nil {
		t.Fatal(err)
	}
	live, _ := c.Outputs()
	if len(live) != 1 || live[0] != "echo:ping" {
		t.Fatalf("unexpected echo output: %v", live)
	}

	c2, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := c2.Replay(context.Background(), echoagent.Harness{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live, replay) {
		t.Fatalf("replay != live: %v vs %v", replay, live)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatal("replay invoked the model (I1)")
	}
}
