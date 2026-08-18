package e2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

// TestStatelessReplayOnGVisor is the stateless tier on real substrate: place the echo harness
// (ResumeActor{boot:true}), drive one turn over a direct dial to the actor's PodIP, then replay the
// journal through the same harness and require it byte-identical with the model never invoked (I1).
//
// This is the claim that a session is reconstructible from its log alone, made against real compute
// rather than an in-process fake.
func TestStatelessReplayOnGVisor(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-replay"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(echoTemplate, desc)
	session := uniqueUID("replay")
	log := journal(t).Session(session)
	p := placement.New(backend, echoagent.Model)

	inc, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "hi")}, 0)
	if inc.ID != "" {
		defer stopQuietly(t, backend, inc.ID)
	}
	if err != nil {
		t.Fatalf("placed exec: %v", err)
	}
	t.Logf("placed actor: address=%s worker=%s", inc.Address, inc.Worker)

	live := lastOutput(t, log)
	if live == "" {
		t.Fatal("placed turn produced no output")
	}

	har, closeHar, err := dialActor(inc.Address)
	if err != nil {
		t.Fatalf("replay dial %s: %v", inc.Address, err)
	}
	defer func() { _ = closeHar() }()

	c, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.Replay(ctx, har)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if want := []string{live}; !reflect.DeepEqual(replayed, want) {
		t.Fatalf("replay diverged: got %v want %v", replayed, want)
	}
	if n := c.ModelInvocations(); n != 0 {
		t.Fatalf("replay invoked the model %d times, want 0 (I1)", n)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("chain verify: %v", err)
	}
	t.Log("replay byte-identical, model invocations=0, chain verified")
}
