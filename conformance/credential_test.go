package conformance_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harness/vendagent"
)

// This is the conformance proof for the credential channel across the process boundary: a harness
// running OUT OF PROCESS (a real Harness.Connect stream) obtains a downstream credential from the
// host, and the host's journal records the request but not the token.
//
// It is the case that matters — an in-process harness can always be handed a credential store by
// reference, so only the wire path proves the contract is real.

const wireToken = "ghs_wire_secret_value"

var wirePrincipal = api.IdentityRef{Principal: "agent://gofer", Issuer: "https://example", Subject: "github:1494193"}

// TestWireCredentialVendedByHost drives the vend harness out of process. The harness holds no
// secret and reaches no credential source: it emits a credential request on the stream and the
// host answers it, exactly as it mediates a model call.
func TestWireCredentialVendedByHost(t *testing.T) {
	var vendedFor []api.IdentityRef
	var asked []string
	vend := func(p api.IdentityRef, req api.CredentialRequest) (api.Credential, error) {
		vendedFor = append(vendedFor, p)
		asked = append(asked, req.Provider)
		return api.Credential{Token: wireToken, ExpiresIn: 300}, nil
	}

	log := eventlog.AsStore(eventlog.New())
	c, err := controller.New(log, echoagent.Model,
		controller.WithCredentialSource(vend),
		controller.WithPrincipal(wirePrincipal),
	)
	if err != nil {
		t.Fatal(err)
	}
	remote := wireHarnessFrom(t, vendagent.Harness{})

	if err := c.Exec(context.Background(), remote, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// The identity context crossed the boundary: the sandbox learned whose credential it may ask
	// for, and asked for the provider it needs.
	if len(asked) != 1 || asked[0] != vendagent.Provider {
		t.Fatalf("providers asked for = %v, want [%s]", asked, vendagent.Provider)
	}
	if len(vendedFor) != 1 || vendedFor[0] != wirePrincipal {
		t.Fatalf("vended for %+v, want %+v", vendedFor, wirePrincipal)
	}

	// The remote harness used the credential and reported the principal it acted as.
	outs, err := c.Outputs()
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 || !strings.Contains(outs[0], wirePrincipal.Subject) {
		t.Fatalf("outputs = %q, want the authenticated call reported for %s", outs, wirePrincipal.Subject)
	}

	// The request is auditable; the token is not durable.
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var sawRequest bool
	for _, r := range recs {
		if r.Event.Kind == api.EventCredentialRequest {
			sawRequest = true
		}
	}
	if !sawRequest {
		t.Error("no CREDENTIAL_REQUEST journaled for the remote vend")
	}
	raw, err := json.Marshal(recs)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), wireToken) {
		t.Fatal("the vended token reached the journal over the wire path")
	}
}

// TestWireCredentialRefusalReachesTheHarness proves a refusal is an ANSWER, not a broken stream:
// the host reports it in the result frame, the harness sees a normal error and can decide what to
// do, and the turn ends as a harness failure rather than a transport failure.
func TestWireCredentialRefusalReachesTheHarness(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	// No credential source: the host refuses.
	c, err := controller.New(log, echoagent.Model, controller.WithPrincipal(wirePrincipal))
	if err != nil {
		t.Fatal(err)
	}
	// askingHarness ignores CanMintTokens, so the refusal has to travel back over the wire.
	remote := wireHarnessFrom(t, wireAskingHarness{})

	err = c.Exec(context.Background(), remote, []api.Message{*api.TextMessage("user", "go")}, 0)
	if err == nil {
		t.Fatal("want an error when the host has no credential source")
	}
	if !strings.Contains(err.Error(), "no credential source") {
		t.Fatalf("err = %v, want the host's refusal reason to survive the boundary", err)
	}
}

// wireAskingHarness asks for a credential without checking whether the host offers one.
type wireAskingHarness struct{}

func (wireAskingHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "asking"}, nil
}

func (wireAskingHarness) Run(_ context.Context, _ *api.Start, sink api.EventSink) error {
	_, err := sink.Credential(api.CredentialRequest{Provider: "github"})
	return err
}
