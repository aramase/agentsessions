package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/vendagent"
)

// The credential channel is the host's authority seam: the harness asks, the host vends for the
// session's principal, and the log records that it happened without recording the secret.

const vendedToken = "ghs_super_secret_value"

// principal is the session identity the host vends for. The harness never names it.
var principal = api.IdentityRef{Principal: "agent://gofer", Issuer: "https://example", Subject: "github:1494193"}

// credentialSource is a host credential source that records what it was asked for.
type credentialSource struct {
	calls []api.CredentialRequest
	for_  []api.IdentityRef
}

func (s *credentialSource) vend(p api.IdentityRef, req api.CredentialRequest) (api.Credential, error) {
	s.calls = append(s.calls, req)
	s.for_ = append(s.for_, p)
	return api.Credential{Token: vendedToken, ExpiresIn: 300}, nil
}

// journalContains reports whether the raw journal (every event, fully serialized) contains s. It is
// the blunt instrument on purpose: "the token is not written" has to hold for the WHOLE record, not
// just the field we remembered to check.
func journalContains(t *testing.T, log eventlog.Store, s string) bool {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	raw, err := json.Marshal(recs)
	if err != nil {
		t.Fatalf("marshal log: %v", err)
	}
	return strings.Contains(string(raw), s)
}

// A vended credential reaches the harness, and the journal records the REQUEST — who asked, for
// what, as whom — while the token itself never lands in the durable, hash-chained, forkable log.
func TestCredentialVendedButNeverJournaled(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	src := &credentialSource{}
	c, err := controller.New(log, echoModel,
		controller.WithCredentialSource(src.vend),
		controller.WithPrincipal(principal),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), vendagent.Harness{}, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// The host vended once, for the session's principal, for the provider the harness named.
	if len(src.calls) != 1 {
		t.Fatalf("vended %d times, want 1", len(src.calls))
	}
	if src.calls[0].Provider != vendagent.Provider {
		t.Errorf("vended provider %q, want %q", src.calls[0].Provider, vendagent.Provider)
	}
	if src.for_[0] != principal {
		t.Errorf("vended for %+v, want %+v", src.for_[0], principal)
	}

	// The request is on the log (audit), attributed to the principal.
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var found *api.Event
	for i := range recs {
		if recs[i].Event.Kind == api.EventCredentialRequest {
			found = &recs[i].Event
		}
	}
	if found == nil {
		t.Fatal("no CREDENTIAL_REQUEST event journaled: the authority a turn exercised must be auditable")
	}
	if found.Credential == nil || found.Credential.Provider != vendagent.Provider {
		t.Errorf("journaled request = %+v, want provider %q", found.Credential, vendagent.Provider)
	}
	if found.Actor != principal {
		t.Errorf("journaled actor = %+v, want %+v", found.Actor, principal)
	}

	// The token is not.
	if journalContains(t, log, vendedToken) {
		t.Fatal("the vended token was written to the journal: a durable, forkable log must never be a secret store")
	}

	// The harness did its authenticated work and said so, naming the principal, not the token.
	outs, err := c.Outputs()
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 || !strings.Contains(outs[0], principal.Subject) {
		t.Fatalf("outputs = %q, want the authenticated call reported for %s", outs, principal.Subject)
	}
}

// A host with no credential source fails closed, and tells the harness up front so it can degrade
// instead of failing blind.
func TestNoCredentialSourceFailsClosed(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	c, err := controller.New(log, echoModel, controller.WithPrincipal(principal))
	if err != nil {
		t.Fatal(err)
	}
	// vendagent reads Start.Identity.CanMintTokens and skips the authenticated call.
	if err := c.Exec(context.Background(), vendagent.Harness{}, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	outs, err := c.Outputs()
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 || !strings.Contains(outs[0], "no credential source") {
		t.Fatalf("outputs = %q, want the harness to degrade honestly", outs)
	}

	// A harness that asks anyway is refused rather than handed an empty token.
	fresh, err := controller.New(eventlog.AsStore(eventlog.New()), echoModel, controller.WithPrincipal(principal))
	if err != nil {
		t.Fatal(err)
	}
	err = fresh.Exec(context.Background(), &askingHarness{}, []api.Message{*api.TextMessage("user", "go")}, 0)
	if !errors.Is(err, controller.ErrNoCredentialSource) {
		t.Fatalf("err = %v, want ErrNoCredentialSource", err)
	}
}

// askingHarness ignores CanMintTokens and asks regardless.
type askingHarness struct{}

func (askingHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "asking"}, nil
}

func (askingHarness) Run(_ context.Context, _ *api.Start, sink api.EventSink) error {
	_, err := sink.Credential(api.CredentialRequest{Provider: "github"})
	return err
}

// Replay RE-VENDS rather than serving a recorded credential: nothing was recorded to serve, and a
// token is a capability with a lifetime, not content. Determinism is unaffected — the credential
// never enters the effect stream or the hash chain — so the replay still reproduces the outputs.
func TestReplayReVendsRatherThanServingFromTheJournal(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	src := &credentialSource{}
	c, err := controller.New(log, echoModel,
		controller.WithCredentialSource(src.vend),
		controller.WithPrincipal(principal),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), vendagent.Harness{}, []api.Message{*api.TextMessage("user", "go")}, 0); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	outs, err := c.Replay(context.Background(), vendagent.Harness{})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(src.calls) != 2 {
		t.Fatalf("vended %d times across live+replay, want 2 (replay re-vends)", len(src.calls))
	}
	if len(outs) != 1 || !strings.Contains(outs[0], principal.Subject) {
		t.Fatalf("replayed outputs = %q, want the live outputs reproduced", outs)
	}
	// Replay is read-only: re-vending must not append anything, not even a second audit record.
	after, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if after != head {
		t.Fatalf("replay appended to the log (head %d -> %d)", head, after)
	}
}
