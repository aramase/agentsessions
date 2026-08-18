// Package vendagent is a STATELESS_REPLAY api.Harness that exercises the credential channel: it
// asks the HOST for a downstream credential, uses it, and reports what it did — without ever
// holding a standing secret or reaching a credential source itself.
//
// It stands in for the real shape of a delegated agent (fetch the user's GitHub work, call an API
// as the user): the workload knows WHICH provider it needs, the host knows WHOSE credential to
// vend, and the token exists only for the turn. Because acquisition is an EventSink call, the
// harness runs identically in-process and in a sandbox on the far side of Harness.Connect.
package vendagent

import (
	"context"
	"fmt"

	"github.com/aramase/agentsessions/api"
)

// Provider is the downstream credential this harness asks for.
const Provider = "github"

// Harness is the credential-vending BYOH. It holds no durable in-memory state beyond the event
// log — the credential is deliberately NOT state: it is re-vended on every execution, so replay
// and fork need no secret from the journal.
type Harness struct{}

// Describe returns the static contract: a stateless-replay harness that runs on any runtime.
func (Harness) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{
		ID:     "vend",
		Models: []string{"echo"},
		Capabilities: api.Capabilities{
			Resumability: api.ResumabilityStatelessReplay,
			ForkSafe:     true,
		},
	}, nil
}

// Run vends a credential for the session's principal and reports the call it would make with it.
// The token is used and dropped; what reaches the log is the request (audit) and the output, never
// the secret.
func (Harness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if !s.Identity.CanMintTokens {
		// Honest degradation: the host has no credential source, so do the part that needs no
		// authority rather than failing the turn.
		return sink.Output("no credential source; skipping the authenticated call")
	}
	cred, err := sink.Credential(api.CredentialRequest{Provider: Provider})
	if err != nil {
		return fmt.Errorf("vendagent: vend %s: %w", Provider, err)
	}
	if cred.Token == "" {
		return fmt.Errorf("vendagent: host vended an empty %s credential", Provider)
	}
	// A real harness would call the provider here. Report the principal it acted as, never the
	// token — the output is journaled.
	return sink.Output(fmt.Sprintf("called %s as %s", Provider, s.Identity.Principal.Subject))
}
