package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aramase/agentsessions/integrations/substrate/e2e/internal/approvalfixture"
	"github.com/aramase/agentsessions/runtime/local"
)

// This always runs offline: actual Sessions gRPC, real Harness.Connect park handshake and SQLite
// reopen/new Registry. It qualifies the fixture algorithm, not substrate snapshots or microVMs.
func TestApprovalFixturePublicSQLiteRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	backend := local.New(approvalfixture.Harness{})
	defer func() { _ = backend.Close() }()
	runApprovalSuite(t, ctx, backend, func(string) {}, false)
}
