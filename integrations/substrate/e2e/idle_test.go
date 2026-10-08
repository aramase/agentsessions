package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

// TestHarnessStreamIdlePastRouteTimeout holds one harnesswire Connect stream open and idle through
// the atenet-router for longer than the router's route timeout (5m by default, --route-timeout), the
// way a turn parked on a slow model call does: the harness has sent its model call and waits for the
// host's reply, and nothing crosses the stream until the host answers.
//
// It records what the router does to such a stream. It is opt-in because it runs for the whole gap:
// set E2E_IDLE_GAP (for example 5m30s) to run it.
func TestHarnessStreamIdlePastRouteTimeout(t *testing.T) {
	gapSetting := env("E2E_IDLE_GAP", "")
	if gapSetting == "" {
		t.Skip("set E2E_IDLE_GAP (e.g. 5m30s) to hold a harness stream idle across the router's route timeout")
	}
	gap, err := time.ParseDuration(gapSetting)
	if err != nil {
		t.Fatalf("E2E_IDLE_GAP=%q: %v", gapSetting, err)
	}
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-idle"))
	ctx, cancel := context.WithTimeout(context.Background(), gap+10*time.Minute)
	defer cancel()

	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(t, echoTemplateSpec, desc)
	session := uniqueUID("idle")
	log := journal(t).Session(session)

	// The model call is where the stream sits idle: the host holds the harness's model call for the
	// whole gap before it replies.
	var answeredAfter time.Duration
	var modelStart time.Time
	slowModel := func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		modelStart = time.Now()
		select {
		case <-time.After(gap):
		case <-ctx.Done():
			return api.ModelResponse{}, context.Cause(ctx)
		}
		answeredAfter = time.Since(modelStart)
		return echoagent.Model(ctx, req)
	}
	p := placement.New(backend, slowModel)

	start := time.Now()
	inc, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "idle")}, 0)
	elapsed := time.Since(start)
	if inc.ID != "" {
		defer stopQuietly(t, backend, inc.ID)
	}
	if ctx.Err() != nil {
		t.Fatalf("the turn was still running when the test deadline expired after %s: a stream the router "+
			"tore down must surface as an error, not a hang", elapsed.Round(time.Second))
	}

	if err == nil {
		t.Logf("OUTCOME: stream survived. Idle gap %s, model answered after %s, turn completed in %s, output %q",
			gap, answeredAfter.Round(time.Second), elapsed.Round(time.Second), lastOutput(t, log))
		return
	}
	idleFor := time.Duration(0)
	if !modelStart.IsZero() {
		idleFor = time.Since(modelStart)
	}
	var code string
	if s, ok := status.FromError(errors.Unwrap(err)); ok {
		code = s.Code().String()
	} else if s, ok := status.FromError(err); ok {
		code = s.Code().String()
	}
	t.Logf("OUTCOME: stream torn down. Idle gap %s, turn failed after %s (stream idle %s), grpc code %q, error: %v",
		gap, elapsed.Round(time.Second), idleFor.Round(time.Second), code, err)
}
