package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
)

// TestSuspendUnderAnIdleHarnessStream checkpoints a session while its turn is parked on a model call,
// so its Connect stream is open and idle through the router. The worker drains an actor's in-flight
// requests before it snapshots, and that stream is one of them.
//
// It runs the checkpoint twice on separate sessions: once straight through the Runtime with the
// stream left open (what the Placer did before it closed streams first), recorded for comparison
// only, and once through Placer.Suspend, which must close the stream and then succeed promptly.
func TestSuspendUnderAnIdleHarnessStream(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-suspend-idle"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(t, echoTemplateSpec, desc)

	// park starts a turn whose model call never answers on its own, and returns once the harness has
	// sent that call, i.e. once the Connect stream is open and idle.
	park := func(p *placement.Placer, log eventlog.Store, session string, called chan struct{}) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "idle")}, 0)
			done <- err
		}()
		select {
		case <-called:
		case err := <-done:
			t.Fatalf("turn for %s returned before parking on the model call: %v", session, err)
		case <-ctx.Done():
			t.Fatalf("turn for %s never reached the model call", session)
		}
		return done
	}
	parkingModel := func(called chan struct{}) func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		return func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
			close(called)
			<-ctx.Done()
			return api.ModelResponse{}, context.Cause(ctx)
		}
	}

	// Baseline: checkpoint through the Runtime while the stream stays open.
	rawSession := uniqueUID("suspend-raw")
	rawCalled := make(chan struct{})
	rawCtx, rawCancel := context.WithCancel(ctx)
	rawPlacer := placement.New(backend, parkingModel(rawCalled))
	rawDone := make(chan error, 1)
	go func() {
		_, err := rawPlacer.Exec(rawCtx, journal(t).Session(rawSession), rawSession, []api.Message{*api.TextMessage("user", "idle")}, 0)
		rawDone <- err
	}()
	select {
	case <-rawCalled:
	case err := <-rawDone:
		t.Fatalf("baseline turn returned before parking: %v", err)
	case <-ctx.Done():
		t.Fatal("baseline turn never reached the model call")
	}
	start := time.Now()
	_, rawErr := backend.Snapshot(ctx, api.Incarnation{ID: rawSession}, api.SnapshotExternal)
	t.Logf("BASELINE: SuspendActor with an idle Connect stream open took %s, err=%v", time.Since(start).Round(time.Millisecond), rawErr)
	rawCancel()
	<-rawDone
	stopQuietly(t, backend, rawSession)

	// Placer.Suspend closes the session's streams first.
	session := uniqueUID("suspend-placer")
	called := make(chan struct{})
	p := placement.New(backend, parkingModel(called))
	log := journal(t).Session(session)
	done := park(p, log, session, called)
	start = time.Now()
	if _, err := p.Suspend(ctx, log, session); err != nil {
		t.Fatalf("Placer.Suspend with a parked turn: %v", err)
	}
	took := time.Since(start)
	t.Logf("PLACER: Suspend with a parked turn took %s (stream closed first)", took.Round(time.Millisecond))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the parked turn reported success after its session was suspended")
		}
		t.Logf("parked turn ended with: %v", err)
	case <-time.After(time.Minute):
		t.Fatal("the parked turn never returned after its stream was closed")
	}
}
