package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/substrate"
)

// TestSuspendUnderAnIdleHarnessStream suspends a session while its turn is parked on a model call,
// so its Connect stream is open and idle through the router. The worker drains an actor's in-flight
// requests before it snapshots, and that stream is one of them.
//
// Placer.Suspend does not interrupt the turn: it must be refused with ErrSessionBusy while the turn
// runs, and once the turn has returned, and its stream is closed, it must succeed promptly. With
// E2E_SUSPEND_BASELINE=1 it first checkpoints another session straight through the Runtime with the
// stream left open, the checkpoint the refusal avoids, and records how long that takes. It is opt-in
// because it is slow: on a kind cluster at substrate 362637f9 it took 5m30s, i.e. the suspend waited
// until the router gave up on the stream.
func TestSuspendUnderAnIdleHarnessStream(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-suspend-idle"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(t, echoTemplateSpec, desc)

	// park starts a turn whose model call never answers on its own, and returns once the harness has
	// sent that call, i.e. once the Connect stream is open and idle.
	park := func(turnCtx context.Context, p *placement.Placer, log eventlog.Store, session string, called chan struct{}) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := p.Exec(turnCtx, log, session, []api.Message{*api.TextMessage("user", "idle")}, 0)
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

	if env("E2E_SUSPEND_BASELINE", "") == "1" {
		suspendBaseline(ctx, t, backend, parkingModel)
	}

	// Placer.Suspend refuses the session while its turn runs and leaves the turn alone.
	session := uniqueUID("suspend-placer")
	defer stopQuietly(t, backend, session)
	called := make(chan struct{})
	p := placement.New(backend, parkingModel(called))
	log := journal(t).Session(session)
	turnCtx, endTurn := context.WithCancel(ctx)
	defer endTurn()
	done := park(turnCtx, p, log, session, called)
	start := time.Now()
	if _, err := p.Suspend(ctx, log, session); !errors.Is(err, placement.ErrSessionBusy) {
		t.Fatalf("Placer.Suspend with a parked turn returned %v, want ErrSessionBusy (ABORTED)", err)
	}
	t.Logf("PLACER: Suspend with a parked turn was refused in %s", time.Since(start).Round(time.Millisecond))
	select {
	case err := <-done:
		t.Fatalf("the refused Suspend ended the parked turn: %v", err)
	default:
	}

	// Once the turn has returned, its stream is closed and Suspend goes through.
	endTurn()
	select {
	case err := <-done:
		t.Logf("parked turn ended with: %v", err)
	case <-time.After(time.Minute):
		t.Fatal("the parked turn never returned after its caller cancelled it")
	}
	start = time.Now()
	if _, err := p.Suspend(ctx, log, session); err != nil {
		t.Fatalf("Placer.Suspend after the turn returned: %v", err)
	}
	t.Logf("PLACER: Suspend after the turn returned took %s", time.Since(start).Round(time.Millisecond))
}

// suspendBaseline checkpoints a session through the Runtime while its turn's stream stays open and
// idle, and logs how long the suspend takes.
func suspendBaseline(ctx context.Context, t *testing.T, backend *substrate.Backend, parkingModel func(chan struct{}) func(context.Context, api.ModelRequest) (api.ModelResponse, error)) {
	t.Helper()
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
}
