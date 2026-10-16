package e2e

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/harness/echoagent"
	ateadapter "github.com/agent-primitives/sessions/integrations/substrate"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// counterDescriptor is the in-RAM counter: state the journal cannot reconstruct, so it declares
// REQUIRES_MEMORY_SNAPSHOT and may only be placed on memory-capable compute.
var counterDescriptor = api.Descriptor{
	ID:           "counter",
	Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot},
}

// TestMemorySnapshotSuspendResume is the stateful tier on real substrate, and the differentiator a
// plain pod structurally cannot provide: drive the counter to N (count lives in guest RAM, never in
// the journal), suspend to a memory snapshot, then continue and require N+1.
//
// N+1 is only reachable if the RAM came back. 1 would mean a cold boot; 2N+1 would mean the journal
// was replayed into restored RAM (double-application, the I4 violation).
func TestMemorySnapshotSuspendResume(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-memory"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	backend := f.backend(t, counterTemplateSpec, counterDescriptor)
	if !controller.CanPlace(counterDescriptor.Capabilities, backend.Capabilities()) {
		t.Fatal("substrate refused a REQUIRES_MEMORY_SNAPSHOT harness (CanPlace=false)")
	}

	session := uniqueUID("memory")
	log := journal(t).Session(session)
	p := placement.New(backend, echoagent.Model)
	defer stopQuietly(t, backend, session)

	const turns = 3
	driveTurns(ctx, t, p, log, session, turns)
	if got, want := lastOutput(t, log), strconv.Itoa(turns); got != want {
		t.Fatalf("pre-suspend count=%s want %s", got, want)
	}
	t.Logf("drove %d turns; count=%s lives in guest RAM", turns, lastOutput(t, log))

	ref, err := p.Suspend(ctx, log, session)
	if err != nil {
		t.Fatalf("session suspend: %v", err)
	}
	t.Logf("suspended: memory snapshot at %s", ref.ExternalURI)

	if err := p.Resume(ctx, log, session); err != nil {
		t.Fatalf("session resume: %v", err)
	}
	// One more turn after explicit restore: the counter must continue where its RAM left off.
	driveTurns(ctx, t, p, log, session, 1)

	got := lastOutput(t, log)
	if want := strconv.Itoa(turns + 1); got != want {
		if got == strconv.Itoa(2*turns+1) {
			t.Fatalf("double-application: count=%s means the journal was replayed into restored RAM", got)
		}
		t.Fatalf("continuity broken: post-restore count=%s want %s (the in-RAM count did not survive the snapshot)", got, want)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("chain verify across the snapshot boundary: %v", err)
	}
	t.Logf("continuity=%s (N+1), no double-apply, chain verified across suspend", got)
}

// TestMemorySessionWhoseActorIsLostIsRefused deletes a stateful session's actor after a committed
// turn, the way an operator cleanup or a host restarted against another atespace loses it. The next
// turn must be refused with api.ErrMemoryStateLost, and no actor may be created for the session: a
// fresh one from the template would answer 1 again, continuing the journal with a counter that forgot it.
func TestMemorySessionWhoseActorIsLostIsRefused(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-memory"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	backend := f.backend(t, counterTemplateSpec, counterDescriptor)
	session := uniqueUID("memory-lost")
	log := journal(t).Session(session)
	p := placement.New(backend, echoagent.Model)
	defer stopQuietly(t, backend, session)

	driveTurns(ctx, t, p, log, session, 1)
	if err := backend.Stop(ctx, api.Incarnation{ID: session}); err != nil {
		t.Fatalf("delete the session's actor: %v", err)
	}
	ctl := ateadapter.New(f.conn)
	actor := substrate.ActorRef{Atespace: f.atespace, Name: session}
	waitActorGone(ctx, t, ctl, actor)
	head, err := log.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}

	_, err = p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "inc")}, head)
	if !errors.Is(err, api.ErrMemoryStateLost) {
		t.Fatalf("turn after the actor was lost: err=%v, want api.ErrMemoryStateLost", err)
	}
	if !strings.Contains(err.Error(), session) {
		t.Errorf("error %q does not name the session", err)
	}
	if _, err := ctl.GetActor(ctx, actor); !errors.Is(err, substrate.ErrActorNotFound) {
		t.Fatalf("after the refused turn, GetActor(%s) err=%v, want ErrActorNotFound: no actor may be created", session, err)
	}
	if got, err := log.Head(); err != nil || got != head {
		t.Fatalf("journal head after the refused turn = %d (err %v), want %d", got, err, head)
	}
	t.Logf("refused as expected, nothing created: %v", err)
}

// waitActorGone polls until substrate reports the actor absent, since a delete may finish after the
// call returns.
func waitActorGone(ctx context.Context, t *testing.T, ctl substrate.ControlClient, actor substrate.ActorRef) {
	t.Helper()
	for {
		_, err := ctl.GetActor(ctx, actor)
		if errors.Is(err, substrate.ErrActorNotFound) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actor %s still present after delete (last err %v)", actor.Name, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// driveTurns runs n turns through the Placer, each at the log's current head.
func driveTurns(ctx context.Context, t *testing.T, p *placement.Placer, log *sqlitelog.Log, session string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		head, err := log.Head()
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if _, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "inc")}, head); err != nil {
			t.Fatalf("drive turn %d of %d: %v", i+1, n, err)
		}
	}
}
