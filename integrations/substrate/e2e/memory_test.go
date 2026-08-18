package e2e

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
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

	backend := f.backend(counterTemplate, counterDescriptor)
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

	// Suspend through the Runtime SPI rather than Placer.Suspend, which also Stops (deletes) the
	// actor — correct for a stateless session, fatal for one whose RAM we intend to restore.
	ref, err := backend.Snapshot(ctx, api.Incarnation{ID: session}, api.SnapshotExternal)
	if err != nil {
		t.Fatalf("suspend (snapshot): %v", err)
	}
	if err := recordSuspend(log, ref); err != nil {
		t.Fatalf("record SUSPEND event: %v", err)
	}
	t.Logf("suspended: memory snapshot at %s", ref.ExternalURI)

	// One more turn. The actor is SUSPENDED, so placing it restores the snapshot instead of booting
	// over it, and the counter continues where its RAM left off.
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

// recordSuspend appends a SUSPEND lifecycle event carrying the snapshot ref, so the hash chain — and
// therefore provenance — spans the suspend/restore boundary.
func recordSuspend(log *sqlitelog.Log, ref api.SnapshotRef) error {
	head, err := log.Head()
	if err != nil {
		return err
	}
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	_, err = log.Append(head, fence, api.Event{
		Kind:      api.EventLifecycle,
		Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref},
	})
	return err
}
