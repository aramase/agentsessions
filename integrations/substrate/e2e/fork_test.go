package e2e

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

// TestForkFanOutFromMemorySnapshot is the claim "logical branching of a fleet of actors from a base
// session", made against real substrate rather than a control-client double.
//
// Drive the counter to N so N exists ONLY in guest RAM, fan out to k children in one call, then give
// each child a turn. Every child must answer N+1:
//
//   - N+1 from EVERY child means each one really restored the parent's RAM. A child that cold-booted
//     would answer 1, which is exactly the silent divergence this feature exists to prevent.
//   - N+1 from every child rather than N+1, N+2, N+3… means the children are INDEPENDENT. Shared
//     state would make the counts climb in the order the children were driven.
//
// The parent is then resumed and must also continue at N+1: a fork checkpoints its parent, it does
// not consume it.
func TestForkFanOutFromMemorySnapshot(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-fork"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	backend := f.backend(counterTemplate, counterDescriptor)
	store := journal(t)
	parent := uniqueUID("fork-parent")
	parentLog := store.Session(parent)
	p := placement.New(backend, echoagent.Model)
	defer stopQuietly(t, backend, parent)

	const (
		turns = 3 // N: the count that exists only in the parent's RAM
		// The fan-out width. The parent stays live across the fork, so this needs
		// children+1 free micro-VM workers; deploy/substrate/counter-microvm-actortemplate.yaml
		// sizes its pool for this number. Raising it without raising replicas there fails
		// the fork with "no free workers available".
		children = 3
	)

	driveTurns(ctx, t, p, parentLog, parent, turns)
	if got, want := lastOutput(t, parentLog), strconv.Itoa(turns); got != want {
		t.Fatalf("pre-fork count=%s want %s", got, want)
	}
	atSeq, err := parentLog.Head()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("parent %s at count=%d, head=%d", parent, turns, atSeq)

	kids := make([]placement.ForkChild, 0, children)
	for i := 0; i < children; i++ {
		uid := fmt.Sprintf("%s-child-%d", parent, i)
		kids = append(kids, placement.ForkChild{UID: uid, Log: store.Session(uid)})
		defer stopQuietly(t, backend, uid)
	}

	// One call, one parent checkpoint, k clones.
	start := time.Now()
	if err := p.Fork(ctx, parentLog, parent, kids, atSeq); err != nil {
		t.Fatalf("fork %d children at seq %d: %v", children, atSeq, err)
	}
	elapsed := time.Since(start)
	// Cost, measured rather than assumed: substrate restores each child into a private per-actor
	// directory with no node-local snapshot cache, so this is k full restores, not k references to
	// one image (RuntimeCapabilities.CoWFork stays false).
	t.Logf("forked %d children in %s (%s per child, one shared parent checkpoint)",
		children, elapsed.Round(time.Millisecond), (elapsed / children).Round(time.Millisecond))

	want := strconv.Itoa(turns + 1)
	for i, kid := range kids {
		log := store.Session(kid.UID)

		// The child inherited the parent's prefix plus a FORK marker, and the inherited chain must
		// still verify: a branch is a branch of the same hash tree, not a fresh log.
		if err := log.Verify(); err != nil {
			t.Fatalf("child %d (%s) chain verify: %v", i, kid.UID, err)
		}
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		if head <= atSeq {
			t.Fatalf("child %d head=%d, want > parent fork seq %d (inherited prefix + FORK marker)", i, head, atSeq)
		}

		driveTurns(ctx, t, p, log, kid.UID, 1)

		switch got := lastOutput(t, log); got {
		case want: // N+1: this child restored the parent's RAM and continued from it
		case "1":
			t.Fatalf("child %d (%s) answered 1: it cold-booted instead of restoring the parent's RAM, "+
				"which is the silent divergence a stateful fork must never produce", i, kid.UID)
		default:
			t.Fatalf("child %d (%s) answered %s, want %s: children must branch from identical state "+
				"and evolve independently, not share a counter", i, kid.UID, got, want)
		}
		if err := log.Verify(); err != nil {
			t.Fatalf("child %d (%s) chain verify after its turn: %v", i, kid.UID, err)
		}
	}
	t.Logf("all %d children continued at %s independently", children, want)

	// The parent was checkpointed, not consumed: resume it and it continues from the same base.
	if err := p.Resume(ctx, parentLog, parent); err != nil {
		t.Fatalf("resume the forked parent: %v", err)
	}
	driveTurns(ctx, t, p, parentLog, parent, 1)
	if got := lastOutput(t, parentLog); got != want {
		t.Fatalf("parent continued at %s, want %s: a fork must checkpoint its parent, not consume it", got, want)
	}
	if err := parentLog.Verify(); err != nil {
		t.Fatalf("parent chain verify across the fork: %v", err)
	}
	t.Logf("parent resumed and continued at %s; chain verified across the fork", want)
}

// TestForkOfMemoryHarnessAtHistoricalSeqIsRefused pins the honest-degradation half on real compute:
// a memory clone reflects the parent's RAM *now*, so pairing it with an older log prefix would be a
// child whose journal and RAM disagree. That is refused, not approximated.
func TestForkOfMemoryHarnessAtHistoricalSeqIsRefused(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-fork"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	backend := f.backend(counterTemplate, counterDescriptor)
	store := journal(t)
	parent := uniqueUID("fork-historical")
	parentLog := store.Session(parent)
	p := placement.New(backend, echoagent.Model)
	defer stopQuietly(t, backend, parent)

	driveTurns(ctx, t, p, parentLog, parent, 2)
	head, err := parentLog.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head < 2 {
		t.Fatalf("head=%d, need a history to fork behind", head)
	}

	child := parent + "-child"
	err = p.Fork(ctx, parentLog, parent, []placement.ForkChild{{UID: child, Log: store.Session(child)}}, head-1)
	if err == nil {
		stopQuietly(t, backend, child)
		t.Fatal("forking a REQUIRES_MEMORY_SNAPSHOT session at a historical seq must be refused")
	}
	t.Logf("refused as expected: %v", err)

	// A refusal must leave the parent usable, not half-checkpointed.
	driveTurns(ctx, t, p, parentLog, parent, 1)
	if got, want := lastOutput(t, parentLog), "3"; got != want {
		t.Fatalf("parent count=%s after a refused fork, want %s", got, want)
	}
}
