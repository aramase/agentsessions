package placement_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

// impostor is a STATELESS_REPLAY harness that reports a descriptor id other than echo's, so only
// the identity check can refuse it.
var impostor = api.Descriptor{ID: "impostor", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}

// A Placer built WithDescriptorID refuses Exec, Resume and Fork on a harness that reports another
// id, before Restore and before anything is written to the log, and runs them again once the
// expected harness answers.
func TestDescriptorIDGatesExecResumeAndFork(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	inner := local.New(failOnceHarness{failed: &atomic.Bool{}})
	t.Cleanup(func() { _ = inner.Close() })
	backend := &describeOverride{Backend: inner}
	p := placement.New(backend, echoagent.Model, placement.WithDescriptorID("echo"))
	if got := p.DescriptorID(); got != "echo" {
		t.Fatalf("DescriptorID = %q, want echo", got)
	}

	if _, err := p.Exec(ctx, log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err == nil || errors.Is(err, placement.ErrDescriptorMismatch) {
		t.Fatalf("the first turn should have run on echo and been interrupted, got %v", err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head == 0 {
		t.Fatal("the first turn wrote nothing")
	}

	backend.desc = &impostor
	if err := p.Resume(ctx, log, "s"); !errors.Is(err, placement.ErrDescriptorMismatch) {
		t.Fatalf("resume on a harness reporting another id: got %v, want ErrDescriptorMismatch", err)
	}
	if n := backend.restores.Load(); n != 0 {
		t.Fatalf("a refused resume restored compute %d time(s)", n)
	}
	if _, err := p.Exec(ctx, log, "s", []api.Message{*api.TextMessage("user", "again")}, head); !errors.Is(err, placement.ErrDescriptorMismatch) {
		t.Fatalf("exec on a harness reporting another id: got %v, want ErrDescriptorMismatch", err)
	}
	child := store.Session("child")
	if err := p.Fork(ctx, log, "s", []placement.ForkChild{{UID: "child", Log: child}}, head); !errors.Is(err, placement.ErrDescriptorMismatch) {
		t.Fatalf("fork of a harness reporting another id: got %v, want ErrDescriptorMismatch", err)
	}
	if got, _ := log.Head(); got != head {
		t.Fatalf("a refused call wrote to the log: head %d -> %d", head, got)
	}
	if got, _ := child.Head(); got != 0 {
		t.Fatalf("a refused fork wrote %d event(s) to the child", got)
	}

	// The refusals were the identity check: the same session resumes once echo answers again.
	backend.desc = nil
	if err := p.Resume(ctx, log, "s"); err != nil {
		t.Fatalf("resume on the expected harness: %v", err)
	}
	if got, _ := log.Head(); got <= head {
		t.Fatal("resume did not re-drive the interrupted turn")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// Without WithDescriptorID the id is not checked: a harness reporting any id runs the turn.
func TestDescriptorIDEmptyIsNotChecked(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	p := placement.New(&describeOverride{Backend: inner, desc: &impostor}, echoagent.Model)
	if got := p.DescriptorID(); got != "" {
		t.Fatalf("DescriptorID = %q, want empty", got)
	}
	if _, err := p.Exec(context.Background(), store.Session("s"), "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatalf("exec with no expected id: %v", err)
	}
}

// idHarness is echo reporting another descriptor id.
type idHarness struct {
	echoagent.Harness
	id string
}

func (h idHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	d, err := h.Harness.Describe(ctx)
	d.ID = h.id
	return d, err
}

// For a live-describing backend the id is checked on the turn's own connection too: the backend's
// Describe reaches echo, the connection that would run the turn reaches a harness reporting another
// id, and Exec is refused before anything is written to the log.
func TestDescriptorIDCheckedOnTheTurnsConnection(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	dial := placement.WithDialer(func(string) (api.Harness, func() error, error) {
		return idHarness{id: "impostor"}, func() error { return nil }, nil
	})
	p := placement.New(liveLocal{inner}, echoagent.Model, dial, placement.WithDescriptorID("echo"))
	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); !errors.Is(err, placement.ErrDescriptorMismatch) {
		t.Fatalf("exec where the turn's connection reaches another harness: got %v, want ErrDescriptorMismatch", err)
	}
	if head, _ := log.Head(); head != 0 {
		t.Fatalf("a refused exec wrote to the log: head %d", head)
	}
}
