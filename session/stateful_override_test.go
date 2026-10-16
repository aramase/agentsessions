package session_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// The memory-tier override rule: an override is FAILED_PRECONDITION when either harness is
// REQUIRES_MEMORY_SNAPSHOT. Suspend and Fork route by the session's own harness, so a turn an
// ExecRequest.harness override ran on a memory-tier harness would leave state in an actor that
// neither checkpoints nor clones; and a memory-tier session's actor would miss a turn run
// elsewhere. Either override is refused before any actor is created, and nothing is journaled. An
// override onto an alias of the session's own memory harness is not an override: they share the
// Placer. A session on the memory harness suspends and forks through its actor. The stateless
// override of a memory-tier session is pinned in approval_override_test.go.
func TestOverrideOntoAMemoryHarnessIsRefused(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	desc, err := echoagent.Harness{}.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	desc.ID = "mem"
	desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
	backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "mem"}, desc)
	dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
		return echoagent.Harness{}, func() error { return nil }, nil
	})
	echo := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = echo.Close() })
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	mem := placement.New(backend, echoagent.Model, dial)
	desc2 := desc
	desc2.ID = "mem2"
	backend2 := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "mem2"}, desc2)
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo":      placement.New(echo, echoagent.Model),
		"mem":       mem,
		"mem-alias": mem,
		"mem2":      placement.New(backend2, echoagent.Model, dial),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := serveRegistry(t, store, r)
	ctx := context.Background()
	hasActor := func(sess string) bool {
		ctl.mu.Lock()
		defer ctl.mu.Unlock()
		_, ok := ctl.status[sess]
		return ok
	}

	t.Run("echo session refuses a memory override", func(t *testing.T) {
		sess := mustCreate(t, c)
		if err := execHi(c, sess); err != nil {
			t.Fatalf("default turn: %v", err)
		}
		head, err := store.Session(sess).Head()
		if err != nil {
			t.Fatal(err)
		}
		err = execOn(ctx, c, sess, "mem")
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("override onto the memory harness: want FailedPrecondition, got %v", err)
		}
		if msg := status.Convert(err).Message(); !strings.Contains(msg, "cannot involve a REQUIRES_MEMORY_SNAPSHOT harness") {
			t.Errorf("error %q is not the stateful-override refusal", msg)
		}
		if hasActor(sess) {
			t.Error("the refused override created an actor")
		}
		if got, err := store.Session(sess).Head(); err != nil || got != head {
			t.Errorf("journal head after the refused override = %d (err %v), want %d", got, err, head)
		}
		// The session is unchanged: its own harness still suspends and forks.
		if err := suspendSession(c, sess); err != nil {
			t.Fatalf("Suspend after the refused override: %v", err)
		}
		if err := forkParent(c, sess); err != nil {
			t.Fatalf("Fork after the refused override: %v", err)
		}
	})

	t.Run("echo session refuses to resume a memory turn", func(t *testing.T) {
		// A pending turn recorded on the memory harness, as a host without this rule would have
		// journaled it.
		sess := mustCreate(t, c)
		log := store.Session(sess)
		seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "mem"}, false)
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Resume(ctx, &v1.ResumeRequest{Session: sess})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Resume of a memory turn on an echo session: want FailedPrecondition, got %v", err)
		}
		if msg := status.Convert(err).Message(); !strings.Contains(msg, "cannot involve a REQUIRES_MEMORY_SNAPSHOT harness") {
			t.Errorf("error %q is not the stateful-override refusal", msg)
		}
		if hasActor(sess) {
			t.Error("the refused Resume created an actor")
		}
		if got, err := log.Head(); err != nil || got != head {
			t.Errorf("journal head after the refused Resume = %d (err %v), want %d", got, err, head)
		}
	})

	t.Run("memory session refuses another memory harness", func(t *testing.T) {
		sess, err := createOn(ctx, c, "mem")
		if err != nil {
			t.Fatal(err)
		}
		if err := execHi(c, sess); err != nil {
			t.Fatalf("default turn: %v", err)
		}
		log := store.Session(sess)
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		ctl.mu.Lock()
		creates, suspends := ctl.created, ctl.suspends
		ctl.mu.Unlock()
		err = execOn(ctx, c, sess, "mem2")
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("memory override of a memory session: want FailedPrecondition, got %v", err)
		}
		msg := status.Convert(err).Message()
		for _, want := range []string{"cannot involve a REQUIRES_MEMORY_SNAPSHOT harness", `on harness "mem"`} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q does not contain %q", msg, want)
			}
		}
		ctl.mu.Lock()
		gotCreates, gotSuspends := ctl.created, ctl.suspends
		ctl.mu.Unlock()
		if gotCreates != creates || gotSuspends != suspends {
			t.Errorf("the refused override changed compute: creates %d -> %d, suspends %d -> %d", creates, gotCreates, suspends, gotSuspends)
		}
		if got, err := log.Head(); err != nil || got != head {
			t.Errorf("journal head after the refused override = %d (err %v), want %d", got, err, head)
		}
	})

	t.Run("memory session runs an alias override", func(t *testing.T) {
		sess, err := createOn(ctx, c, "mem")
		if err != nil {
			t.Fatal(err)
		}
		if err := execOn(ctx, c, sess, "mem-alias"); err != nil {
			t.Fatalf("override onto an alias of the session's harness: %v", err)
		}
		if !hasActor(sess) {
			t.Fatal("the alias turn did not run on the session's actor")
		}
	})

	t.Run("memory session suspends through its actor", func(t *testing.T) {
		sess, err := createOn(ctx, c, "mem")
		if err != nil {
			t.Fatal(err)
		}
		if err := execHi(c, sess); err != nil {
			t.Fatalf("turn: %v", err)
		}
		ctl.mu.Lock()
		before := ctl.suspends
		ctl.mu.Unlock()
		if err := suspendSession(c, sess); err != nil {
			t.Fatalf("Suspend: %v", err)
		}
		ctl.mu.Lock()
		after, st := ctl.suspends, ctl.status[sess]
		ctl.mu.Unlock()
		if after != before+1 || st != substrate.StatusSuspended {
			t.Fatalf("Suspend did not snapshot the actor: suspends %d -> %d, status %v", before, after, st)
		}
	})

	t.Run("memory session forks by cloning its actor", func(t *testing.T) {
		sess, err := createOn(ctx, c, "mem")
		if err != nil {
			t.Fatal(err)
		}
		if err := execHi(c, sess); err != nil {
			t.Fatalf("turn: %v", err)
		}
		resp, err := c.Fork(ctx, &v1.ForkRequest{Session: sess})
		if err != nil {
			t.Fatalf("Fork: %v", err)
		}
		if len(resp.GetChildren()) != 1 {
			t.Fatalf("Fork returned %d children, want 1", len(resp.GetChildren()))
		}
		child := resp.GetChildren()[0].GetMetadata().GetUid()
		if !hasActor(child) {
			t.Fatal("Fork did not clone an actor for the child")
		}
		if err := execHi(c, child); err != nil {
			t.Fatalf("first turn on the child: %v", err)
		}
	})
}
