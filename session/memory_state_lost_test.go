package session_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
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

// A session whose actor disappeared after a turn (deleted out of band, or a host restarted
// against another atespace) has no compute to attach to and nothing to restore. For a
// REQUIRES_MEMORY_SNAPSHOT harness the next Exec must be refused with FAILED_PRECONDITION, naming the
// session, and must not create an actor: a fresh one would continue the journal without the state the
// recorded turns built. A STATELESS_REPLAY harness rebuilds from the journal, so it keeps working.
func TestExecAfterTheActorIsLost(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tier       api.Resumability
		wantRefuse bool
	}{
		{name: "memory tier is refused", tier: api.ResumabilityRequiresMemorySnapshot, wantRefuse: true},
		{name: "stateless tier creates a new actor", tier: api.ResumabilityStatelessReplay},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			desc, err := echoagent.Harness{}.Describe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			desc.Capabilities.Resumability = tc.tier
			backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "echo"}, desc)
			dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return echoagent.Harness{}, func() error { return nil }, nil
			})
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(backend, echoagent.Model, dial)})
			if err != nil {
				t.Fatal(err)
			}
			c := serveRegistry(t, store, r)
			sess := mustCreate(t, c)

			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			if err := ctl.DeleteActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: sess}); err != nil {
				t.Fatal(err)
			}
			head, err := store.Session(sess).Head()
			if err != nil {
				t.Fatal(err)
			}

			err = execHi(c, sess)
			ctl.mu.Lock()
			_, recreated := ctl.status[sess]
			ctl.mu.Unlock()
			if !tc.wantRefuse {
				if err != nil || !recreated {
					t.Fatalf("second turn after the actor was lost: err=%v recreated=%v, want a new actor and success", err, recreated)
				}
				return
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("second turn after the actor was lost: want FailedPrecondition, got %v", err)
			}
			msg := status.Convert(err).Message()
			for _, want := range []string{sess, "cannot be reconstructed"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not contain %q", msg, want)
				}
			}
			if recreated {
				t.Error("the refused turn created an actor for the session")
			}
			if got, err := store.Session(sess).Head(); err != nil || got != head {
				t.Errorf("journal head after the refused turn = %d (err %v), want %d: nothing may be journaled", got, err, head)
			}
		})
	}
}

// The memory-tier guard counts only the turns its own harness ran. A turn ExecRequest.harness sent to
// another harness left nothing in the memory harness's actor, so a default turn after it creates the
// actor as on a new session. Once the memory harness has run a turn, losing its actor is refused
// however many override turns ran in between. The memory-tier override rule now refuses such an
// override, so the override turns are journaled here as a host without that rule wrote them.
func TestMemoryStateLostCountsOnlyTheMemoryHarnessTurns(t *testing.T) {
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
	r, err := placement.NewRegistry("mem", map[string]*placement.Placer{
		"mem":  placement.New(backend, echoagent.Model, dial),
		"echo": placement.New(echo, echoagent.Model),
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
	loseActor := func(sess string) {
		t.Helper()
		if err := ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: sess}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("override turn then default turn creates the actor", func(t *testing.T) {
		sess := mustCreate(t, c)
		if err := execOn(ctx, c, sess, "echo"); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("override turn: want FailedPrecondition, got %v", err)
		}
		journalOverrideTurn(t, store, sess, "echo")
		if hasActor(sess) {
			t.Fatal("the override turn created a substrate actor")
		}
		if err := execHi(c, sess); err != nil {
			t.Fatalf("default turn after an override turn: %v", err)
		}
		if !hasActor(sess) {
			t.Fatal("the default turn did not create the memory harness's actor")
		}
	})

	t.Run("default, override, lost actor, default is refused", func(t *testing.T) {
		sess := mustCreate(t, c)
		if err := execHi(c, sess); err != nil {
			t.Fatalf("first default turn: %v", err)
		}
		journalOverrideTurn(t, store, sess, "echo")
		loseActor(sess)
		err := execHi(c, sess)
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("default turn after the actor was lost: want FailedPrecondition, got %v", err)
		}
		msg := status.Convert(err).Message()
		for _, want := range []string{sess, "1 started turn(s) on harness \"mem\"", "cannot be reconstructed"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q does not contain %q", msg, want)
			}
		}
		if hasActor(sess) {
			t.Error("the refused turn created an actor for the session")
		}
	})
}

// journalOverrideTurn appends a completed turn recorded on harness, as a host without the
// memory-tier override rule journaled an ExecRequest.harness override.
func journalOverrideTurn(t *testing.T, store *sqlitelog.Store, sess, harness string) {
	t.Helper()
	log := store.Session(sess)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("override-%d", head)
	start := api.Event{Kind: api.EventExecutionStart, ExecutionID: id, ExecutionStart: &api.ExecutionStart{
		Harness: harness, InputCount: proto.Int64(0),
	}}
	if _, err := log.Append(head, fence, start); err != nil {
		t.Fatal(err)
	}
	end := api.Event{Kind: api.EventEnd, ExecutionID: id, End: &api.HarnessEnd{State: "COMPLETED"}}
	if _, err := log.Append(head+1, fence, end); err != nil {
		t.Fatal(err)
	}
}

// Resume of a suspended session restores its actor's own snapshot. When the actor is gone, a
// REQUIRES_MEMORY_SNAPSHOT session's state went with it: Resume is refused with FAILED_PRECONDITION,
// naming the session, creates no actor and journals nothing. A STATELESS_REPLAY session keeps the
// INTERNAL resume error it had before; this rule is only about RAM state.
func TestResumeAfterTheActorIsLost(t *testing.T) {
	for _, tc := range []struct {
		name string
		tier api.Resumability
		want codes.Code
	}{
		{name: "memory tier is refused", tier: api.ResumabilityRequiresMemorySnapshot, want: codes.FailedPrecondition},
		{name: "stateless tier is unchanged", tier: api.ResumabilityStatelessReplay, want: codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			desc, err := echoagent.Harness{}.Describe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			desc.Capabilities.Resumability = tc.tier
			backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "echo"}, desc)
			dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return echoagent.Harness{}, func() error { return nil }, nil
			})
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(backend, echoagent.Model, dial)})
			if err != nil {
				t.Fatal(err)
			}
			c := serveRegistry(t, store, r)
			ctx := context.Background()
			sess := mustCreate(t, c)
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
				t.Fatalf("suspend: %v", err)
			}
			if err := ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: sess}); err != nil {
				t.Fatal(err)
			}
			head, err := store.Session(sess).Head()
			if err != nil {
				t.Fatal(err)
			}

			_, err = c.Resume(ctx, &v1.ResumeRequest{Session: sess})
			if status.Code(err) != tc.want {
				t.Fatalf("resume after the actor was lost: want %v, got %v", tc.want, err)
			}
			ctl.mu.Lock()
			_, recreated := ctl.status[sess]
			ctl.mu.Unlock()
			if recreated {
				t.Error("the failed resume created an actor for the session")
			}
			if got, err := store.Session(sess).Head(); err != nil || got != head {
				t.Errorf("journal head after the failed resume = %d (err %v), want %d: nothing may be journaled", got, err, head)
			}
			if tc.want != codes.FailedPrecondition {
				return
			}
			msg := status.Convert(err).Message()
			for _, want := range []string{sess, "cannot be restored"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not contain %q", msg, want)
				}
			}
		})
	}
}

// failingHarness is the echo harness with a turn that fails after it has started, the way a harness
// fails part way through, having possibly changed its in-memory state.
type failingHarness struct{ echoagent.Harness }

var errTurnFailed = errors.New("turn failed part way")

func (failingHarness) Run(context.Context, *api.Start, api.EventSink) error { return errTurnFailed }

// newMemoryHost serves one REQUIRES_MEMORY_SNAPSHOT substrate harness, "mem", on ctl and store, as one
// agentsessionsd host would; the harness each turn reaches is har.
func newMemoryHost(t *testing.T, ctl *actorControl, store *sqlitelog.Store, har api.Harness) v1.SessionsClient {
	t.Helper()
	desc, err := echoagent.Harness{}.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	desc.ID = "mem"
	desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
	backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "mem"}, desc)
	dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
		return har, func() error { return nil }, nil
	})
	r, err := placement.NewRegistry("mem", map[string]*placement.Placer{"mem": placement.New(backend, echoagent.Model, dial)})
	if err != nil {
		t.Fatal(err)
	}
	return serveRegistry(t, store, r)
}

// A turn that started and then failed may have changed the harness's RAM before it failed, so it
// counts as much as a completed one: when the only turn of a memory-tier session ended in ERROR and
// its actor is lost, the next Exec is refused with FAILED_PRECONDITION, creates no actor and journals
// nothing, rather than continue the journal (which holds that turn's input) on a fresh actor.
func TestExecAfterAFailedTurnAndALostActorIsRefused(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := newMemoryHost(t, ctl, store, failingHarness{})
	sess := mustCreate(t, c)
	if err := execHi(c, sess); err == nil {
		t.Fatal("the first turn succeeded; want it to fail")
	}
	if err := ctl.DeleteActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: sess}); err != nil {
		t.Fatal(err)
	}
	head, err := store.Session(sess).Head()
	if err != nil {
		t.Fatal(err)
	}

	err = execHi(c, sess)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("turn after a failed turn and a lost actor: want FailedPrecondition, got %v", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "1 started turn(s)") {
		t.Errorf("error %q does not report the failed turn", msg)
	}
	ctl.mu.Lock()
	_, recreated := ctl.status[sess]
	ctl.mu.Unlock()
	if recreated {
		t.Error("the refused turn created an actor for the session")
	}
	if got, err := store.Session(sess).Head(); err != nil || got != head {
		t.Errorf("journal head after the refused turn = %d (err %v), want %d: nothing may be journaled", got, err, head)
	}
}

// Two hosts share the journal and the atespace. Host A starts the session's first turn and, before
// its Create looks for the actor, host B runs the first turn on a new actor, which is then deleted.
// A finds no actor. Had A counted the session's turns before looking (0), it would create a fresh
// actor, and once its stale append failed the next turn would attach to that actor and continue the
// journal without B's turn's state. The count is taken after GetActor reports the actor missing, so
// A sees B's turn and refuses with FAILED_PRECONDITION, creating nothing.
func TestTurnRecordedByAnotherHostBeforeTheActorIsLostIsCounted(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hostA := newMemoryHost(t, ctl, store, echoagent.Harness{})
	hostB := newMemoryHost(t, ctl, store, echoagent.Harness{})
	sess := mustCreate(t, hostA)

	var fired atomic.Bool
	var hostBErr error
	ctl.onGet = func(actor string) {
		// Only host A's first look runs this; host B's own GetActor calls pass straight through.
		if actor != sess || !fired.CompareAndSwap(false, true) {
			return
		}
		// Host A is inside Create, about to look for the actor. Host B runs the first turn, then
		// the actor is lost.
		hostBErr = errors.Join(execHi(hostB, sess),
			ctl.DeleteActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: sess}))
	}
	err = execHi(hostA, sess)
	ctl.onGet = nil
	if hostBErr != nil {
		t.Fatalf("host B's turn: %v", hostBErr)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("host A's turn after host B's turn and a lost actor: want FailedPrecondition, got %v", err)
	}
	ctl.mu.Lock()
	_, recreated := ctl.status[sess]
	ctl.mu.Unlock()
	if recreated {
		t.Error("host A created an actor over the state host B's turn built")
	}
}

// completedTurns counts the END events in a session's journal.
func completedTurns(t *testing.T, store *sqlitelog.Store, sess string) int {
	t.Helper()
	recs, err := store.Session(sess).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range recs {
		if r.Event.Kind == api.EventEnd {
			n++
		}
	}
	return n
}

// The interleaving the started-turn count cannot see. Host A's Create finds no actor and counts no
// started turn. Before A's CreateActor, host B creates the actor, journals and runs the session's
// first turn, and that actor is deleted out of band. A's CreateActor then succeeds, so an actor holding
// none of B's state now sits under the session's name. Nothing deletes it, and every later Create
// attaches to it without counting. B's turn recorded its actor's UID in EXECUTION_START, so A's turn and
// every later one, on either host, is refused with FAILED_PRECONDITION and journals nothing.
func TestActorCreatedAfterTheCountOverALostActorIsRefused(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hostA := newMemoryHost(t, ctl, store, echoagent.Harness{})
	hostB := newMemoryHost(t, ctl, store, echoagent.Harness{})
	sess := mustCreate(t, hostA)

	var fired atomic.Bool
	var hostBErr error
	ctl.onCreate = func(actor string) {
		// Only host A's CreateActor runs this: A has looked for the actor and counted no started turn.
		if actor != sess || !fired.CompareAndSwap(false, true) {
			return
		}
		hostBErr = errors.Join(execHi(hostB, sess),
			ctl.DeleteActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: sess}))
	}
	errA := execHi(hostA, sess)
	ctl.onCreate = nil
	if hostBErr != nil {
		t.Fatalf("host B's first turn: %v", hostBErr)
	}
	if !fired.Load() {
		t.Fatal("host A never reached CreateActor")
	}
	ctl.mu.Lock()
	_, replaced := ctl.status[sess]
	ctl.mu.Unlock()
	if !replaced {
		t.Fatal("host A did not create the replacement actor this test is about")
	}
	if status.Code(errA) != codes.FailedPrecondition {
		t.Fatalf("host A's turn on the replacement actor: want FailedPrecondition, got %v", errA)
	}
	head, err := store.Session(sess).Head()
	if err != nil {
		t.Fatal(err)
	}
	for name, host := range map[string]v1.SessionsClient{"host A": hostA, "host B": hostB} {
		err := execHi(host, sess)
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("%s's next turn on the replacement actor: want FailedPrecondition, got %v", name, err)
		}
		if msg := status.Convert(err).Message(); !strings.Contains(msg, sess) || !strings.Contains(msg, "replaced") {
			t.Errorf("%s: error %q does not name the session and the replaced instance", name, msg)
		}
	}
	if got, err := store.Session(sess).Head(); err != nil || got != head {
		t.Errorf("journal head after the refused turns = %d (err %v), want %d: nothing may be journaled", got, err, head)
	}
	if n := completedTurns(t, store, sess); n != 1 {
		t.Errorf("completed turns = %d, want only host B's", n)
	}
}

// Two hosts racing a session's first turn on a healthy actor is ordinary, and the loser must be
// able to retry: it gets OK or a retryable ABORTED, never FAILED_PRECONDITION or INTERNAL, and
// afterwards both hosts keep running turns on the one actor.
func TestTwoHostsRacingTheFirstTurnKeepTheSessionHealthy(t *testing.T) {
	runB := func(hostB v1.SessionsClient, sess string) func(*actorControl) error {
		return func(*actorControl) error { return execHi(hostB, sess) }
	}
	for _, tc := range []struct {
		name string
		// arm installs the hook through which host B acts inside host A's placement.
		arm func(ctl *actorControl, hook func(actor string))
		// during is what host B does inside that hook; after, when set, is what it does once host
		// A's turn has returned.
		during, after func(hostB v1.SessionsClient, sess string) func(*actorControl) error
		wantA         []codes.Code
	}{
		{
			// B creates the actor and finishes its turn after A found no actor and before A's
			// CreateActor, which then finds the name taken and attaches to B's actor.
			name:   "B creates the actor before A does",
			arm:    func(ctl *actorControl, hook func(string)) { ctl.onCreate = hook },
			during: runB,
			wantA:  []codes.Code{codes.OK, codes.Aborted},
		},
		{
			// A creates the actor; B finds it, resumes it and finishes its turn before A's own
			// resume returns and before A's turn is journaled.
			name:   "B runs on the actor A created",
			arm:    func(ctl *actorControl, hook func(string)) { ctl.onResume = hook },
			during: runB,
			wantA:  []codes.Code{codes.OK, codes.Aborted},
		},
		{
			// B has created the actor and is still resuming it when A's CreateActor finds the name
			// taken, so A finds the actor RESUMING. That settles on its own: A's turn is ABORTED,
			// not INTERNAL, and A's retry runs once B's resume and turn are done.
			name: "A finds B's actor mid-resume",
			arm:  func(ctl *actorControl, hook func(string)) { ctl.onCreate = hook },
			during: func(_ v1.SessionsClient, sess string) func(*actorControl) error {
				return func(ctl *actorControl) error {
					ctl.mu.Lock()
					defer ctl.mu.Unlock()
					ctl.status[sess] = substrate.StatusTransitioning
					ctl.newUID(sess)
					return nil
				}
			},
			after: func(hostB v1.SessionsClient, sess string) func(*actorControl) error {
				return func(ctl *actorControl) error {
					if _, err := ctl.ResumeActor(context.Background(), substrate.ActorRef{Atespace: "space", Name: sess}); err != nil {
						return err
					}
					return execHi(hostB, sess)
				}
			},
			wantA: []codes.Code{codes.Aborted},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			hostA := newMemoryHost(t, ctl, store, echoagent.Harness{})
			hostB := newMemoryHost(t, ctl, store, echoagent.Harness{})
			sess := mustCreate(t, hostA)

			var fired atomic.Bool
			var hostBErr error
			tc.arm(ctl, func(actor string) {
				if actor != sess || !fired.CompareAndSwap(false, true) {
					return
				}
				hostBErr = tc.during(hostB, sess)(ctl)
			})
			errA := execHi(hostA, sess)
			ctl.onCreate, ctl.onResume = nil, nil
			if !fired.Load() {
				t.Fatal("host B never acted inside host A's placement")
			}
			if hostBErr == nil && tc.after != nil {
				hostBErr = tc.after(hostB, sess)(ctl)
			}
			if hostBErr != nil {
				t.Fatalf("host B's first turn: %v", hostBErr)
			}
			if code := status.Code(errA); !slices.Contains(tc.wantA, code) {
				t.Fatalf("host A's racing first turn: want one of %v, got %v", tc.wantA, errA)
			}
			if status.Code(errA) == codes.Aborted {
				if err := execHi(hostA, sess); err != nil {
					t.Fatalf("host A's retry: %v", err)
				}
			}
			if err := execHi(hostB, sess); err != nil {
				t.Fatalf("host B's next turn: %v", err)
			}
			if n := completedTurns(t, store, sess); n != 3 {
				t.Errorf("completed turns = %d, want 3 (B's, A's and B's next)", n)
			}
			ctl.mu.Lock()
			created := ctl.created
			ctl.mu.Unlock()
			if created != 1 {
				t.Errorf("actors created = %d, want 1: both hosts must share the session's actor", created)
			}
		})
	}
}

// A Fork of a memory-tier session whose actor was replaced must not clone the replacement: every
// child would run without the parent's state, and nothing would refuse it later, since a child's
// own actor has a new UID and the FORK marker resets the comparison its turns make. After the
// arbitration's interleaving (host B's turn recorded on the lost actor, host A's replacement
// RUNNING under the name), Fork is refused with FAILED_PRECONDITION before it suspends anything,
// creates no child actor and no child session, and journals nothing on the parent.
func TestForkOfASessionWhoseActorWasReplacedIsRefused(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hostA := newMemoryHost(t, ctl, store, echoagent.Harness{})
	hostB := newMemoryHost(t, ctl, store, echoagent.Harness{})
	ctx := context.Background()
	sess := mustCreate(t, hostA)

	var fired atomic.Bool
	var hostBErr error
	ctl.onCreate = func(actor string) {
		if actor != sess || !fired.CompareAndSwap(false, true) {
			return
		}
		hostBErr = errors.Join(execHi(hostB, sess),
			ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: sess}))
	}
	errA := execHi(hostA, sess)
	ctl.onCreate = nil
	if hostBErr != nil || !fired.Load() {
		t.Fatalf("host B's first turn inside host A's create: fired=%v err=%v", fired.Load(), hostBErr)
	}
	if status.Code(errA) != codes.FailedPrecondition {
		t.Fatalf("host A's turn on the replacement actor: want FailedPrecondition, got %v", errA)
	}
	ctl.mu.Lock()
	created, suspends, replacement := ctl.created, ctl.suspends, ctl.status[sess]
	ctl.mu.Unlock()
	if replacement != substrate.StatusRunning {
		t.Fatalf("replacement actor is %v, want running: the fork must find it under the session's name", replacement)
	}
	head, err := store.Session(sess).Head()
	if err != nil {
		t.Fatal(err)
	}
	before, err := hostA.ListSessions(ctx, &v1.ListSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}

	for name, host := range map[string]v1.SessionsClient{"host A": hostA, "host B": hostB} {
		_, err = host.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 2})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("%s: fork of a session whose actor was replaced: want FailedPrecondition, got %v", name, err)
		}
		if msg := status.Convert(err).Message(); !strings.Contains(msg, "replaced") {
			t.Errorf("%s: error %q does not say the instance was replaced", name, msg)
		}
	}
	ctl.mu.Lock()
	gotCreated, gotSuspends, gotStatus := ctl.created, ctl.suspends, ctl.status[sess]
	ctl.mu.Unlock()
	if gotCreated != created {
		t.Errorf("actors created = %d after the refused forks, want %d: no child actor may be created", gotCreated, created)
	}
	if gotSuspends != suspends || gotStatus != substrate.StatusRunning {
		t.Errorf("suspends = %d, replacement %v after the refused forks, want %d and running: nothing may be snapshotted",
			gotSuspends, gotStatus, suspends)
	}
	if got, err := store.Session(sess).Head(); err != nil || got != head {
		t.Errorf("parent journal head after the refused forks = %d (err %v), want %d", got, err, head)
	}
	after, err := hostA.ListSessions(ctx, &v1.ListSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.GetSessions()) != len(before.GetSessions()) {
		t.Errorf("sessions = %d after the refused forks, want %d: no child session may be recorded",
			len(after.GetSessions()), len(before.GetSessions()))
	}
}

// Resume of a suspended memory-tier session must not hand the session to an actor created under its
// name after its own actor was lost: the snapshot it would restore went with the lost actor, and the
// replacement holds none of the state. The recorded UID differs, so Resume is refused with
// FAILED_PRECONDITION and journals nothing.
func TestResumeOntoAReplacementActorIsRefused(t *testing.T) {
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := newMemoryHost(t, ctl, store, echoagent.Harness{})
	ctx := context.Background()
	sess := mustCreate(t, c)
	if err := execHi(c, sess); err != nil {
		t.Fatalf("first turn: %v", err)
	}
	if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	ref := substrate.ActorRef{Atespace: "space", Name: sess}
	if err := ctl.DeleteActor(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := ctl.CreateActor(ctx, ref, substrate.ObjectRef{Atespace: "space", Name: "mem"}); err != nil {
		t.Fatal(err)
	}
	head, err := store.Session(sess).Head()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Resume(ctx, &v1.ResumeRequest{Session: sess})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resume onto a replacement actor: want FailedPrecondition, got %v", err)
	}
	if got, err := store.Session(sess).Head(); err != nil || got != head {
		t.Errorf("journal head after the refused resume = %d (err %v), want %d", got, err, head)
	}
}

// Suspend and a stateful Fork check the actor they snapshot against the instance the journal last
// recorded, as Exec and Resume do. When the session's actor is gone, or another one has been created
// under its name, its in-RAM state cannot be captured: both are refused with FAILED_PRECONDITION
// (not a retryable INTERNAL), snapshot nothing, create no actor and journal nothing, so no SUSPEND
// records a checkpoint without the session's state.
//
// A fork child before its first turn has no recorded instance yet; when its cloned actor is gone,
// the cloned state went with it, so it is refused the same way.
func TestSuspendAndForkOfALostOrReplacedActorAreRefused(t *testing.T) {
	ctx := context.Background()
	for _, actor := range []string{"gone", "replaced", "child-gone"} {
		for _, op := range []string{"suspend", "fork"} {
			t.Run(actor+"/"+op, func(t *testing.T) {
				ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
				store, err := sqlitelog.Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				c := newMemoryHost(t, ctl, store, echoagent.Harness{})
				sess := mustCreate(t, c)
				if err := execHi(c, sess); err != nil {
					t.Fatalf("first turn: %v", err)
				}
				if actor == "child-gone" {
					resp, err := c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
					if err != nil {
						t.Fatalf("fork: %v", err)
					}
					sess = resp.GetChildren()[0].GetMetadata().GetUid()
				}
				ref := substrate.ActorRef{Atespace: "space", Name: sess}
				if err := ctl.DeleteActor(ctx, ref); err != nil {
					t.Fatal(err)
				}
				wantMsg := "no longer exists"
				if actor == "replaced" {
					if err := ctl.CreateActor(ctx, ref, substrate.ObjectRef{Atespace: "space", Name: "mem"}); err != nil {
						t.Fatal(err)
					}
					wantMsg = "replaced"
				}
				ctl.mu.Lock()
				created, suspends := ctl.created, ctl.suspends
				ctl.mu.Unlock()
				head, err := store.Session(sess).Head()
				if err != nil {
					t.Fatal(err)
				}

				if op == "suspend" {
					_, err = c.Suspend(ctx, &v1.SuspendRequest{Session: sess})
				} else {
					_, err = c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
				}
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("%s of a session whose actor is %s: want FailedPrecondition, got %v", op, actor, err)
				}
				if msg := status.Convert(err).Message(); !strings.Contains(msg, wantMsg) {
					t.Errorf("error %q does not contain %q", msg, wantMsg)
				}
				ctl.mu.Lock()
				gotCreated, gotSuspends := ctl.created, ctl.suspends
				ctl.mu.Unlock()
				if gotCreated != created || gotSuspends != suspends {
					t.Errorf("actors created %d->%d, suspends %d->%d: the refused %s may create or snapshot nothing",
						created, gotCreated, suspends, gotSuspends, op)
				}
				if got, err := store.Session(sess).Head(); err != nil || got != head {
					t.Errorf("journal head after the refused %s = %d (err %v), want %d", op, got, err, head)
				}
			})
		}
	}
}

// A stateful Fork checks that the suspended parent still holds its snapshot on both sides of the tag.
// A parent deleted in between took that snapshot with it, so the fork is FAILED_PRECONDITION, not
// INTERNAL, and no child actor is created.
func TestForkWhoseParentIsDeletedAtTheTagIsRefused(t *testing.T) {
	ctx := context.Background()
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := newMemoryHost(t, ctl, store, echoagent.Harness{})
	sess := mustCreate(t, c)
	if err := execHi(c, sess); err != nil {
		t.Fatalf("first turn: %v", err)
	}
	ctl.onTag = func(actor string) {
		if err := ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor}); err != nil {
			t.Error(err)
		}
	}
	ctl.mu.Lock()
	created := ctl.created
	ctl.mu.Unlock()

	_, err = c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("fork whose parent is deleted at the tag: want FailedPrecondition, got %v", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "no longer exists") {
		t.Errorf("error %q does not say the parent no longer exists", msg)
	}
	ctl.mu.Lock()
	gotCreated := ctl.created
	ctl.mu.Unlock()
	if gotCreated != created {
		t.Errorf("actors created %d->%d: the refused fork may create no child", created, gotCreated)
	}
}

// A stateful Fork whose TagActor call fails is classified by reading the parent again, since
// substrate reports a missing source actor as a plain FAILED_PRECONDITION and contention for the
// actor's lease as ABORTED: a parent deleted during the tag is FAILED_PRECONDITION
// (api.ErrMemoryStateLost), one woken and suspended again holds another snapshot and is ABORTED
// (api.ErrSnapshotSuperseded), and a tag refused because another operation holds the parent is
// ABORTED (substrate.ErrActorBusy). Each creates no child.
func TestForkWhoseTagFailsIsClassified(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		onTag   func(ctl *actorControl, actor string)
		tagErr  error
		want    codes.Code
		wantMsg string
	}{
		{
			name: "parent deleted during the tag",
			onTag: func(ctl *actorControl, actor string) {
				_ = ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor})
			},
			tagErr:  errors.New("rpc error: code = FailedPrecondition desc = Actor space/x not found"),
			want:    codes.FailedPrecondition,
			wantMsg: "no longer exists",
		},
		{
			name: "parent suspended again during the tag",
			onTag: func(ctl *actorControl, actor string) {
				ctl.mu.Lock()
				ctl.snapshots[actor] = "snap-newer"
				ctl.mu.Unlock()
			},
			tagErr:  errors.New("rpc error: code = Internal desc = copy failed"),
			want:    codes.Aborted,
			wantMsg: "want suspended holding",
		},
		{
			name:    "tag contends for the parent's lease",
			tagErr:  fmt.Errorf("%w: space/x: rpc error: code = Aborted desc = another operation is in progress", substrate.ErrActorBusy),
			want:    codes.Aborted,
			wantMsg: "another operation is in progress",
		},
		{
			name:    "any other tag failure stays internal",
			tagErr:  errors.New("rpc error: code = Unavailable desc = ate-api unavailable"),
			want:    codes.Internal,
			wantMsg: "ate-api unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			c := newMemoryHost(t, ctl, store, echoagent.Harness{})
			sess := mustCreate(t, c)
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			if tc.onTag != nil {
				ctl.onTag = func(actor string) { tc.onTag(ctl, actor) }
			}
			ctl.tagErr = tc.tagErr
			ctl.mu.Lock()
			created := ctl.created
			ctl.mu.Unlock()

			_, err = c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
			if status.Code(err) != tc.want {
				t.Fatalf("fork: want %v, got %v", tc.want, err)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("error %q does not contain %q", msg, tc.wantMsg)
			}
			ctl.mu.Lock()
			gotCreated := ctl.created
			ctl.mu.Unlock()
			if gotCreated != created {
				t.Errorf("actors created %d->%d: the refused fork may create no child", created, gotCreated)
			}
		})
	}
}

// Resume of a memory-tier session whose actor substrate reports missing is FAILED_PRECONDITION even
// when a third party creates a replacement under the session's name before Restore could ask
// GetActor: after the resume's ErrActorNotFound, GetActor finds an actor that does not hold the
// snapshot the session was suspended to (the replacement holds none), so the state is lost.
func TestResumeWhoseActorIsReplacedAfterTheFailedResumeIsRefused(t *testing.T) {
	ctx := context.Background()
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := newMemoryHost(t, ctl, store, echoagent.Harness{})
	sess := mustCreate(t, c)
	if err := execHi(c, sess); err != nil {
		t.Fatalf("first turn: %v", err)
	}
	if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	head, err := store.Session(sess).Head()
	if err != nil {
		t.Fatal(err)
	}
	// The resume finds the actor deleted; the first GetActor after it finds a replacement.
	var deleted, replaced atomic.Bool
	ctl.onResume = func(actor string) {
		if actor == sess && deleted.CompareAndSwap(false, true) {
			_ = ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor})
		}
	}
	ctl.onGet = func(actor string) {
		if actor == sess && deleted.Load() && replaced.CompareAndSwap(false, true) {
			_ = ctl.CreateActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor}, substrate.ObjectRef{})
		}
	}

	_, err = c.Resume(ctx, &v1.ResumeRequest{Session: sess})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resume of a replaced actor: want FailedPrecondition, got %v", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "cannot be restored") {
		t.Errorf("error %q does not say the state cannot be restored", msg)
	}
	if got, err := store.Session(sess).Head(); err != nil || got != head {
		t.Errorf("journal head after the refused resume = %d (err %v), want %d", got, err, head)
	}
}

// Resume of a memory-tier session with no SUSPEND recorded (its host died during a turn) restores the
// actor by name, with no snapshot handle to compare. Substrate answers ResumeActor with NOT_FOUND for
// an actor that still exists when its template's volume is missing, so a NOT_FOUND resume alone does
// not prove the state is gone. Restore then compares the actor GetActor finds with the UID the
// journal recorded: the recorded actor keeps the resume error (INTERNAL), and only a replacement is
// FAILED_PRECONDITION. Neither journals anything.
func TestResumeWithoutASnapshotIsLostOnlyWhenTheRecordedActorIsGone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replace bool
		want    codes.Code
	}{
		{name: "the recorded actor is still there", want: codes.Internal},
		{name: "the actor was replaced after the failed resume", replace: true, want: codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			c := newMemoryHost(t, ctl, store, echoagent.Harness{})
			sess := mustCreate(t, c)
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			head, err := store.Session(sess).Head()
			if err != nil {
				t.Fatal(err)
			}
			var failed atomic.Bool
			ctl.resumeErr = func(actor string) error {
				if actor != sess || !failed.CompareAndSwap(false, true) {
					return nil
				}
				if tc.replace {
					_ = ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor})
					_ = ctl.CreateActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor}, substrate.ObjectRef{})
				}
				return fmt.Errorf("%w: template volume not found", substrate.ErrActorNotFound)
			}

			_, err = c.Resume(ctx, &v1.ResumeRequest{Session: sess})
			if status.Code(err) != tc.want {
				t.Fatalf("resume: want %v, got %v", tc.want, err)
			}
			if got, err := store.Session(sess).Head(); err != nil || got != head {
				t.Errorf("journal head after the failed resume = %d (err %v), want %d: nothing may be journaled", got, err, head)
			}
			if tc.want == codes.FailedPrecondition {
				if msg := status.Convert(err).Message(); !strings.Contains(msg, "cannot be restored") {
					t.Errorf("error %q does not say the state cannot be restored", msg)
				}
			}
		})
	}
}

// An Exec finds the session's actor SUSPENDED and resumes it, and the actor is deleted in between.
// For a REQUIRES_MEMORY_SNAPSHOT session that is the actor lost, as when Exec finds it missing:
// FAILED_PRECONDITION, no actor created and nothing journaled, where the bare resume error used to
// surface as INTERNAL. A STATELESS_REPLAY session gets a new actor and the turn runs.
func TestExecWhoseActorIsDeletedDuringTheResume(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tier       api.Resumability
		wantRefuse bool
	}{
		{name: "memory tier is refused", tier: api.ResumabilityRequiresMemorySnapshot, wantRefuse: true},
		{name: "stateless tier creates a new actor", tier: api.ResumabilityStatelessReplay},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			desc, err := echoagent.Harness{}.Describe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			desc.Capabilities.Resumability = tc.tier
			backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "echo"}, desc)
			dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return echoagent.Harness{}, func() error { return nil }, nil
			})
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(backend, echoagent.Model, dial)})
			if err != nil {
				t.Fatal(err)
			}
			c := serveRegistry(t, store, r)
			sess := mustCreate(t, c)
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
				t.Fatalf("suspend: %v", err)
			}
			head, err := store.Session(sess).Head()
			if err != nil {
				t.Fatal(err)
			}
			// The next Exec's GetActor sees the actor SUSPENDED; it is deleted before the resume.
			var deleted atomic.Bool
			ctl.onResume = func(actor string) {
				if actor == sess && deleted.CompareAndSwap(false, true) {
					_ = ctl.DeleteActor(ctx, substrate.ActorRef{Atespace: "space", Name: actor})
				}
			}

			err = execHi(c, sess)
			ctl.mu.Lock()
			_, recreated := ctl.status[sess]
			ctl.mu.Unlock()
			if !deleted.Load() {
				t.Fatal("the Exec never resumed the suspended actor")
			}
			if !tc.wantRefuse {
				if err != nil || !recreated {
					t.Fatalf("turn whose actor was deleted during the resume: err=%v recreated=%v, want a new actor and success", err, recreated)
				}
				return
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("turn whose actor was deleted during the resume: want FailedPrecondition, got %v", err)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, sess) || !strings.Contains(msg, "cannot be reconstructed") {
				t.Errorf("error %q does not name the session and the lost state", msg)
			}
			if recreated {
				t.Error("the refused turn created an actor for the session")
			}
			if got, err := store.Session(sess).Head(); err != nil || got != head {
				t.Errorf("journal head after the refused turn = %d (err %v), want %d: nothing may be journaled", got, err, head)
			}
		})
	}
}
