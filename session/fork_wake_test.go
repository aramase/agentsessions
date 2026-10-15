package session_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// actorControl is an in-memory substrate control plane for the substrate runtime: actors with a
// status and the snapshot they hold, a new snapshot on every suspend of a running actor, and tags
// that clone them.
// onResume and onGet, when set, run before ResumeActor and GetActor answer, outside the lock.
type actorControl struct {
	mu        sync.Mutex
	status    map[string]substrate.ActorStatus
	snapshots map[string]string
	suspends  int
	onResume  func(actor string)
	onSuspend func(actor string)
	onGet     func(actor string)
	// onTag, when set, runs when TagActor tags actor; tagErr, when set, is what TagActor then returns.
	onTag  func(actor string)
	tagErr error
	// onCreate, when set, runs before CreateActor looks at the registry.
	onCreate func(actor string)
	// uids is each actor's server-assigned UID, minted at create and dropped at delete, so an actor
	// created again under a name reports a new one.
	uids    map[string]string
	created int
}

// newUID mints the UID substrate assigns an actor at creation. c.mu must be held.
func (c *actorControl) newUID(name string) {
	if c.uids == nil {
		c.uids = map[string]string{}
	}
	c.created++
	c.uids[name] = fmt.Sprintf("uid-%d", c.created)
}

func (c *actorControl) CreateActor(_ context.Context, a substrate.ActorRef, _ substrate.ObjectRef) error {
	if c.onCreate != nil {
		c.onCreate(a.Name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.status[a.Name]; ok {
		return fmt.Errorf("%w: %q", substrate.ErrActorExists, a.Name)
	}
	c.status[a.Name] = substrate.StatusSuspended
	c.newUID(a.Name)
	return nil
}

func (c *actorControl) ResumeActor(_ context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	if c.onResume != nil {
		c.onResume(a.Name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.status[a.Name]; !ok {
		return substrate.ActorInfo{}, substrate.ErrActorNotFound
	}
	c.status[a.Name] = substrate.StatusRunning
	return substrate.ActorInfo{Status: substrate.StatusRunning, Worker: "w", UID: c.uids[a.Name]}, nil
}

func (c *actorControl) SuspendActor(_ context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	c.mu.Lock()
	if _, ok := c.status[a.Name]; !ok {
		c.mu.Unlock()
		return substrate.ActorInfo{}, substrate.ErrActorNotFound
	}
	// Substrate's suspend is a no-op on an actor that is already SUSPENDED holding a snapshot.
	snap := c.snapshots[a.Name]
	if c.status[a.Name] != substrate.StatusSuspended || snap == "" {
		c.suspends++
		snap = fmt.Sprintf("snap-%s-%d", a.Name, c.suspends)
		c.status[a.Name], c.snapshots[a.Name] = substrate.StatusSuspended, snap
	}
	uid := c.uids[a.Name]
	c.mu.Unlock()
	if c.onSuspend != nil {
		c.onSuspend(a.Name)
	}
	return substrate.ActorInfo{Status: substrate.StatusSuspended, Worker: "w", Snapshot: snap, UID: uid}, nil
}

func (c *actorControl) DeleteActor(_ context.Context, a substrate.ActorRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.status, a.Name)
	delete(c.uids, a.Name)
	// The snapshot went with the actor: one created again under the name holds none.
	delete(c.snapshots, a.Name)
	return nil
}

func (c *actorControl) GetActor(_ context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	if c.onGet != nil {
		c.onGet(a.Name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.status[a.Name]
	if !ok {
		return substrate.ActorInfo{}, substrate.ErrActorNotFound
	}
	return substrate.ActorInfo{Status: st, Worker: "w", Snapshot: c.snapshots[a.Name], UID: c.uids[a.Name]}, nil
}

func (c *actorControl) TagActor(_ context.Context, a substrate.ActorRef, _ substrate.SnapshotID) error {
	if c.onTag != nil {
		c.onTag(a.Name)
	}
	return c.tagErr
}

func (c *actorControl) CreateActorFromTag(_ context.Context, a substrate.ActorRef, _ substrate.ObjectRef, _ substrate.SnapshotID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status[a.Name] = substrate.StatusSuspended
	c.newUID(a.Name)
	return nil
}

func (c *actorControl) DeleteTag(context.Context, substrate.SnapshotID) error { return nil }

// A turn whose Runtime.Create or Runtime.Restore is already in flight when a stateful fork installs
// its checkpoint mark is past the Placer's checkpoint check, so nothing in this Registry stops it
// waking the parent between the fork's checkpoint and its tag. The substrate runtime then refuses to tag a snapshot the parent
// no longer holds. That is a lost race the caller retries at the parent's new head, so Fork must
// report it as Aborted, not Internal; and the turn, which finds the parent held by the fork, is
// Aborted as well.
func TestForkWhoseParentAnInFlightTurnWakesIsAborted(t *testing.T) {
	for _, tc := range []struct {
		name string
		turn func(v1.SessionsClient, string) error
	}{
		{"exec", execHi},
		{"resume", func(c v1.SessionsClient, sess string) error {
			_, err := c.Resume(context.Background(), &v1.ResumeRequest{Session: sess})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			desc, err := echoagent.Harness{}.Describe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
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
			// One turn provisions the parent's actor; suspending it leaves the actor SUSPENDED, so
			// the next turn's Create or Restore has to resume it.
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			if _, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess}); err != nil {
				t.Fatalf("suspend: %v", err)
			}

			// Force the interleaving: the turn's resume of the parent waits until the fork has
			// checkpointed it, and the fork's check of the parent waits until the turn has returned.
			var once sync.Once
			waking, checkpointed, turnDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			hookErr := make(chan error, 2)
			wait := func(ch <-chan struct{}, what string) {
				select {
				case <-ch:
				case <-time.After(10 * time.Second):
					hookErr <- fmt.Errorf("timed out waiting for %s", what)
				}
			}
			ctl.onResume = func(actor string) {
				if actor != sess {
					return
				}
				once.Do(func() {
					close(waking)
					wait(checkpointed, "the fork's checkpoint")
				})
			}
			ctl.onSuspend = func(actor string) {
				if actor == sess {
					select {
					case <-waking:
						close(checkpointed)
					default:
					}
				}
			}
			ctl.onGet = func(actor string) {
				select {
				case <-checkpointed:
					if actor == sess {
						wait(turnDone, "the turn to return")
					}
				default:
				}
			}

			turnErr := make(chan error, 1)
			go func() {
				err := tc.turn(c, sess)
				close(turnDone)
				turnErr <- err
			}()
			select {
			case <-waking:
			case err := <-turnErr:
				t.Fatalf("turn returned before it resumed the parent: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("turn never resumed the parent")
			}
			forkErr := forkParent(c, sess)
			select {
			case err := <-hookErr:
				t.Fatal(err)
			default:
			}
			if status.Code(forkErr) != codes.Aborted || !strings.Contains(status.Convert(forkErr).Message(), substrate.ErrSnapshotSuperseded.Error()) {
				t.Fatalf("fork whose parent a turn woke before the tag: want Aborted for a superseded snapshot, got %v", forkErr)
			}
			if err := <-turnErr; status.Code(err) != codes.Aborted {
				t.Fatalf("turn that woke the parent during the fork: want Aborted, got %v", err)
			}
		})
	}
}
