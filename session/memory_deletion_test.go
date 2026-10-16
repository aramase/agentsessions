package session_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/eventlog"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/substrate"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// countingControl records every ControlClient and SnapshotCloner call that reaches actorControl.
type countingControl struct {
	*actorControl
	mu    sync.Mutex
	calls []string
}

func (c *countingControl) record(op, actor string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, op+" "+actor)
}

func (c *countingControl) reset() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls := c.calls
	c.calls = nil
	return calls
}

func (c *countingControl) CreateActor(ctx context.Context, a substrate.ActorRef, t substrate.ObjectRef) error {
	c.record("CreateActor", a.Name)
	return c.actorControl.CreateActor(ctx, a, t)
}

func (c *countingControl) ResumeActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	c.record("ResumeActor", a.Name)
	return c.actorControl.ResumeActor(ctx, a)
}

func (c *countingControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	c.record("SuspendActor", a.Name)
	return c.actorControl.SuspendActor(ctx, a)
}

func (c *countingControl) DeleteActor(ctx context.Context, a substrate.ActorRef) error {
	c.record("DeleteActor", a.Name)
	return c.actorControl.DeleteActor(ctx, a)
}

func (c *countingControl) GetActor(ctx context.Context, a substrate.ActorRef) (substrate.ActorInfo, error) {
	c.record("GetActor", a.Name)
	return c.actorControl.GetActor(ctx, a)
}

func (c *countingControl) TagActor(ctx context.Context, a substrate.ActorRef, tag substrate.SnapshotID) error {
	c.record("TagActor", a.Name)
	return c.actorControl.TagActor(ctx, a, tag)
}

func (c *countingControl) CreateActorFromTag(ctx context.Context, a substrate.ActorRef, t substrate.ObjectRef, tag substrate.SnapshotID) (substrate.ActorInfo, error) {
	c.record("CreateActorFromTag", a.Name)
	return c.actorControl.CreateActorFromTag(ctx, a, t, tag)
}

func (c *countingControl) DeleteTag(ctx context.Context, tag substrate.SnapshotID) error {
	c.record("DeleteTag", tag.Name)
	return c.actorControl.DeleteTag(ctx, tag)
}

// A permanently deleted memory-tier session is refused before runtime/substrate touches its actor:
// no Exec, Resume, Suspend or Fork creates, resumes, suspends, tags or clones one. That holds for
// the public RPCs, which refuse at the service's lookup, and for the Placer and Registry entry
// points a lookup that ran before the deletion reaches, which recheck it under the session guard.
// The session is deleted with its actor running, suspended, or as a stateful fork's child before
// its first turn.
func TestDeletedMemorySessionIsRefusedBeforeSubstrate(t *testing.T) {
	for _, state := range []string{"running", "suspended", "fork child"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			ctl := &countingControl{actorControl: &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}}
			desc, err := echoagent.Harness{}.Describe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			desc.ID = "mem"
			desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
			backend := substrate.New(ctl, "space", substrate.ObjectRef{Atespace: "space", Name: "mem"}, desc)
			p := placement.New(backend, echoagent.Model, placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return echoagent.Harness{}, func() error { return nil }, nil
			}))
			r, err := placement.NewRegistry("mem", map[string]*placement.Placer{"mem": p})
			if err != nil {
				t.Fatal(err)
			}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			c := serveRegistry(t, store, r)

			sess := mustCreate(t, c)
			if err := execHi(c, sess); err != nil {
				t.Fatalf("first turn: %v", err)
			}
			switch state {
			case "suspended":
				if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
					t.Fatalf("suspend: %v", err)
				}
			case "fork child":
				resp, err := c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
				if err != nil {
					t.Fatalf("fork: %v", err)
				}
				sess = resp.GetChildren()[0].GetMetadata().GetUid()
			}
			if _, err := c.DeleteSession(ctx, &v1.DeleteSessionRequest{Uid: sess}); err != nil {
				t.Fatalf("delete: %v", err)
			}
			log := store.Session(sess)
			head, err := log.Head()
			if err != nil {
				t.Fatal(err)
			}
			ctl.mu.Lock()
			actor := ctl.status[sess]
			ctl.mu.Unlock()
			ctl.reset()

			public := map[string]func() error{
				"Exec": func() error { return execHi(c, sess) },
				"Resume": func() error {
					_, err := c.Resume(ctx, &v1.ResumeRequest{Session: sess})
					return err
				},
				"Suspend": func() error {
					_, err := c.Suspend(ctx, &v1.SuspendRequest{Session: sess})
					return err
				},
				"Fork": func() error {
					_, err := c.Fork(ctx, &v1.ForkRequest{Session: sess, Count: 1})
					return err
				},
			}
			for op, call := range public {
				err := call()
				if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "permanently deleted") {
					t.Errorf("Sessions.%s of a deleted session = %v, want FailedPrecondition naming the deletion", op, err)
				}
			}
			// A service whose lookup ran before the deletion reaches these entry points; they
			// recheck it under the session guard before any backend call.
			child := store.Session(sess + "-child")
			direct := map[string]func() error{
				"Placer.Exec": func() error {
					_, err := p.Exec(ctx, log, sess, []api.Message{*api.TextMessage("user", "hi")}, head)
					return err
				},
				"Placer.Resume": func() error { return p.Resume(ctx, log, sess) },
				"Placer.Suspend": func() error {
					_, err := p.Suspend(ctx, log, sess)
					return err
				},
				"Placer.Fork": func() error {
					return p.Fork(ctx, log, sess, []placement.ForkChild{{UID: sess + "-child", Log: child}}, head)
				},
				"Registry.Resume": func() error { return r.Resume(ctx, log, sess, "mem") },
				"Registry.Suspend": func() error {
					_, err := r.Suspend(ctx, log, sess, "mem")
					return err
				},
			}
			for op, call := range direct {
				if err := call(); !errors.Is(err, eventlog.ErrDeleted) {
					t.Errorf("%s of a deleted session = %v, want eventlog.ErrDeleted", op, err)
				}
			}

			if calls := ctl.reset(); len(calls) != 0 {
				t.Errorf("refused calls on a deleted session reached the control plane: %v", calls)
			}
			ctl.mu.Lock()
			got := ctl.status[sess]
			ctl.mu.Unlock()
			if got != actor {
				t.Errorf("actor status after the refused calls = %v, want %v", got, actor)
			}
			if got, err := log.Head(); err != nil || got != head {
				t.Errorf("journal head after the refused calls = %d (err %v), want %d", got, err, head)
			}
			if got, err := child.Head(); err != nil || got != 0 {
				t.Errorf("fork child journal head = %d (err %v), want 0", got, err)
			}
		})
	}
}
