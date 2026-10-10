package placement

import (
	"context"
	"errors"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/runtime/local"
)

type approvalGuardLog struct {
	eventlog.Store
	onRead func()
}

func (l *approvalGuardLog) Read(from int64) ([]eventlog.Record, error) {
	if hook := l.onRead; hook != nil {
		l.onRead = nil
		hook()
	}
	return l.Store.Read(from)
}

type approvalGuardBackend struct {
	Backend
	onSnapshot func()
}

func (b approvalGuardBackend) Snapshot(ctx context.Context, inc api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	return b.Backend.Snapshot(ctx, inc, kind)
}

// Every Registry operation and every cross-Placer operation must acquire the same session guard
// BEFORE inspecting stale evidence. Nil logs make any accidental pre-lock read fail immediately.
func TestApprovalGuardCrossOperationsAndDrain(t *testing.T) {
	for _, outer := range []string{"Exec", "Resume", "Suspend", "Registry.Resume", "Registry.Suspend", "Approve"} {
		t.Run(outer, func(t *testing.T) {
			backend := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = backend.Close() })
			p, other := New(backend, echoagent.Model), New(backend, echoagent.Model)
			r, err := NewRegistry("default", map[string]*Placer{"default": other, "recorded": p})
			if err != nil {
				t.Fatal(err)
			}
			log := &approvalGuardLog{Store: eventlog.AsStore(eventlog.New())}
			fence, _ := log.NewFence()
			zero := int64(0)
			events := []api.Event{
				{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "recorded", InputCount: &zero}},
				{ExecutionID: "e1", Kind: api.EventToolCall, ToolCall: &api.ToolCall{ID: "c1", Tool: "write", Mediation: api.MediationRequiresApproval, IdempotencyKey: "key"}},
				{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}},
			}
			for i, ev := range events {
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			decision := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 3, Approved: true}
			hookRan := false
			hook := func() {
				hookRan = true
				operations := []struct {
					name string
					run  func() error
				}{
					{"Exec", func() error { _, err := other.Exec(t.Context(), nil, "s", nil, 0); return err }},
					{"Resume", func() error { return other.Resume(t.Context(), nil, "s") }},
					{"Suspend", func() error { _, err := other.Suspend(t.Context(), nil, "s"); return err }},
					{"Registry.Resume", func() error { return r.Resume(t.Context(), nil, "s", "unserved") }},
					{"Registry.Suspend", func() error { _, err := r.Suspend(t.Context(), nil, "s", "unserved"); return err }},
					{"Approve", func() error { _, err := r.Approve(t.Context(), nil, "s", api.ApprovalDecision{}); return err }},
				}
				for _, op := range operations {
					if err := op.run(); !errors.Is(err, ErrSessionBusy) {
						t.Errorf("%s during %s=%v, want ErrSessionBusy", op.name, outer, err)
					}
				}
				// Metadata lookups/Add use Registry.mu, not the held session guard.
				if _, err := r.For("recorded"); err != nil {
					t.Fatal(err)
				}
				if err := r.Add("added", New(backend, echoagent.Model)); err != nil {
					t.Fatal(err)
				}
			}
			if outer == "Suspend" {
				p.backend = approvalGuardBackend{Backend: backend, onSnapshot: hook}
			} else {
				log.onRead = hook
			}
			switch outer {
			case "Exec":
				_, err = p.Exec(t.Context(), log, "s", nil, 3)
			case "Resume":
				err = p.Resume(t.Context(), log, "s")
			case "Suspend":
				_, err = p.Suspend(t.Context(), log, "s")
			case "Registry.Resume":
				err = r.Resume(t.Context(), log, "s", "default")
			case "Registry.Suspend":
				_, err = r.Suspend(t.Context(), log, "s", "default")
			case "Approve":
				_, err = r.Approve(t.Context(), log, "s", decision)
			}
			if !hookRan {
				t.Fatal("outer operation did not reach guarded hook")
			}
			var want error
			if outer == "Exec" {
				want = controller.ErrApprovalBlocked
			}
			if outer == "Resume" || outer == "Registry.Resume" {
				want = api.ErrApprovalParked
			}
			if !errors.Is(err, want) {
				t.Fatalf("outer %s=%v want=%v", outer, err, want)
			}
			if n := guardEntries(p); n != 0 {
				t.Fatalf("outer %s retained %d guards after %v", outer, n, err)
			}
			// Invalid approval failure also drains the guard; its stale validation must not
			// permanently block an independent Placer that shares the Registry.
			if _, err := r.Approve(t.Context(), log, "s", api.ApprovalDecision{}); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
				t.Fatalf("invalid decision=%v", err)
			}
			if n := guardEntries(other); n != 0 {
				t.Fatalf("validation failure retained %d guards", n)
			}
		})
	}
}
