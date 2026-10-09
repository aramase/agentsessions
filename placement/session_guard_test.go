package placement

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

func guardEntries(p *Placer) int {
	p.guard.mu.Lock()
	defer p.guard.mu.Unlock()
	return len(p.guard.entries)
}

func TestSessionGuardDrainsAfterContention(t *testing.T) {
	p := New(nil, nil)
	release, err := p.trySessionLock("session")
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := p.trySessionLock("session"); !errors.Is(err, ErrSessionBusy) {
			t.Fatalf("overlapping acquisition = %v, want ErrSessionBusy", err)
		}
		if n := guardEntries(p); n != 1 {
			t.Fatalf("contention removed the holder's guard: entries=%d", n)
		}
	}
	other, err := p.trySessionLock("independent")
	if err != nil {
		t.Fatal(err)
	}
	other()
	release()
	if n := guardEntries(p); n != 0 {
		t.Fatalf("released session guards retained %d entries", n)
	}
}

func TestSessionGuardConcurrentReleaseAndAcquire(t *testing.T) {
	p := New(nil, nil)
	var active atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				release, err := p.trySessionLock("session")
				if err != nil {
					if !errors.Is(err, ErrSessionBusy) {
						t.Errorf("acquisition = %v, want ErrSessionBusy", err)
					}
					continue
				}
				if active.Add(1) != 1 {
					t.Error("two live guards for the same session")
				}
				active.Add(-1)
				release()
			}
		})
	}
	wg.Wait()
	if n := guardEntries(p); n != 0 {
		t.Fatalf("concurrent acquisitions retained %d entries", n)
	}
}

type failingGuardBackend struct {
	Backend
	err error
}

func (b failingGuardBackend) Create(context.Context, *api.SessionSpec) (api.Incarnation, error) {
	return api.Incarnation{}, b.err
}

func (b failingGuardBackend) Snapshot(context.Context, api.Incarnation, api.SnapshotKind) (api.SnapshotRef, error) {
	return api.SnapshotRef{}, b.err
}

func (b failingGuardBackend) Restore(context.Context, api.SnapshotRef) (api.Incarnation, error) {
	return api.Incarnation{}, b.err
}

type failingGuardStore struct {
	eventlog.Store
	err error
}

func (s failingGuardStore) NewFence() (int64, error) { return 0, s.err }

func TestRegistryResumeGuardDrainsAfterRoutingErrors(t *testing.T) {
	zero, one := int64(0), int64(1)
	for _, tc := range []struct {
		name        string
		event       *api.Event
		defaultName string
		want        error
	}{
		{"incomplete invocation", &api.Event{Kind: api.EventExecutionStart, ExecutionID: "pending", ExecutionStart: &api.ExecutionStart{InputCount: &one}}, "default", controller.ErrIncompleteInvocation},
		{"unserved recorded name", &api.Event{Kind: api.EventExecutionStart, ExecutionID: "pending", ExecutionStart: &api.ExecutionStart{InputCount: &zero, Harness: "unserved"}}, "default", ErrRecordedHarnessNotServed},
		{"unknown stored default", nil, "unserved", ErrUnknownHarness},
		{"invalid journal", &api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "missing identity")}, "default", controller.ErrInvalidExecutionLog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(nil, nil)
			r, err := NewRegistry("default", map[string]*Placer{"default": p})
			if err != nil {
				t.Fatal(err)
			}
			log := eventlog.AsStore(eventlog.New())
			if tc.event != nil {
				fence, err := log.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := log.Append(0, fence, *tc.event); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := r.Resume(t.Context(), log, "session", tc.defaultName); !errors.Is(err, tc.want) {
					t.Fatalf("Registry.Resume=%v, want %v", err, tc.want)
				}
				if n := guardEntries(p); n != 0 {
					t.Fatalf("routing failure retained %d guard entries", n)
				}
			}
		})
	}
}

// Exercise each operation's deferred release, including backend and journal error returns.
func TestSessionGuardDrainsAfterOperations(t *testing.T) {
	for _, failure := range []string{"none", "backend", "journal"} {
		for _, operation := range []string{"Exec", "Suspend", "Resume", "Registry.Resume"} {
			t.Run(failure+"/"+operation, func(t *testing.T) {
				backend := local.New(echoagent.Harness{})
				t.Cleanup(func() { _ = backend.Close() })
				var b Backend = backend
				backendErr := errors.New("backend failed")
				journalErr := errors.New("journal failed")
				if failure == "backend" {
					b = failingGuardBackend{Backend: backend, err: backendErr}
				}
				p := New(b, echoagent.Model)
				registry, err := NewRegistry("echo", map[string]*Placer{"echo": p})
				if err != nil {
					t.Fatal(err)
				}
				store, err := sqlitelog.Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				var log eventlog.Store = store.Session("session")
				if failure == "journal" {
					log = failingGuardStore{Store: log, err: journalErr}
				}
				for range 2 {
					switch operation {
					case "Exec":
						head, _ := log.Head()
						_, err = p.Exec(t.Context(), log, "session", []api.Message{*api.TextMessage("user", "hello")}, head)
					case "Suspend":
						_, err = p.Suspend(t.Context(), log, "session")
					case "Resume":
						err = p.Resume(t.Context(), log, "session")
					case "Registry.Resume":
						err = registry.Resume(t.Context(), log, "session", "echo")
					}
					if failure == "none" && err != nil {
						t.Fatal(err)
					}
					if failure == "backend" && !errors.Is(err, backendErr) {
						t.Fatalf("want backend failure, got %v", err)
					}
					if failure == "journal" && !errors.Is(err, journalErr) {
						t.Fatalf("want journal failure, got %v", err)
					}
					if n := guardEntries(p); n != 0 {
						t.Fatalf("%s retained %d guards after %s", operation, n, failure)
					}
				}
			})
		}
	}
}
