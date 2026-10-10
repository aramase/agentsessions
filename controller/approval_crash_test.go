package controller_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

// The test executor commits its effect independently of the controller's journal. Reopening
// its SQLite database models loss of process memory in the effect-before-receipt crash gap.
func TestApprovalEffectBeforeReceiptDurableDedupAndSessionScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.db")
	open := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	if _, err := db.Exec("CREATE TABLE effects (session TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (session, key))"); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	executor := func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		attempts++
		if call.Mediation != api.MediationRequiresApproval || call.ID != "c1" || call.IdempotencyKey != "key" || call.Args["path"] != "report.txt" {
			t.Fatalf("changed original call=%+v", call)
		}
		if _, err := db.Exec("INSERT INTO effects (session, key) VALUES (?, ?) ON CONFLICT DO NOTHING", scope.SessionUID, call.IdempotencyKey); err != nil {
			return api.ToolResult{}, err
		}
		return api.ToolResult{Output: map[string]any{"effect": "durable"}}, nil
	}
	log := approvalLog(t, "sqlite")
	approvalSeed(t, log, true, api.EventApprovalResult)
	cause := errors.New("receipt disk unavailable")
	c, err := controller.New(failGateAppend{log, api.EventToolResult, cause}, nil, controller.WithSessionUID("session-1"), controller.WithToolExecutor(executor))
	if err != nil {
		t.Fatal(err)
	}
	h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		_, err := sink.ToolCall(ctx, gatedCall())
		return err
	})
	if resumed, err := c.Resume(t.Context(), h); !resumed || !errors.Is(err, cause) || errors.Is(err, controller.ErrReplayDiverged) {
		t.Fatalf("first Resume=%t,%v", resumed, err)
	}
	assertNoGateContinuation(t, log)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	fresh, err := controller.New(log, nil, controller.WithSessionUID("session-1"), controller.WithToolExecutor(executor))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := fresh.Resume(t.Context(), h); !resumed || err != nil {
		t.Fatalf("recovered Resume=%t,%v", resumed, err)
	}
	var effects int
	if err := db.QueryRow("SELECT COUNT(*) FROM effects").Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || attempts != 2 {
		t.Fatalf("same session effects=%d attempts=%d", effects, attempts)
	}
	if _, err := fresh.Replay(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatal("receipt replay repeated executor")
	}
	receipt := approvalRecord(t, log, api.EventToolResult)
	if receipt.Event.Result.Approval != nil || receipt.Event.Result.Output["effect"] != "durable" {
		t.Fatalf("receipt=%+v", receipt.Event.Result)
	}
	// The same harness-chosen key in another session is a distinct dedup scope.
	other := approvalLog(t, "sqlite")
	approvalSeed(t, other, true, api.EventApprovalResult)
	oc, err := controller.New(other, nil, controller.WithSessionUID("session-2"), controller.WithToolExecutor(executor))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := oc.Resume(t.Context(), h); !resumed || err != nil {
		t.Fatalf("other session=%t,%v", resumed, err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM effects").Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 2 || attempts != 3 {
		t.Fatalf("different session effects=%d attempts=%d", effects, attempts)
	}
}

type advanceGateRead struct {
	eventlog.Store
	advance bool
	fence   int64
}

func (s *advanceGateRead) NewFence() (int64, error) {
	fence, err := s.Store.NewFence()
	s.fence = fence
	return fence, err
}
func (s *advanceGateRead) Read(from int64) ([]eventlog.Record, error) {
	records, err := s.Store.Read(from)
	if err != nil {
		return nil, err
	}
	if s.advance {
		s.advance = false
		head, err := s.Store.Head()
		if err != nil {
			return nil, err
		}
		_, err = s.Store.Append(head, s.fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}})
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func TestApprovalRepairUsesCapturedHeadFenceAndObservation(t *testing.T) {
	for _, scenario := range []string{"observed", "conflict", "fenced", "append failure"} {
		t.Run(scenario, func(t *testing.T) {
			log := approvalLog(t, "memory")
			approvalSeed(t, log, true, api.EventToolCall)
			store := eventlog.Store(log)
			cause := errors.New("repair disk unavailable")
			if scenario == "conflict" {
				store = &advanceGateRead{Store: log, advance: true}
			}
			if scenario == "append failure" {
				store = failGateAppend{log, api.EventApprovalRequest, cause}
			}
			var observed []eventlog.Record
			c, err := controller.New(store, nil, controller.WithObserver(controller.Observer{OnRecord: func(rec eventlog.Record) { observed = append(observed, rec) }}))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "fenced" {
				if _, err := log.NewFence(); err != nil {
					t.Fatal(err)
				}
			}
			resumed, err := c.Resume(t.Context(), nil)
			want := api.ErrApprovalParked
			switch scenario {
			case "conflict":
				want = eventlog.ErrConflict
			case "fenced":
				want = eventlog.ErrFenced
			case "append failure":
				want = cause
			}
			if !resumed || !errors.Is(err, want) || errors.Is(err, controller.ErrReplayDiverged) {
				t.Fatalf("repair=%t,%v", resumed, err)
			}
			if scenario == "observed" {
				request := approvalRecord(t, log, api.EventApprovalRequest)
				if len(observed) != 1 || !reflect.DeepEqual(observed[0], request) {
					t.Fatalf("repair observed=%+v", observed)
				}
				if _, err := c.Resume(t.Context(), nil); !errors.Is(err, api.ErrApprovalParked) || len(observed) != 1 {
					t.Fatalf("repeat repair=%v observations=%d", err, len(observed))
				}
			} else {
				records, _ := log.Read(1)
				for _, rec := range records {
					if rec.Event.Kind == api.EventApprovalRequest {
						t.Fatal("failed repair wrote request")
					}
				}
			}
		})
	}
}

func TestApprovalResumeLiveRequestAppendFailureIsSticky(t *testing.T) {
	log := approvalLog(t, "sqlite")
	approvalAppend(t, log, 0, api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0), Harness: "recorded", HarnessVersion: "v1"}})
	cause := errors.New("request append unavailable")
	c, err := controller.New(failGateAppend{log, api.EventApprovalRequest, cause}, nil, controller.WithSessionUID("session"))
	if err != nil {
		t.Fatal(err)
	}
	h := gateHarness(func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		_, err := sink.ToolCall(ctx, gatedCall())
		if !errors.Is(err, cause) {
			t.Errorf("initial cause=%v", err)
		}
		for _, got := range gateSinkCalls(ctx, sink) {
			if !errors.Is(got, cause) || errors.Is(got, controller.ErrReplayDiverged) {
				t.Errorf("continuation=%v", got)
			}
		}
		return nil
	})
	if resumed, err := c.Resume(t.Context(), h); !resumed || !errors.Is(err, cause) {
		t.Fatalf("Resume=%t,%v", resumed, err)
	}
	assertNoGateContinuation(t, log)
	fresh, err := controller.New(log, nil, controller.WithSessionUID("session"))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := fresh.Resume(t.Context(), nil); !resumed || !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("repair=%t,%v", resumed, err)
	}
}
