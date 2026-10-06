package controller_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Only the selected INPUT append fails; preceding appends commit to the real store.
type failInputStore struct {
	eventlog.Store
	failAt int
	inputs int
	err    error
}

func (s *failInputStore) Append(last, fence int64, event api.Event) (eventlog.Record, error) {
	if event.Kind == api.EventInput {
		s.inputs++
		if s.inputs == s.failAt {
			return eventlog.Record{}, s.err
		}
	}
	return s.Store.Append(last, fence, event)
}

func assertIncompleteInvocation(t *testing.T, log eventlog.Store, c *controller.Controller, har api.Harness) {
	t.Helper()
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(t.Context(), har); resumed || !errors.Is(err, controller.ErrInvalidExecutionLog) {
		t.Errorf("Resume = %v, %v; want false, ErrInvalidExecutionLog", resumed, err)
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Error("invalid invocation appended records (including END)")
	}
}

func TestResumeRejectsFailedInputAppendAfterSQLiteReopen(t *testing.T) {
	for _, values := range []struct {
		name   string
		config []byte
		cursor int64
	}{
		{"default", nil, 0},
		{"config", []byte("opaque"), 0},
		{"cursor", nil, 7},
	} {
		for _, inputs := range [][]api.Message{{msg("first")}, {msg("first"), msg("second")}} {
			for failAt := 1; failAt <= len(inputs); failAt++ {
				t.Run(values.name+"/inputs-"+strconv.Itoa(len(inputs))+"/fail-"+strconv.Itoa(failAt), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "journal.db")
					store, err := sqlitelog.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					log := store.Session("session")
					injected := errors.New("INPUT unavailable")
					failing := &failInputStore{Store: log, failAt: failAt, err: injected}
					runs, modelCalls, toolCalls, observed := 0, 0, 0, 0
					model := func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
						modelCalls++
						return api.ModelResponse{Message: *api.TextMessage("assistant", "reply")}, nil
					}
					tool := func(context.Context, api.ToolCall) (api.ToolResult, error) {
						toolCalls++
						return api.ToolResult{ID: "t1"}, nil
					}
					har := checkedStartHarness{executionConfigHarness{}, func(*api.Start) { runs++ }}
					live, err := controller.New(failing, model, controller.WithStart(values.config, values.cursor),
						controller.WithToolExecutor(tool), controller.WithObserver(controller.Observer{OnRecord: func(eventlog.Record) { observed++ }}))
					if err != nil {
						t.Fatal(err)
					}
					if err := live.Exec(t.Context(), har, inputs, 0); !errors.Is(err, injected) {
						t.Fatalf("Exec = %v, want injected INPUT error", err)
					}
					if runs != 0 || modelCalls != 0 || toolCalls != 0 || observed != failAt {
						t.Fatalf("failed input: runs=%d model=%d tool=%d observed=%d", runs, modelCalls, toolCalls, observed)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					store, err = sqlitelog.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					defer store.Close()
					log = store.Session("session")
					if head, err := log.Head(); err != nil || head != int64(failAt) {
						t.Fatalf("reopened head = %d, %v; want %d", head, err, failAt)
					}
					recovery, err := controller.New(log, model, controller.WithToolExecutor(tool),
						controller.WithObserver(controller.Observer{OnRecord: func(eventlog.Record) { observed++ }}))
					if err != nil {
						t.Fatal(err)
					}
					// Running this harness would reach both external boundaries.
					recoverHar := checkedEffectsHarness{beforeRun: func(*api.Start) { runs++ }}
					assertIncompleteInvocation(t, log, recovery, recoverHar)
					if runs != 0 || modelCalls != 0 || toolCalls != 0 || observed != failAt {
						t.Errorf("incomplete recovery ran effects: runs=%d model=%d tool=%d observed=%d", runs, modelCalls, toolCalls, observed)
					}
					if err := log.Verify(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

type checkedEffectsHarness struct {
	executionConfigHarness
	beforeRun func(*api.Start)
}

func (h checkedEffectsHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.beforeRun(start)
	return (interruptedConfigHarness{h.executionConfigHarness, false}).Run(ctx, start, sink)
}

func TestResumeInputCompletenessFromTruncatedSQLiteSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inputs []api.Message
		cut    int64
		valid  bool
	}{
		{"single missing", []api.Message{msg("first")}, 1, false},
		{"multiple missing", []api.Message{msg("first"), msg("second")}, 1, false},
		{"multiple partial", []api.Message{msg("first"), msg("second")}, 2, false},
		{"all inputs", []api.Message{msg("first"), msg("second")}, 3, true},
		{"genuinely inputless", nil, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store, err := sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			live, err := controller.New(store.Session("session"), echoModel)
			if err != nil {
				t.Fatal(err)
			}
			if err := live.Exec(t.Context(), executionConfigHarness{}, tc.inputs, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			// Truncate the completed fixture to the prefix durable at the simulated crash boundary.
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("DELETE FROM events WHERE session = ? AND seq > ?", "session", tc.cut); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("session")
			runs, models, tools := 0, 0, 0
			var starts []api.Start
			recovery, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				models++
				return api.ModelResponse{Message: *api.TextMessage("assistant", "reply")}, nil
			}, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				tools++
				return api.ToolResult{ID: "t1"}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			har := checkedEffectsHarness{executionConfigHarness{&starts}, func(*api.Start) { runs++ }}
			if !tc.valid {
				assertIncompleteInvocation(t, log, recovery, har)
				if runs != 0 || models != 0 || tools != 0 {
					t.Errorf("incomplete snapshot ran harness/model/tool: %d/%d/%d", runs, models, tools)
				}
			} else {
				if resumed, err := recovery.Resume(t.Context(), har); err != nil || !resumed {
					t.Fatalf("complete invocation Resume = %v, %v", resumed, err)
				}
				if runs != 1 || models != 1 || tools != 1 || !reflect.DeepEqual(messageTexts(starts[0].Inputs), messageTexts(tc.inputs)) {
					t.Fatalf("complete invocation not restored: runs=%d model=%d tool=%d starts=%#v", runs, models, tools, starts)
				}
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReconstructionRejectsInvalidInputCountsBeforeHarness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  *int64
		inputs []*api.Message
	}{
		{"absent with input", nil, []*api.Message{api.TextMessage("user", "hi")}},
		{"absent inputless", nil, nil},
		{"negative", inputCount(-1), nil},
		{"missing single", inputCount(1), nil},
		{"partial multiple", inputCount(2), []*api.Message{api.TextMessage("user", "hi")}},
		{"excess", inputCount(0), []*api.Message{api.TextMessage("user", "hi")}},
		{"missing payload", inputCount(1), []*api.Message{nil}},
		{"unexpected missing payload", inputCount(0), []*api.Message{nil}},
	} {
		for _, completed := range []bool{false, true} {
			t.Run(tc.name+"/completed-"+strconv.FormatBool(completed), func(t *testing.T) {
				log := eventlog.AsStore(eventlog.New())
				// A valid completed prefix must not run before a malformed completed turn is found.
				live, err := controller.New(log, echoModel)
				if err != nil {
					t.Fatal(err)
				}
				if err := live.Exec(t.Context(), executionConfigHarness{}, []api.Message{msg("prefix")}, 0); err != nil {
					t.Fatal(err)
				}
				head, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				fence, err := log.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				events := []api.Event{{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: tc.count}}}
				for _, message := range tc.inputs {
					events = append(events, api.Event{Kind: api.EventInput, Message: message})
				}
				if completed {
					events = append(events, api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
				}
				for _, event := range events {
					event.ExecutionID = "invalid"
					if _, err := log.Append(head, fence, event); err != nil {
						t.Fatal(err)
					}
					head++
				}
				c, err := controller.New(log, echoModel)
				if err != nil {
					t.Fatal(err)
				}
				runs := 0
				har := checkedStartHarness{executionConfigHarness{}, func(*api.Start) { runs++ }}
				assertIncompleteInvocation(t, log, c, har)
				before, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Replay(t.Context(), har); completed && !errors.Is(err, controller.ErrInvalidExecutionLog) || !completed && err != nil {
					t.Errorf("completed=%v Replay = %v", completed, err)
				}
				wantRuns := 0
				if !completed {
					wantRuns = 1 // completed replay skips the malformed trailing invocation
				}
				if runs != wantRuns {
					t.Errorf("harness runs = %d, want %d", runs, wantRuns)
				}
				after, err := log.Read(1)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("Replay changed log: %v", err)
				}
			})
		}
	}
}

func inputCount(n int64) *int64 { return &n }

func TestReplayCompletedPrefixSkipsIncompleteTrailingInvocation(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	live, err := controller.New(log, echoModel, controller.WithStart([]byte("opaque"), 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(t.Context(), executionConfigHarness{}, []api.Message{msg("complete")}, 0); err != nil {
		t.Fatal(err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	failing := &failInputStore{Store: log, failAt: 1, err: errors.New("INPUT unavailable")}
	second, err := controller.New(failing, echoModel, controller.WithStart([]byte("opaque"), 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Exec(t.Context(), executionConfigHarness{}, []api.Message{msg("missing")}, head); err == nil {
		t.Fatal("expected INPUT failure")
	}
	replay, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	if _, err := replay.Replay(t.Context(), executionConfigHarness{&starts}); err != nil {
		t.Fatalf("completed prefix Replay = %v", err)
	}
	if len(starts) != 1 || !reflect.DeepEqual(messageTexts(starts[0].Inputs), []string{"complete"}) {
		t.Fatalf("Replay ran wrong invocations: %#v", starts)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("Replay changed log: %v", err)
	}
}
