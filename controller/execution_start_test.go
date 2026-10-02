package controller_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
)

// executionConfigHarness makes both opaque start values affect the real model request, so dropping
// either during reconstruction fails the controller's I0 check rather than just a capture assertion.
type executionConfigHarness struct {
	starts *[]api.Start
}

func (executionConfigHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "execution-config", Capabilities: api.Capabilities{
		Resumability: api.ResumabilityStatelessReplay, ForkSafe: true,
	}}, nil
}

func (h executionConfigHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.starts != nil {
		captured := *start
		captured.Config = append([]byte(nil), start.Config...)
		captured.History = append([]api.Event{}, start.History...)
		captured.Inputs = append([]api.Message{}, start.Inputs...)
		*h.starts = append(*h.starts, captured)
	}
	messages := []api.Message{}
	for _, event := range start.History {
		if event.Message != nil && (event.Kind == api.EventInput || event.Kind == api.EventOutput) {
			messages = append(messages, *event.Message)
		}
	}
	messages = append(messages, start.Inputs...)
	_, err := sink.Model(ctx, api.ModelRequest{
		Model: "config-sensitive", Messages: messages,
		Params: map[string]string{
			"config": base64.StdEncoding.EncodeToString(start.Config),
			"cursor": strconv.FormatInt(start.ResumeFromSeq, 10),
		},
	})
	return err
}

func TestReplayRestoresExecutionConfigAfterSQLiteReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var liveStarts []api.Start
	configs := [][]byte{{0, 255, ' ', '\n', '\t'}, []byte("  {\"model\":\"other\"} \r\n")}
	for i, config := range configs {
		log := store.Session("session")
		live, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
			return api.ModelResponse{Message: *api.TextMessage("assistant", "recorded reply")}, nil
		}, controller.WithStart(config, int64(17+i)))
		if err != nil {
			t.Fatal(err)
		}
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		if err := live.Exec(t.Context(), executionConfigHarness{starts: &liveStarts}, []api.Message{*api.TextMessage("user", "same")}, head); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("session")
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruction must use the journal, not the new controller's caller-supplied values.
	replay, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		t.Fatal("replay invoked a model")
		return api.ModelResponse{}, nil
	}, controller.WithStart([]byte("wrong"), 999))
	if err != nil {
		t.Fatal(err)
	}
	var replayStarts []api.Start
	outputs, err := replay.Replay(t.Context(), executionConfigHarness{starts: &replayStarts})
	if err != nil {
		t.Fatalf("replay config-sensitive harness: %v", err)
	}
	if !reflect.DeepEqual(outputs, []string{"recorded reply", "recorded reply"}) {
		t.Fatalf("outputs = %v", outputs)
	}
	if !reflect.DeepEqual(replayStarts, liveStarts) {
		t.Fatalf("reconstructed starts differ\nlive: %#v\nreplay: %#v", liveStarts, replayStarts)
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("replay changed the journal")
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
	// Both a historical completed prefix and HEAD must retain each inherited turn's values.
	for _, count := range []int{1, 2} {
		atSeq := int64(0)
		ends := 0
		for _, rec := range before {
			if rec.Event.Kind == api.EventEnd {
				ends++
				if ends == count {
					atSeq = rec.Seq
					break
				}
			}
		}
		child := store.Session("child-" + strconv.Itoa(count))
		if err := controller.Fork(log, child, atSeq); err != nil {
			t.Fatal(err)
		}
		forkReplay, err := controller.New(child, nil)
		if err != nil {
			t.Fatal(err)
		}
		var forkStarts []api.Start
		if _, err := forkReplay.Replay(t.Context(), executionConfigHarness{starts: &forkStarts}); err != nil {
			t.Fatalf("fork at %d: %v", atSeq, err)
		}
		if !reflect.DeepEqual(forkStarts, liveStarts[:count]) {
			t.Fatalf("fork at %d changed inherited starts", atSeq)
		}
		childRecords, err := child.Read(1)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(atSeq); i++ {
			if childRecords[i].Hash != before[i].Hash {
				t.Fatalf("fork changed prefix hash at %d", i+1)
			}
		}
		if err := child.Verify(); err != nil {
			t.Fatal(err)
		}
	}
}

// interruptedConfigHarness loses the process after durable model/tool results, but before its
// final output. Recovery must serve the results once and then record only the missing output.
type interruptedConfigHarness struct {
	executionConfigHarness
	interrupt bool
}

func (h interruptedConfigHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if err := h.executionConfigHarness.Run(ctx, start, sink); err != nil {
		return err
	}
	if _, err := sink.ToolCall(ctx, api.ToolCall{
		ID: "t1", Tool: "effect", Mediation: api.MediationControllerMediated, IdempotencyKey: "once",
	}); err != nil {
		return err
	}
	if h.interrupt {
		return errors.New("interrupted")
	}
	return sink.Output(ctx, "continued")
}

func TestResumeRestoresExecutionStartAfterSQLiteReopen(t *testing.T) {
	for _, tc := range []struct {
		name       string
		withInputs bool
		config     []byte
		cursor     int64
	}{
		{"inputful config", true, []byte{255, 0, '\n', '\t', ' '}, -7},
		{"inputless config", false, []byte{255, 0, '\n', '\t', ' '}, -7},
		{"legacy defaults", true, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store, err := sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			modelCalls, toolCalls := 0, 0
			model := func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				modelCalls++
				return api.ModelResponse{Message: *api.TextMessage("assistant", "recorded")}, nil
			}
			tool := func(context.Context, api.ToolCall) (api.ToolResult, error) {
				toolCalls++
				return api.ToolResult{ID: "t1", Output: map[string]any{"ok": true}}, nil
			}
			log := store.Session("session")
			live, err := controller.New(log, model, controller.WithStart(tc.config, tc.cursor), controller.WithToolExecutor(tool))
			if err != nil {
				t.Fatal(err)
			}
			var inputs []api.Message
			if tc.withInputs {
				inputs = []api.Message{msg("hello")}
			}
			var liveStarts []api.Start
			if err := live.Exec(t.Context(), interruptedConfigHarness{executionConfigHarness{&liveStarts}, true}, inputs, 0); err == nil {
				t.Fatal("expected interruption")
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
			resume, err := controller.New(log, model, controller.WithStart([]byte("wrong"), 999), controller.WithToolExecutor(tool))
			if err != nil {
				t.Fatal(err)
			}
			var resumedStarts []api.Start
			har := interruptedConfigHarness{executionConfigHarness{&resumedStarts}, false}
			if resumed, err := resume.Resume(t.Context(), har); err != nil || !resumed {
				t.Fatalf("Resume = %v, %v", resumed, err)
			}
			if !reflect.DeepEqual(liveStarts, resumedStarts) {
				t.Fatalf("resume changed Start\nlive: %#v\nresume: %#v", liveStarts, resumedStarts)
			}
			if modelCalls != 1 || toolCalls != 1 {
				t.Fatalf("effects ran model=%d tool=%d times; want 1/1", modelCalls, toolCalls)
			}
			if outputs, err := resume.Outputs(); err != nil || !reflect.DeepEqual(outputs, []string{"recorded", "continued"}) {
				t.Fatalf("outputs = %v, %v", outputs, err)
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if resumed, err := resume.Resume(t.Context(), har); err != nil || resumed {
				t.Fatalf("second Resume = %v, %v; want no-op", resumed, err)
			}
			if _, err := resume.Replay(t.Context(), har); err != nil {
				t.Fatal(err)
			}
			after, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || modelCalls != 1 || toolCalls != 1 {
				t.Fatal("completed recovery/replay changed the journal or repeated effects")
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type checkedStartHarness struct {
	executionConfigHarness
	beforeRun func(*api.Start)
}

func (h checkedStartHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.beforeRun(start)
	return h.executionConfigHarness.Run(ctx, start, sink)
}

func TestExecutionStartBoundaryAndLegacyDefaults(t *testing.T) {
	cases := []struct {
		name       string
		config     []byte
		cursor     int64
		inputs     []api.Message
		wantMarker bool
	}{
		{"legacy defaults", nil, 0, []api.Message{msg("hello")}, false},
		{"empty config", []byte{}, 0, []api.Message{msg("hello")}, false},
		{"config only", []byte(" \t\nprivate-config"), 0, []api.Message{msg("hello")}, true},
		{"cursor only", nil, 23, []api.Message{msg("hello")}, true},
		{"inputless defaults", nil, 0, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := eventlog.AsStore(eventlog.New())
			var operational bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&operational, &slog.HandlerOptions{Level: slog.LevelDebug}))
			var observed []eventlog.Record
			live, err := controller.New(log, echoModel, controller.WithStart(tc.config, tc.cursor), controller.WithLogger(logger),
				controller.WithObserver(controller.Observer{OnRecord: func(r eventlog.Record) { observed = append(observed, r) }}))
			if err != nil {
				t.Fatal(err)
			}
			var liveStarts []api.Start
			har := checkedStartHarness{executionConfigHarness{&liveStarts}, func(start *api.Start) {
				recs, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				if len(recs) == 0 || !reflect.DeepEqual(observed, recs) {
					t.Fatal("harness ran before first durable/observed record")
				}
				if got := recs[0].Event.Kind == api.EventExecutionStart; got != tc.wantMarker {
					t.Fatalf("start marker = %v, want %v", got, tc.wantMarker)
				}
				if tc.wantMarker {
					body := recs[0].Event.ExecutionStart
					if body == nil || !bytes.Equal(body.Config, tc.config) || body.ResumeFromSeq != tc.cursor {
						t.Fatal("start values not durable before harness ran")
					}
				}
				if len(start.History) != 0 {
					t.Fatal("current start leaked into history")
				}
			}}
			if err := live.Exec(t.Context(), har, tc.inputs, 0); err != nil {
				t.Fatal(err)
			}
			replay, err := controller.New(log, nil, controller.WithStart([]byte("wrong"), 999))
			if err != nil {
				t.Fatal(err)
			}
			var replayStarts []api.Start
			if _, err := replay.Replay(t.Context(), executionConfigHarness{&replayStarts}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(liveStarts, replayStarts) {
				t.Fatal("reconstructed Start differs, including legacy defaults")
			}
			if strings.Contains(operational.String(), "private-config") || strings.Contains(operational.String(), "config\":") {
				t.Fatal("config leaked into operational logs")
			}
		})
	}
}

func TestExecutionStartConfigDoesNotAliasHarnessBytes(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	config := []byte("original")
	live, err := controller.New(log, echoModel, controller.WithStart(config, 3))
	if err != nil {
		t.Fatal(err)
	}
	har := checkedStartHarness{executionConfigHarness{}, func(start *api.Start) { start.Config[0] = 'X' }}
	if err := live.Exec(t.Context(), har, []api.Message{msg("hello")}, 0); err != nil {
		t.Fatal(err)
	}
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(records[0].Event.ExecutionStart.Config, []byte("original")) || string(config) != "original" {
		t.Fatal("harness mutation changed committed or caller-owned config")
	}
	if _, err := live.Replay(t.Context(), har); err != nil {
		t.Fatal(err)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("replay mutated committed config: %v", err)
	}
}

func TestForkExecutionStartOnlyPrefixCanResume(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")
	config := []byte{0, 255, ' ', '\n'}
	live, err := controller.New(parent, echoModel, controller.WithStart(config, 5))
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Exec(t.Context(), executionConfigHarness{}, []api.Message{msg("not inherited")}, 0); err != nil {
		t.Fatal(err)
	}
	// A historical prefix ending at the invocation marker is interrupted, with no inputs/effects.
	if err := controller.Fork(parent, child, 1); err != nil {
		t.Fatal(err)
	}
	resume, err := controller.New(child, echoModel)
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	if resumed, err := resume.Resume(t.Context(), executionConfigHarness{&starts}); err != nil || !resumed {
		t.Fatalf("marker-only prefix Resume = %v, %v", resumed, err)
	}
	if len(starts) != 1 || !bytes.Equal(starts[0].Config, config) || starts[0].ResumeFromSeq != 5 ||
		len(starts[0].History) != 0 || len(starts[0].Inputs) != 0 {
		t.Fatalf("marker-only resumed Start = %#v", starts)
	}
	if err := child.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestReconstructionRejectsInvalidExecutionStart(t *testing.T) {
	for _, events := range [][]api.Event{
		{{Kind: api.EventExecutionStart}},
		{{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}, {Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}}},
		{{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}}, {Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}}},
	} {
		log := eventlog.AsStore(eventlog.New())
		fence, err := log.NewFence()
		if err != nil {
			t.Fatal(err)
		}
		for i, event := range events {
			event.ExecutionID = "invalid"
			if _, err := log.Append(int64(i), fence, event); err != nil {
				t.Fatal(err)
			}
		}
		reconstruct, err := controller.New(log, nil)
		if err != nil {
			t.Fatal(err)
		}
		har := checkedStartHarness{executionConfigHarness{}, func(*api.Start) { t.Fatal("ran harness on invalid start") }}
		if _, err := reconstruct.Replay(t.Context(), har); !errors.Is(err, controller.ErrInvalidExecutionLog) {
			t.Fatalf("Replay error = %v, want invalid start", err)
		}
		if _, err := reconstruct.Resume(t.Context(), har); !errors.Is(err, controller.ErrInvalidExecutionLog) {
			t.Fatalf("Resume error = %v, want invalid start", err)
		}
	}
}

type rejectAppendStore struct {
	eventlog.Store
	err error
}

func (s rejectAppendStore) Append(int64, int64, api.Event) (eventlog.Record, error) {
	return eventlog.Record{}, s.err
}

func TestExecutionStartFailedFirstAppendNeverRunsHarness(t *testing.T) {
	for _, values := range []struct {
		name   string
		config []byte
		cursor int64
		inputs []api.Message
	}{
		{"config", []byte("opaque"), 0, []api.Message{msg("hello")}},
		{"cursor", nil, 3, []api.Message{msg("hello")}},
		{"inputless", nil, 0, nil},
		{"default", nil, 0, []api.Message{msg("hello")}},
	} {
		for _, failure := range []string{"CAS", "fence", "storage"} {
			t.Run(values.name+"/"+failure, func(t *testing.T) {
				log := eventlog.AsStore(eventlog.New())
				modelCalls, runs, observed := 0, 0, 0
				wantErr := errors.New("disk unavailable")
				if failure == "storage" {
					log = rejectAppendStore{log, wantErr}
				}
				live, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
					modelCalls++
					return api.ModelResponse{}, nil
				}, controller.WithStart(values.config, values.cursor), controller.WithObserver(controller.Observer{
					OnRecord: func(eventlog.Record) { observed++ },
				}))
				if err != nil {
					t.Fatal(err)
				}
				expected := int64(0)
				if failure == "CAS" {
					expected = 1
					wantErr = eventlog.ErrConflict
				} else if failure == "fence" {
					if _, err := log.NewFence(); err != nil {
						t.Fatal(err)
					}
					wantErr = eventlog.ErrFenced
				}
				har := checkedStartHarness{executionConfigHarness{}, func(*api.Start) { runs++ }}
				if err := live.Exec(t.Context(), har, values.inputs, expected); !errors.Is(err, wantErr) {
					t.Fatalf("Exec = %v, want %v", err, wantErr)
				}
				head, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				if head != 0 || runs != 0 || modelCalls != 0 || observed != 0 {
					t.Fatalf("failed first append: head=%d runs=%d model=%d observed=%d", head, runs, modelCalls, observed)
				}
			})
		}
	}
}
