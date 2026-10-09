package session_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

func TestResumeIncompleteInvocationCanExecAgain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inputCount int64
		inputs     []*api.Message
	}{
		{"missing input", 1, nil},
		{"partial inputs", 2, []*api.Message{api.TextMessage("user", "first")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store := openStore(t, path)
			const uid = "incomplete"
			if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "echo"}); err != nil {
				t.Fatal(err)
			}
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			// This is the durable prefix left by a crash before all INPUT appends commit.
			events := []api.Event{{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(tc.inputCount)}}}
			for _, input := range tc.inputs {
				events = append(events, api.Event{Kind: api.EventInput, Message: input})
			}
			for i, event := range events {
				event.ExecutionID = "interrupted"
				if _, err := log.Append(int64(i), fence, event); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openStore(t, path)
			log = store.Session(uid)
			client := serve(t, store)
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}

			_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("incomplete Resume = %v, want FailedPrecondition", err)
			}
			message := status.Convert(err).Message()
			for _, hint := range []string{"never reached the harness", "Exec again", "inputs"} {
				if !strings.Contains(message, hint) {
					t.Errorf("Resume message %q lacks recovery guidance %q", message, hint)
				}
			}
			after, err := log.Read(1)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected Resume changed journal: %v", err)
			}

			if outputs := execOutputs(t, client, uid, "retry", int64(len(before))); !reflect.DeepEqual(outputs, []string{"echo:retry"}) {
				t.Fatalf("Exec after rejected Resume outputs = %v", outputs)
			}
			committed, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := client.Replay(t.Context(), &v1.ReplayRequest{Session: uid})
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range committed {
				got, err := replayed.Recv()
				if err != nil || !proto.Equal(got, eventlog.RecordToProto(record)) {
					t.Fatalf("Replay record %d changed: %v, %v", record.Seq, got, err)
				}
			}
			if _, err := replayed.Recv(); err != io.EOF {
				t.Fatalf("Replay did not finish at journal head: %v", err)
			}
			// Controller replay, unlike Sessions.Replay, reconstructs completed turns. The
			// rejected prefix must stay skipped even after the new turn completes.
			recovery, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				t.Error("replay invoked the model")
				return api.ModelResponse{}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if outputs, err := recovery.Replay(t.Context(), echoagent.Harness{}); err != nil || !reflect.DeepEqual(outputs, []string{"echo:retry"}) {
				t.Fatalf("controller Replay = %v, %v", outputs, err)
			}
			if after, err := log.Read(1); err != nil || !reflect.DeepEqual(committed, after) {
				t.Fatalf("Replay changed journal: %v", err)
			}
			resumed, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if err != nil {
				t.Fatalf("Resume after Exec and Replay: %v", err)
			}
			if resumed.GetLastSeq() != int64(len(committed))+1 || resumed.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
				t.Fatalf("Resume after completed retry = %v, want one RESUME marker and LIVE compute", resumed)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// An inherited intent is a caller recovery precondition, not a retryable server failure.
func TestResumeForkedToolIntentFailedPrecondition(t *testing.T) {
	store := openStore(t, ":memory:")
	backend := local.New(registryToolHarness{key: "shared-key"})
	t.Cleanup(func() { _ = backend.Close() })
	var effects atomic.Int32
	client := serveRegistry(t, store, echoRegistry(t, backend, placement.WithToolExecutor(
		func(_ context.Context, scope controller.ToolCallContext, _ api.ToolCall) (api.ToolResult, error) {
			effects.Add(1)
			return api.ToolResult{Output: map[string]any{"receipt": scope.SessionUID}}, nil
		},
	)))
	uid := mustCreate(t, client)
	if outputs := execOutputs(t, client, uid, "charge", 0); !reflect.DeepEqual(outputs, []string{"service-call:" + uid}) {
		t.Fatalf("parent Exec outputs = %v", outputs)
	}
	parent, err := store.Session(uid).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var cut int64
	for _, record := range parent {
		if record.Event.Kind == api.EventToolCall {
			cut = record.Seq
		}
	}
	if cut == 0 {
		t.Fatal("Exec did not journal a TOOL_CALL")
	}
	fork, err := client.Fork(t.Context(), &v1.ForkRequest{Session: uid, AtSeq: cut, Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	childUID := fork.GetChildren()[0].GetMetadata().GetUid()
	child := store.Session(childUID)
	before, err := child.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: childUID})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("inherited intent Resume = %v, want FailedPrecondition", err)
	}
	if message := status.Convert(err).Message(); !strings.Contains(message, "fork at or after the TOOL_RESULT, or Exec a new turn") {
		t.Errorf("Resume message lacks recovery guidance: %q", message)
	}
	after, err := child.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected Resume changed child journal: %v", err)
	}
	if got := effects.Load(); got != 1 {
		t.Fatalf("inherited Resume repeated the parent's effect: effects=%d, want 1", got)
	}
	if outputs := execOutputs(t, client, childUID, "new charge", int64(len(before))); !reflect.DeepEqual(outputs, []string{"service-call:" + childUID}) {
		t.Fatalf("new child Exec outputs = %v", outputs)
	}
	if got := effects.Load(); got != 2 {
		t.Fatalf("new child Exec effects=%d, want 2", got)
	}
	if err := child.Verify(); err != nil {
		t.Fatal(err)
	}
}

// These released-writer journals catch routing legacy recovery to another entry, changing the
// last-INPUT selection, repeating recorded effects, or treating a legacy ERROR as unfinished.
func TestResumeV012JournalThroughSessions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		runs, models int32
		input        string
		history      int
		outputs      []string
		appended     []api.EventKind
		code         codes.Code
	}{
		{name: "crashed-input", runs: 1, models: 1, input: "hello", outputs: []string{"echo:hello"}, appended: []api.EventKind{api.EventModelCall, api.EventOutput, api.EventEnd, api.EventLifecycle}},
		{name: "crashed-output", runs: 1, input: "hello", outputs: []string{"echo:hello"}, appended: []api.EventKind{api.EventEnd, api.EventLifecycle}},
		{name: "crashed-model", runs: 1, input: "hello", appended: []api.EventKind{api.EventError}, code: codes.Internal},
		{name: "multi-turn-crashed-output", runs: 1, input: "second", history: 4, outputs: []string{"echo:first", "echo:second"}, appended: []api.EventKind{api.EventEnd, api.EventLifecycle}},
		{name: "mixed-interrupted", runs: 1, input: "new", history: 4, outputs: []string{"echo:hello", "echo:new"}, appended: []api.EventKind{api.EventEnd, api.EventLifecycle}},
		{name: "completed", outputs: []string{"echo:hello"}, appended: []api.EventKind{api.EventLifecycle}},
		{name: "multi-turn", outputs: []string{"echo:first", "echo:second"}, appended: []api.EventKind{api.EventLifecycle}},
		{name: "model-error", appended: []api.EventKind{api.EventLifecycle}},
		{name: "upgraded-resume", outputs: []string{"echo:hello"}, appended: []api.EventKind{api.EventLifecycle}},
		{name: "mixed-completed", outputs: []string{"echo:hello", "echo:new"}, appended: []api.EventKind{api.EventLifecycle}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const uid = "legacy-routing-session"
			store, original := reopenedV012Session(t, tc.name, uid)
			log := store.Session(uid)
			invocation, err := controller.ResumeInvocation(log)
			if err != nil {
				t.Fatalf("fixture routing query: %v", err)
			}
			if tc.name == "mixed-interrupted" {
				if invocation == nil || invocation.Harness != "" || invocation.HarnessVersion != "" {
					t.Fatalf("old start marker must retain absent harness/version: %+v", invocation)
				}
			} else if invocation != nil {
				t.Fatalf("legacy/completed fixture invented a pending start marker: %+v", invocation)
			}

			a := &v012RoutingHarness{id: "descriptor-a", version: "current-a"}
			b := &v012RoutingHarness{id: "descriptor-b", version: "current-b"}
			var aModels, aTools, bModels, bTools atomic.Int32
			// The stored session default wins even if the host's registry default changes.
			registry, err := placement.NewRegistry("alias-b", map[string]*placement.Placer{
				"alias-a": v012RoutingPlacer(t, a, &aModels, &aTools),
				"alias-b": v012RoutingPlacer(t, b, &bModels, &bTools),
			})
			if err != nil {
				t.Fatal(err)
			}
			client := serveRegistry(t, store, registry)
			resumed, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if status.Code(err) != tc.code {
				t.Fatalf("Sessions Resume = %v, want %v", err, tc.code)
			}
			if tc.code == codes.Internal && (!strings.Contains(status.Convert(err).Message(), "recorded completion missing") || strings.Contains(status.Convert(err).Message(), "Exec again")) {
				t.Fatalf("missing completion lost historical failure classification: %v", err)
			}
			if a.runs.Load() != tc.runs || aModels.Load() != tc.models || aTools.Load() != 0 || b.runs.Load() != 0 || bModels.Load() != 0 || bTools.Load() != 0 {
				t.Fatalf("wrong recovery route/effects: A Run/model/tool=%d/%d/%d B=%d/%d/%d; want A=%d/%d/0 B=0/0/0",
					a.runs.Load(), aModels.Load(), aTools.Load(), b.runs.Load(), bModels.Load(), bTools.Load(), tc.runs, tc.models)
			}
			if tc.runs == 1 {
				start := <-a.starts
				if len(start.Inputs) != 1 || start.Inputs[0].Role != "user" || start.Inputs[0].Text() != tc.input || start.Config != nil || start.ResumeFromSeq != 0 {
					t.Fatalf("recovered Start changed original invocation or inserted Role.Context: %+v", start)
				}
				var history []api.Event
				for _, record := range original[:tc.history] {
					history = append(history, record.Event)
				}
				if len(start.History) != tc.history || tc.history > 0 && !reflect.DeepEqual(start.History, history) {
					t.Fatalf("recovered Start lost original last-INPUT history: %+v", start)
				}
				if tc.name == "mixed-interrupted" {
					if start.ExecutionID != routingEvent(t, original, api.EventExecutionStart).ExecutionID {
						t.Fatalf("mixed recovery changed recorded execution ID: %q", start.ExecutionID)
					}
				} else if !regexp.MustCompile(`^legacy-[0-9a-f]{64}$`).MatchString(start.ExecutionID) {
					t.Fatalf("legacy Start lacks scoped compatibility identity: %q", start.ExecutionID)
				}
			}

			after := routingRecords(t, log)
			if len(after) != len(original)+len(tc.appended) {
				t.Fatalf("Resume record count = %d, want %d", len(after), len(original)+len(tc.appended))
			}
			assertV012Prefix(t, original, after)
			var outputs []string
			for _, record := range after {
				if record.Event.Kind == api.EventOutput && record.Event.Message != nil {
					outputs = append(outputs, record.Event.Message.Text())
				}
			}
			if !reflect.DeepEqual(outputs, tc.outputs) {
				t.Fatalf("recovered outputs = %v, want %v", outputs, tc.outputs)
			}
			for i, kind := range tc.appended {
				ev := after[len(original)+i].Event
				if ev.Kind != kind {
					t.Fatalf("continuation event %d = %s, want %s", i, ev.Kind, kind)
				}
				wantID := ""
				if tc.name == "mixed-interrupted" && kind != api.EventLifecycle {
					wantID = routingEvent(t, original, api.EventExecutionStart).ExecutionID
				}
				if ev.ExecutionID != wantID {
					t.Fatalf("continuation event %s execution ID = %q, want %q", kind, ev.ExecutionID, wantID)
				}
				switch kind {
				case api.EventEnd:
					if ev.End == nil || ev.End.State != "COMPLETED" {
						t.Fatalf("recovery did not finish with COMPLETED END: %+v", ev)
					}
				case api.EventError:
					if ev.Err == nil || ev.Err.Description != "resume: recorded completion missing" {
						t.Fatalf("missing completion ERROR changed: %+v", ev)
					}
				case api.EventLifecycle:
					if ev.Lifecycle == nil || ev.Lifecycle.Kind != api.LifecycleResume {
						t.Fatalf("completed recovery did not append RESUME: %+v", ev)
					}
				}
			}
			if tc.code == codes.OK && (resumed.GetHarness() != "alias-a" || resumed.GetLastSeq() != int64(len(after)) || resumed.GetComputeState() != v1.ComputeState_COMPUTE_LIVE) {
				t.Fatalf("Resume changed stored default or lost lifecycle: %v", resumed)
			}
			info, err := store.SessionInfo(uid)
			if err != nil || info.Harness != "alias-a" {
				t.Fatalf("Resume mutated Session.Harness: %+v, %v", info, err)
			}
			// Sessions.Replay remains a raw read, even for a durable missing-completion ERROR.
			stream, err := client.Replay(t.Context(), &v1.ReplayRequest{Session: uid})
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range after {
				got, err := stream.Recv()
				if err != nil || !proto.Equal(got, eventlog.RecordToProto(record)) {
					t.Fatalf("raw Replay changed record %d: %v, %v", record.Seq, got, err)
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatalf("raw Replay did not stop at journal head: %v", err)
			}
			// A successful retry must release the registry guard; legacy ERROR is finished too.
			if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
				t.Fatalf("completed second Resume: %v", err)
			}
			finished := routingRecords(t, log)
			if len(finished) != len(after)+1 || finished[len(after)].Event.Lifecycle == nil || finished[len(after)].Event.Lifecycle.Kind != api.LifecycleResume {
				t.Fatalf("completed second Resume must append only RESUME: %+v", finished)
			}
			assertV012Prefix(t, after, finished)
			if a.runs.Load() != tc.runs || aModels.Load() != tc.models || aTools.Load() != 0 || b.runs.Load() != 0 || bModels.Load() != 0 || bTools.Load() != 0 {
				t.Fatal("raw Replay or completed second Resume repeated execution effects")
			}
		})
	}
}

func reopenedV012Session(t *testing.T, name, uid string) (*sqlitelog.Store, []eventlog.Record) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "controller", "testdata", "v0.1.2", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var original []eventlog.Record
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	if err := eventlog.NewFrom(original).Verify(); err != nil {
		t.Fatalf("released-writer fixture chain: %v", err)
	}
	path := filepath.Join(t.TempDir(), "journal.db")
	store := openStore(t, path)
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range original {
		for fence < record.Fence {
			fence, err = log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
		}
		got, err := log.Append(record.Seq-1, fence, record.Event)
		if err != nil || !reflect.DeepEqual(record, got) {
			t.Fatalf("copy original record %d: %#v, %v", record.Seq, got, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, path)
	assertV012Prefix(t, original, routingRecords(t, store.Session(uid)))
	return store, original
}

func assertV012Prefix(t *testing.T, original, after []eventlog.Record) {
	t.Helper()
	if len(after) < len(original) || !reflect.DeepEqual(original, after[:len(original)]) {
		t.Fatal("recovery rewrote original records, fences or hashes")
	}
	beforeBytes, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	afterBytes, err := json.Marshal(after[:len(original)])
	if err != nil || !bytes.Equal(beforeBytes, afterBytes) {
		t.Fatalf("recovery changed original encoded journal bytes: %v", err)
	}
}

type v012RoutingHarness struct {
	id, version string
	runs        atomic.Int32
	starts      chan api.Start
}

func (h *v012RoutingHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	descriptor, err := (echoagent.Harness{}).Describe(ctx)
	descriptor.ID, descriptor.Version = h.id, h.version
	return descriptor, err
}

func (h *v012RoutingHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	h.starts <- *start
	return (echoagent.Harness{}).Run(ctx, start, sink)
}

func v012RoutingPlacer(t *testing.T, h *v012RoutingHarness, models, tools *atomic.Int32) *placement.Placer {
	t.Helper()
	h.starts = make(chan api.Start, 2)
	backend := local.New(h)
	t.Cleanup(func() { _ = backend.Close() })
	return placement.New(backend, func(ctx context.Context, request api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		return echoagent.Model(ctx, request)
	}, placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
		tools.Add(1)
		return api.ToolResult{}, nil
	}))
}

func TestResumeLegacyForkedToolIntentGuidance(t *testing.T) {
	store := openStore(t, ":memory:")
	client := serve(t, store)
	uid := mustCreate(t, client)
	log := store.Session(uid)
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	call := api.ToolCall{ID: "inherited", Tool: "charge", Mediation: api.MediationControllerMediated, IdempotencyKey: "key"}
	for i, event := range []api.Event{
		{Kind: api.EventInput, Message: api.TextMessage("user", "charge")},
		{Kind: api.EventToolCall, ToolCall: &call},
		{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: "parent@2"}},
	} {
		if _, err := log.Append(int64(i), fence, event); err != nil {
			t.Fatal(err)
		}
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy inherited Resume = %v", err)
	}
	if message := status.Convert(err).Message(); !strings.Contains(message, "Exec a new turn") || strings.Contains(message, "only for ID-bearing") {
		t.Fatalf("legacy guidance must allow an explicit modern boundary: %q", message)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy rejection changed journal: %v", err)
	}
	if outputs := execOutputs(t, client, uid, "new turn", int64(len(before))); !reflect.DeepEqual(outputs, []string{"echo:new turn"}) {
		t.Fatalf("Exec after inherited legacy intent = %v", outputs)
	}
}

func TestResumeInvalidExecutionLogDoesNotAssumeIncompleteInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
		detail string
	}{
		{
			name: "missing execution identity",
			events: []api.Event{
				{Kind: api.EventExecutionStart, ExecutionID: "interrupted", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1)}},
				{Kind: api.EventInput, Message: api.TextMessage("user", "hi")},
			},
			detail: "no execution_id",
		},
		{
			name: "count-short completed turn",
			events: []api.Event{
				{Kind: api.EventExecutionStart, ExecutionID: "completed", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1)}},
				{Kind: api.EventEnd, ExecutionID: "completed", End: &api.HarnessEnd{State: "COMPLETED"}},
			},
			detail: "expected 1 INPUT events, committed 0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			client := serve(t, store)
			uid := mustCreate(t, client)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, event := range tc.events {
				if _, err := log.Append(int64(i), fence, event); err != nil {
					t.Fatal(err)
				}
			}
			_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("invalid-log Resume = %v, want FailedPrecondition", err)
			}
			message := status.Convert(err).Message()
			if !strings.Contains(message, tc.detail) {
				t.Errorf("Resume lost invalid-log detail: %q", message)
			}
			if strings.Contains(message, "Exec again") || strings.Contains(message, "never reached the harness") {
				t.Errorf("invalid log received incomplete-input guidance: %q", message)
			}
		})
	}
}
