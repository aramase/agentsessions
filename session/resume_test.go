package session_test

import (
	"context"
	"io"
	"path/filepath"
	"reflect"
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

func TestResumeInvalidExecutionLogDoesNotAssumeIncompleteInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
		detail string
	}{
		{
			name:   "missing execution identity",
			events: []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}},
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
			if status.Code(err) != codes.Internal {
				t.Errorf("invalid-log Resume = %v, want Internal", err)
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
