package session_test

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

type routingHarness struct {
	id      string
	version string
	runs    atomic.Int32
}

func (h *routingHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: h.id, Version: h.version, Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h *routingHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	result, err := sink.ToolCall(ctx, routingToolCall())
	if err != nil {
		return err
	}
	return sink.Output(ctx, fmt.Sprintf("%s:%v", result.ID, result.Output["receipt"]))
}

func routingToolCall() api.ToolCall {
	return api.ToolCall{ID: "routing-call", Tool: "charge", Args: map[string]any{"account": "a1"},
		Mediation: api.MediationControllerMediated, IdempotencyKey: "original-routing-key"}
}

func routingPlacer(t *testing.T, h *routingHarness, receipt string, models, tools *atomic.Int32) *placement.Placer {
	t.Helper()
	backend := local.New(h)
	t.Cleanup(func() { _ = backend.Close() })
	return placement.New(backend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		return api.ModelResponse{}, nil
	}, placement.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		tools.Add(1)
		if scope.SessionUID != "routing-session" || !reflect.DeepEqual(call, routingToolCall()) {
			return api.ToolResult{}, fmt.Errorf("wrong routed tool scope/call: %+v %+v", scope, call)
		}
		return api.ToolResult{Output: map[string]any{"receipt": receipt}}, nil
	}))
}

func routingRegistry(t *testing.T, entries map[string]*placement.Placer) *placement.Registry {
	t.Helper()
	r, err := placement.NewRegistry("alias-a", entries)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Resolving Resume only from session metadata routes the pending B tool intent to A's executor.
// Registry aliases deliberately differ from Describe.ID, so descriptor-ID lookup cannot pass.
func TestResumeRecordedOverrideAfterServiceRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store := openStore(t, path)
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	seedRoutingInvocation(t, store.Session(uid), &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b"}, false)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, path)
	a, b := &routingHarness{id: "descriptor-a"}, &routingHarness{id: "descriptor-b"}
	var aModels, aTools, bModels, bTools atomic.Int32
	client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{
		"alias-a": routingPlacer(t, a, "receipt-a", &aModels, &aTools),
		"alias-b": routingPlacer(t, b, "receipt-b", &bModels, &bTools),
	}))
	if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("resume recorded B invocation: %v", err)
	}
	if a.runs.Load() != 0 || aModels.Load() != 0 || aTools.Load() != 0 || b.runs.Load() != 1 || bModels.Load() != 0 || bTools.Load() != 1 {
		t.Fatalf("Resume used wrong execution path: A Run/model/tool=%d/%d/%d, B=%d/%d/%d; want A=0/0/0 B=1/0/1",
			a.runs.Load(), aModels.Load(), aTools.Load(), b.runs.Load(), bModels.Load(), bTools.Load())
	}
	recs, err := store.Session(uid).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	result := routingEvent(t, recs, api.EventToolResult).Result
	output := routingEvent(t, recs, api.EventOutput).Message
	if len(recs) != 7 || result == nil || result.ID != "routing-call" || output == nil || output.Text() != "routing-call:receipt-b" {
		t.Fatalf("Resume lost B receipt or correlation: %+v", recs)
	}
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
	// A one-turn override must not replace the stored default for the next omitted-harness Exec.
	stream, err := client.Exec(t.Context(), &v1.ExecRequest{Session: uid})
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if a.runs.Load() != 1 || aTools.Load() != 1 || b.runs.Load() != 1 || bTools.Load() != 1 {
		t.Fatalf("next Exec did not retain stored default: A=%d/%d B=%d/%d", a.runs.Load(), aTools.Load(), b.runs.Load(), bTools.Load())
	}
}
