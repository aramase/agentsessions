package placement_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

type identityToolHarness struct {
	version string
	runs    atomic.Int32
}

func (h *identityToolHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "connected-id", Version: h.version, Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h *identityToolHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	return (hostToolHarness{key: "original-key"}).Run(ctx, start, sink)
}

type declaredIdentityBackend struct {
	placement.Backend
}

func (declaredIdentityBackend) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "configured-id", Version: "configured-version", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func TestPlacerRecordsConnectedVersionAndPerCallHarness(t *testing.T) {
	for _, tc := range []struct{ name, harness, version, wantHarness string }{
		{"registry alias", "logical-alias", "served-v1", "logical-alias"},
		{"standalone descriptor fallback", "", "served-v1", "connected-id"},
		{"version opt out", "logical-alias", "", "logical-alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			h := &identityToolHarness{version: tc.version}
			backend := local.New(h)
			t.Cleanup(func() { _ = backend.Close() })
			p := placement.New(declaredIdentityBackend{backend}, echoModelForIdentity,
				placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					return api.ToolResult{Output: map[string]any{"receipt": "receipt"}}, nil
				}))
			log := store.Session("identity")
			if _, err := p.Exec(t.Context(), log, "identity", nil, 0, placement.WithHarness(tc.harness)); err != nil {
				t.Fatal(err)
			}
			recs := toolRecords(t, log)
			marker := toolEvent(t, recs, api.EventExecutionStart).ExecutionStart
			if marker == nil || marker.Harness != tc.wantHarness || marker.HarnessVersion != tc.version {
				t.Fatalf("marker=%+v, want connected identity %q/%q, not backend declaration", marker, tc.wantHarness, tc.version)
			}
		})
	}
}

func echoModelForIdentity(context.Context, api.ModelRequest) (api.ModelResponse, error) {
	return api.ModelResponse{}, errors.New("unexpected model call")
}

func TestPlacerResumeIdentityChecksUseConnectedHarness(t *testing.T) {
	for _, tc := range []struct {
		name, selectedName, servedVersion string
		wantErr                           error
	}{
		{"correct alias and version", "recorded-alias", "recorded-v1", nil},
		{"wrong selected alias", "another-alias", "recorded-v1", controller.ErrHarnessMismatch},
		{"changed connected version", "recorded-alias", "served-v2", controller.ErrHarnessVersionMismatch},
		{"empty connected version", "recorded-alias", "", controller.ErrHarnessVersionMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("identity")
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			call := hostToolCall("original-key")
			events := []api.Event{
				{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "recorded-alias", HarnessVersion: "recorded-v1"}},
				{Kind: api.EventInput, Message: api.TextMessage("user", "charge")},
				{Kind: api.EventToolCall, ToolCall: &call},
			}
			for i, ev := range events {
				ev.ExecutionID = "pending"
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			before := toolRecords(t, log)
			h := &identityToolHarness{version: tc.servedVersion}
			backend := local.New(h)
			t.Cleanup(func() { _ = backend.Close() })
			var models, tools atomic.Int32
			p := placement.New(declaredIdentityBackend{backend}, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				models.Add(1)
				return api.ModelResponse{}, nil
			}, placement.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, got api.ToolCall) (api.ToolResult, error) {
				tools.Add(1)
				if scope.SessionUID != "identity" || !reflect.DeepEqual(got, call) {
					return api.ToolResult{}, errors.New("lost recovery scope or original tool key")
				}
				return api.ToolResult{Output: map[string]any{"receipt": "recovered"}}, nil
			}))
			err = p.Resume(t.Context(), log, "identity", placement.WithResumeHarness(tc.selectedName))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Resume=%v want %v", err, tc.wantErr)
				}
				if h.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || !reflect.DeepEqual(before, toolRecords(t, log)) {
					t.Fatal("rejected Resume ran harness/model/tool or changed journal")
				}
				return
			}
			if err != nil || h.runs.Load() != 1 || models.Load() != 0 || tools.Load() != 1 {
				t.Fatalf("matching Resume: err=%v Run/model/tool=%d/%d/%d", err, h.runs.Load(), models.Load(), tools.Load())
			}
		})
	}
}

// A substrate descriptor describes configured placement needs, not the version in restored RAM.
// Refusing its stale declaration would prevent a compatible connected harness from recovering.
func TestPlacerResumeDoesNotVersionCheckConfiguredMemoryDescriptor(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("memory-identity")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	ref := api.SnapshotRef{Local: "memory-identity", ExternalURI: "original-memory", Memory: true}
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionID: "pending", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0), Harness: "memory-alias", HarnessVersion: "restored-v1"}},
		{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}},
	}
	for i, ev := range events {
		if _, err := log.Append(int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
	}
	h := &identityToolHarness{version: "restored-v1"}
	var closes, tools atomic.Int32
	ctl := &cloningControl{}
	backend := substrate.New(ctl, "space", substrate.ObjectRef{Name: "new-image"}, api.Descriptor{
		ID: "configured-id", Version: "new-image-v2", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot},
	})
	p := placement.New(backend, echoModelForIdentity,
		placement.WithDialer(func(string) (api.Harness, func() error, error) {
			return h, func() error { closes.Add(1); return nil }, nil
		}),
		placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
			tools.Add(1)
			return api.ToolResult{Output: map[string]any{"receipt": "restored"}}, nil
		}))
	if err := p.Resume(t.Context(), log, "memory-identity", placement.WithResumeHarness("memory-alias")); err != nil {
		t.Fatalf("compatible restored version refused because configured image differs: %v", err)
	}
	if h.runs.Load() != 1 || tools.Load() != 1 || closes.Load() != 1 {
		t.Fatalf("restored recovery Run/tool/close=%d/%d/%d", h.runs.Load(), tools.Load(), closes.Load())
	}
	if !reflect.DeepEqual(ctl.calls, []string{"resume:memory-identity:boot=false"}) {
		t.Fatalf("memory recovery did not restore original snapshot: %v", ctl.calls)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}
