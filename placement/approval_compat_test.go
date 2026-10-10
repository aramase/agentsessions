package placement_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

func TestPlacementApprovalNewExecEscapeCompatibility(t *testing.T) {
	for _, mode := range []string{"inherited unresolved", "receipt", "ordinary incomplete", "empty"} {
		t.Run(mode, func(t *testing.T) {
			store := newSuspendStore(t)
			log := store.Session("s")
			switch mode {
			case "inherited unresolved":
				parent := store.Session("parent")
				seedPlacementApproval(t, parent, "recorded", api.EventApprovalRequest)
				if err := controller.Fork(parent, log, 3); err != nil {
					t.Fatal(err)
				}
			case "receipt":
				seedPlacementApproval(t, log, "recorded", api.EventApprovalResult)
				fence, _ := log.NewFence()
				if _, err := log.Append(4, fence, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: 3, ApprovalDecisionSeq: 4}}); err != nil {
					t.Fatal(err)
				}
			case "ordinary incomplete":
				fence, _ := log.NewFence()
				if _, err := log.Append(0, fence, api.Event{ExecutionID: "old", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "unserved", InputCount: proto.Int64(1)}}); err != nil {
					t.Fatal(err)
				}
			}
			p := newLocalPlacer(t, echoagent.Harness{})
			head, _ := log.Head()
			if _, err := p.Exec(t.Context(), log, "s", nil, head); err != nil {
				t.Fatalf("new inputless Exec=%v", err)
			}
			if _, err := p.Exec(t.Context(), log, "s", []api.Message{*api.TextMessage("user", "new")}, mustApprovalHead(t, log)); err != nil {
				t.Fatalf("new input Exec=%v", err)
			}
		})
	}
}
func mustApprovalHead(t *testing.T, log eventlog.Store) int64 {
	t.Helper()
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func TestRegistryApprovalInheritedReceiptServing(t *testing.T) {
	store := newSuspendStore(t)
	parent, child := store.Session("parent"), store.Session("child")
	seedPlacementApproval(t, parent, "recorded", api.EventApprovalResult)
	fence, _ := parent.NewFence()
	if _, err := parent.Append(4, fence, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: 3, ApprovalDecisionSeq: 4}}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Fork(parent, child, 5); err != nil {
		t.Fatal(err)
	}
	p := newLocalPlacer(t, placementGateHarness{}, placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
		t.Fatal("inherited receipt redriven")
		return api.ToolResult{}, nil
	}))
	r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Resume(t.Context(), child, "child", "recorded"); err != nil {
		t.Fatalf("receipt recovery=%v", err)
	}
	placementApprovalRecord(t, child, api.EventEnd)
}

func TestRegistryApprovalSuspendOrdinaryAndMarkerlessDefault(t *testing.T) {
	for _, mode := range []string{"ordinary pending", "markerless approval", "receipt"} {
		t.Run(mode, func(t *testing.T) {
			log := newSuspendStore(t).Session("s")
			fence, _ := log.NewFence()
			switch mode {
			case "ordinary pending":
				if _, err := log.Append(0, fence, api.Event{ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "recorded", InputCount: proto.Int64(0)}}); err != nil {
					t.Fatal(err)
				}
			case "markerless approval":
				call := placementGateCall()
				if _, err := log.Append(0, fence, api.Event{ExecutionID: "e1", Kind: api.EventToolCall, ToolCall: &call}); err != nil {
					t.Fatal(err)
				}
				if _, err := log.Append(1, fence, api.Event{ExecutionID: "e1", Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}}); err != nil {
					t.Fatal(err)
				}
			case "receipt":
				seedPlacementApproval(t, log, "recorded", api.EventApprovalResult)
				fence, _ = log.NewFence()
				if _, err := log.Append(4, fence, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", ApprovalRequestSeq: 3, ApprovalDecisionSeq: 4}}); err != nil {
					t.Fatal(err)
				}
			}
			def, db := newApprovalNoIOPlacer(t)
			recorded, rb := newApprovalNoIOPlacer(t)
			r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": def, "recorded": recorded})
			if err != nil {
				t.Fatal(err)
			}
			checked := ""
			ref, err := r.Suspend(t.Context(), log, "s", "default", placement.WithResolvedHarnessCheck(func(name string) error { checked = name; return nil }))
			if err != nil || checked != "default" || ref.Local != "s" {
				t.Fatalf("default Suspend=%+v,%v route=%s", ref, err, checked)
			}
			if db.snapshots != 1 || db.describes != 0 || db.stops != 0 {
				t.Fatalf("default IO=%+v", db)
			}
			rb.assertNoCompute(t)
		})
	}
}

func TestRegistryApprovalRestrictionSurvivesReopen(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult} {
		t.Run(string(cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store, err := sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			seedPlacementApproval(t, store.Session("s"), "recorded", cut)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			log := store.Session("s")
			p, b := newApprovalNoIOPlacer(t)
			r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Exec(t.Context(), log, "s", nil, mustApprovalHead(t, log)); !errors.Is(err, controller.ErrApprovalBlocked) {
				t.Fatalf("restarted Exec=%v", err)
			}
			if cut != api.EventApprovalResult {
				if err := r.Resume(t.Context(), log, "s", "recorded"); !errors.Is(err, api.ErrApprovalParked) {
					t.Fatalf("restarted Resume=%v", err)
				}
			}
			b.assertNoCompute(t)
		})
	}
}

type approvalAbortedBackend struct {
	placement.Backend
	cause error
}

func (b approvalAbortedBackend) Snapshot(context.Context, api.Incarnation, api.SnapshotKind) (api.SnapshotRef, error) {
	return api.SnapshotRef{}, b.cause
}
func TestPlacementApprovalBackendAbortedNotLocalBusy(t *testing.T) {
	cause := status.Error(codes.Aborted, "backend conflict")
	runtime := local.New(placementGateHarness{})
	t.Cleanup(func() { _ = runtime.Close() })
	p := placement.New(approvalAbortedBackend{Backend: runtime, cause: cause}, nil)
	log := newSuspendStore(t).Session("s")
	_, err := p.Exec(t.Context(), log, "s", nil, 0)
	if !errors.Is(err, cause) || status.Code(err) != codes.Aborted || errors.Is(err, placement.ErrSessionBusy) || errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("backend cause=%v", err)
	}
}
