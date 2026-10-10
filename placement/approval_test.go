package placement_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/substrate"
)

type placementGateHarness struct {
	run func(context.Context, *api.Start, api.EventSink) error
}

func (placementGateHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "gate", Version: "v1", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}
func (h placementGateHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.run != nil {
		return h.run(ctx, start, sink)
	}
	_, err := sink.ToolCall(ctx, placementGateCall())
	return err
}
func placementGateCall() api.ToolCall {
	return api.ToolCall{ID: "c1", Tool: "write", Args: map[string]any{"path": "file"}, IdempotencyKey: "key", Mediation: api.MediationRequiresApproval}
}

// The runtime remains real; count only the placement-to-runtime boundary and inject operational
// failures there. Snapshot still updates the actor recorder used by ordinary lifecycle tests.
type approvalBackend struct {
	placement.Backend
	describes, creates, restores, snapshots, stops int
	beforeSnapshot                                 func()
}

func (b *approvalBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	b.describes++
	return b.Backend.Describe(ctx)
}
func (b *approvalBackend) Create(ctx context.Context, spec *api.SessionSpec) (api.Incarnation, error) {
	b.creates++
	return b.Backend.Create(ctx, spec)
}
func (b *approvalBackend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	b.restores++
	return b.Backend.Restore(ctx, ref)
}
func (b *approvalBackend) Snapshot(ctx context.Context, inc api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	b.snapshots++
	if b.beforeSnapshot != nil {
		b.beforeSnapshot()
	}
	return b.Backend.Snapshot(ctx, inc, kind)
}
func (b *approvalBackend) Stop(ctx context.Context, inc api.Incarnation) error {
	b.stops++
	return b.Backend.Stop(ctx, inc)
}
func (b *approvalBackend) assertNoCompute(t *testing.T) {
	t.Helper()
	if b.describes+b.creates+b.restores+b.snapshots+b.stops != 0 {
		t.Fatalf("unexpected runtime IO: describe=%d create=%d restore=%d snapshot=%d stop=%d", b.describes, b.creates, b.restores, b.snapshots, b.stops)
	}
}

func seedPlacementApproval(t *testing.T, log eventlog.Store, harness string, cut api.EventKind) {
	t.Helper()
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	call := placementGateCall()
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: harness, HarnessVersion: "v1", InputCount: proto.Int64(0)}},
		{Kind: api.EventToolCall, ToolCall: &call},
		{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "c1"}},
		{Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", RequestSeq: 3, Approved: true}},
	}
	for i, ev := range events {
		ev.ExecutionID = "e1"
		if _, err := log.Append(int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
		if ev.Kind == cut {
			return
		}
	}
	t.Fatalf("unknown cut %s", cut)
}
func placementApprovalRecord(t *testing.T, log eventlog.Store, kind api.EventKind) eventlog.Record {
	t.Helper()
	for _, rec := range suspendRecords(t, log) {
		if rec.Event.Kind == kind {
			return rec
		}
	}
	t.Fatalf("missing %s", kind)
	return eventlog.Record{}
}
func assertNoApprovalLifecycle(t *testing.T, log eventlog.Store) {
	t.Helper()
	for _, rec := range suspendRecords(t, log) {
		if rec.Event.Kind == api.EventLifecycle || rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Kind == api.EventParked {
			t.Fatalf("unexpected terminal/transport/lifecycle record: %+v", rec)
		}
	}
}

// Moving the owned-gate check behind admission/Create must fail even for no-input Exec and
// a recorded decision that has not produced a receipt.
func TestPlacementApprovalPreflightBeforeRuntime(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult} {
		for _, inputs := range [][]api.Message{nil, {*api.TextMessage("user", "new")}} {
			t.Run(string(cut)+"/"+string(rune('0'+len(inputs))), func(t *testing.T) {
				log := newSuspendStore(t).Session("s")
				seedPlacementApproval(t, log, "recorded", cut)
				b := &approvalBackend{Backend: local.New(placementGateHarness{})}
				t.Cleanup(func() { _ = b.Backend.(*local.Backend).Close() })
				p := placement.New(b, nil)
				before := suspendRecords(t, log)
				if _, err := p.Exec(t.Context(), log, "s", inputs, int64(len(before))); !errors.Is(err, controller.ErrApprovalBlocked) {
					t.Fatalf("Exec=%v, want ErrApprovalBlocked", err)
				}
				b.assertNoCompute(t)
				if !reflect.DeepEqual(before, suspendRecords(t, log)) {
					t.Fatal("blocked Exec wrote journal")
				}
			})
		}
	}
}

// This uses the real local Harness.Connect bridge, including remote park ACK and EOF. A typed
// wire park alone is insufficient: placement must snapshot and commit SUSPEND before returning.
func TestPlacementApprovalWireParkAutoSuspend(t *testing.T) {
	log := newSuspendStore(t).Session("s")
	runtime := local.New(placementGateHarness{})
	t.Cleanup(func() { _ = runtime.Close() })
	b := &approvalBackend{Backend: runtime}
	p := placement.New(b, nil)
	var observed []eventlog.Record
	inc, err := p.Exec(t.Context(), log, "s", nil, 0, placement.WithObserver(controller.Observer{OnRecord: func(rec eventlog.Record) { observed = append(observed, rec) }}))
	var park *api.ApprovalParkedError
	if !errors.As(err, &park) || !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("Exec=%v, want typed park", err)
	}
	request := placementApprovalRecord(t, log, api.EventApprovalRequest)
	if b.snapshots != 1 || b.stops != 0 {
		t.Fatalf("snapshots=%d stops=%d", b.snapshots, b.stops)
	}
	assertSuspendRef(t, log, api.SnapshotRef{Local: "s"})
	for _, rec := range suspendRecords(t, log) {
		if rec.Fence != inc.FenceToken {
			t.Fatalf("fence changed: record=%d invocation=%d", rec.Fence, inc.FenceToken)
		}
		if rec.Event.Kind == api.EventParked || rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError {
			t.Fatalf("unexpected %s", rec.Event.Kind)
		}
	}
	if park.Ref != (api.ApprovalRef{ExecutionID: request.Event.ExecutionID, ToolCallID: "c1", RequestSeq: request.Seq}) {
		t.Fatalf("park=%+v request=%+v", park.Ref, request)
	}
	if !reflect.DeepEqual(observed, suspendRecords(t, log)) {
		t.Fatalf("observer missed committed records: %v", observed)
	}
}

func TestPlacementApprovalCloseBeforeSnapshotOnce(t *testing.T) {
	ctl := newSuspendControl()
	desc, _ := (placementGateHarness{}).Describe(t.Context())
	b := &approvalBackend{Backend: substrate.New(ctl, "space", substrate.ObjectRef{Name: "gate"}, desc)}
	closes := 0
	b.beforeSnapshot = func() {
		if closes != 1 {
			t.Fatalf("snapshot before exactly one close: closes=%d", closes)
		}
	}
	p := placement.New(b, nil, placement.WithDialer(func(string) (api.Harness, func() error, error) {
		return placementGateHarness{}, func() error { closes++; return nil }, nil
	}))
	log := newSuspendStore(t).Session("s")
	if _, err := p.Exec(t.Context(), log, "s", nil, 0); !errors.Is(err, api.ErrApprovalParked) {
		t.Fatal(err)
	}
	if closes != 1 || b.snapshots != 1 || b.stops != 0 {
		t.Fatalf("close=%d snapshots=%d stops=%d", closes, b.snapshots, b.stops)
	}
	actor := ctl.actors[substrate.ActorRef{Atespace: "space", Name: "s"}]
	if actor == nil || actor.worker || actor.status != substrate.StatusSuspended {
		t.Fatalf("actor not retained cold: %+v", actor)
	}
}
