package session_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/wire"
)

type joinedPublicParkHarness struct {
	publicGateHarness
	cause error
	pure  string
}

func (h *joinedPublicParkHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	if _, err := sink.ToolCall(ctx, publicGateCall("call")); err != nil {
		return h.unwind(err)
	}
	if h.second {
		if _, err := sink.ToolCall(ctx, publicGateCall("second")); err != nil {
			return h.unwind(err)
		}
	}
	return sink.Output(ctx, "recovered")
}

func (h *joinedPublicParkHarness) unwind(err error) error {
	if h.pure == "wrapped park" {
		return fmt.Errorf("Run unwind: %w", err)
	}
	if h.pure == "joined parks" {
		return errors.Join(err, fmt.Errorf("Run unwind: %w", err))
	}
	return fmt.Errorf("Run failed: %w", errors.Join(err, h.cause))
}

func TestPublicApprovalJoinedRunFailureIsNotSuccessfulPark(t *testing.T) {
	for _, operation := range []string{"Exec", "Resume repark", "no-input Exec repark"} {
		for _, outcome := range []string{"joined sentinel", "joined status", "wrapped park", "joined parks"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				store := openStore(t, ":memory:")
				cause := errors.New("independent Run failure")
				if outcome == "joined status" {
					cause = status.Error(codes.Unavailable, "independent Run failure")
				}
				h := &joinedPublicParkHarness{cause: cause, pure: outcome}
				independent := outcome == "joined sentinel" || outcome == "joined status"
				h.second = operation != "Exec"
				b := &publicGateBackend{Backend: local.New(h)}
				t.Cleanup(func() { _ = b.Close() })
				var closes, effects atomic.Int32
				p := placement.New(b, nil, placement.WithDialer(func(string) (api.Harness, func() error, error) {
					return h, func() error { closes.Add(1); return nil }, nil
				}), placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					effects.Add(1)
					return api.ToolResult{}, nil
				}))
				r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": p})
				if err != nil {
					t.Fatal(err)
				}
				c := serveRegistry(t, store, r)
				uid := mustCreate(t, c)
				log := store.Session(uid)
				if h.second {
					seedPublicGate(t, log, "gate", api.EventApprovalRequest)
					q := publicDecision(uid)
					q.Approved = proto.Bool(true)
					if _, err := c.Approve(t.Context(), q); err != nil {
						t.Fatal(err)
					}
				}
				if operation == "Resume repark" {
					_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
				} else {
					var frames []*v1.ExecUpdate
					frames, err = collectPublicExec(c, &v1.ExecRequest{Session: uid})
					if len(frames) == 0 || (frames[len(frames)-1].GetSession() != nil) == independent {
						t.Errorf("final Session success=%t, independent=%t: %v", len(frames) > 0 && frames[len(frames)-1].GetSession() != nil, independent, frames)
					}
				}
				if independent {
					if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "independent Run failure") {
						t.Errorf("joined Run failure became park success: %v, want existing Run Internal classification", err)
					}
				} else if err != nil {
					t.Errorf("pure matching park no longer succeeds: %v", err)
				}
				state, err := controller.InspectApproval(log, 0)
				if err != nil || state == nil || state.Request == nil || state.Decision != nil || state.Receipt != nil {
					t.Fatalf("lost pending request: %+v,%v", state, err)
				}
				wantEffects := int32(0)
				if h.second {
					wantEffects = 1
				}
				wantSnapshots := int32(1)
				if independent {
					wantSnapshots = 0
				}
				if closes.Load() != 1 || effects.Load() != wantEffects || b.snapshots.Load() != wantSnapshots {
					t.Fatalf("close=%d effects=%d snapshots=%d", closes.Load(), effects.Load(), b.snapshots.Load())
				}
				for _, rec := range routingRecords(t, log) {
					if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Kind == api.EventLifecycle && (independent || rec.Event.Lifecycle.Kind != api.LifecycleSuspend) {
						t.Fatalf("unexpected terminal/audit/lifecycle: %+v", rec)
					}
				}
				before := b.ioCounts()
				if pending, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil || pending.PendingApproval == nil {
					t.Fatalf("pending retry=%v,%v", pending, err)
				}
				if closes.Load() != 1 || b.ioCounts() != before {
					t.Fatal("pending retry did runtime IO")
				}
				if _, err := c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: state.ExecutionID, ToolCallId: state.Call.ID, RequestSeq: state.Request.Seq, Approved: proto.Bool(true)}); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
					t.Fatalf("decision recovery=%v", err)
				}
				if effects.Load() != wantEffects+1 || closes.Load() != 2 {
					t.Fatal("recovery repeated completed effects")
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type publicGateSafetyHarness struct {
	publicGateHarness
	mode   string
	reject atomic.Bool
}

func (h *publicGateSafetyHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	call := publicGateCall("call")
	if h.reject.Load() && h.mode == "keyless identity" {
		call.Mediation, call.IdempotencyKey = api.MediationControllerMediated, ""
	}
	if _, err := sink.ToolCall(ctx, call); err != nil {
		return err // Harness.Connect ends the turn on a host sink error; no remote retry promise.
	}
	if h.reject.Load() {
		return sink.Report(ctx, api.ToolResult{ID: "call"})
	}
	if err := sink.Report(ctx, api.ToolResult{ID: "ordinary"}); err != nil {
		return err
	}
	return sink.Output(ctx, "recovered")
}

func TestPublicApprovalIdentityAndReportSafetyAcrossHarnessConnect(t *testing.T) {
	for _, mode := range []string{"keyless identity", "report collision"} {
		for _, operation := range []string{"Resume", "no-input Exec"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				store := openStore(t, ":memory:")
				h := &publicGateSafetyHarness{mode: mode}
				h.reject.Store(true)
				b := &publicGateBackend{Backend: local.New(h)}
				t.Cleanup(func() { _ = b.Close() })
				var effects atomic.Int32
				p := placement.New(b, nil, placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					effects.Add(1)
					return api.ToolResult{}, nil
				}))
				r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": p})
				if err != nil {
					t.Fatal(err)
				}
				c := serveRegistry(t, store, r)
				uid := mustCreate(t, c)
				log := store.Session(uid)
				seedPublicGate(t, log, "gate", api.EventApprovalRequest)
				q := publicDecision(uid)
				q.Approved = proto.Bool(true)
				if _, err := c.Approve(t.Context(), q); err != nil {
					t.Fatal(err)
				}
				if operation == "Resume" {
					_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
				} else {
					var frames []*v1.ExecUpdate
					frames, err = collectPublicExec(c, &v1.ExecRequest{Session: uid})
					if len(frames) == 0 || frames[len(frames)-1].GetSession() != nil {
						t.Errorf("rejection sent successful final Session: %v", frames)
					}
				}
				wantCode, wantEffects := codes.Internal, int32(0)
				wantErr := controller.ErrReplayDiverged
				if mode == "report collision" {
					wantCode, wantEffects, wantErr = codes.FailedPrecondition, 1, controller.ErrApprovalReceiptReport
				}
				if status.Code(err) != wantCode || !strings.Contains(err.Error(), wantErr.Error()) {
					t.Errorf("public rejection=%v, want %v / %v", err, wantCode, wantErr)
				}
				state, err := controller.InspectApproval(log, 0)
				if err != nil || state == nil || state.Completed || (state.Receipt != nil) != (mode == "report collision") {
					t.Fatalf("rejection poisoned approval evidence: %+v,%v", state, err)
				}
				if effects.Load() != wantEffects || b.snapshots.Load() != 0 {
					t.Fatal("rejection executed extra effects or handed off")
				}
				for _, rec := range routingRecords(t, log) {
					if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventLifecycle || rec.Event.Kind == api.EventOutput ||
						rec.Event.Kind == api.EventToolResult && (mode == "keyless identity" || rec.Event.Result.ApprovalRequestSeq == 0) {
						t.Fatalf("rejection wrote continuation: %+v", rec)
					}
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
				h.reject.Store(false)
				if _, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
					t.Fatalf("next public Resume=%v", err)
				}
				if effects.Load() != 1 {
					t.Fatal("completed gate redriven")
				}
				ctl, err := controller.New(log, nil, controller.WithSessionUID(uid))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ctl.Replay(t.Context(), h); err != nil {
					t.Fatalf("public recovered Replay=%v", err)
				}
			})
		}
	}
}

// Advance the real SQLite journal between the snapshot and automatic SUSPEND append.
// Both CAS and superseded-fence failures remain errors, not successful approval pauses.
func TestPublicApprovalSuspendAppendConflictRetainsPending(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		t.Run(map[bool]string{false: "CAS", true: "fenced"}[supersede], func(t *testing.T) {
			store := openStore(t, ":memory:")
			r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			b.beforeSnapshot = func() {
				log := store.Session(uid)
				if supersede {
					if _, err := log.NewFence(); err != nil {
						t.Error(err)
					}
					return
				}
				records, err := log.Read(1)
				if err != nil {
					t.Error(err)
					return
				}
				last := records[len(records)-1]
				if _, err := log.Append(last.Seq, last.Fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleBaseline}}); err != nil {
					t.Error(err)
				}
			}
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
			if status.Code(err) != codes.Aborted {
				t.Fatalf("failed automatic append=%v", err)
			}
			if frames[len(frames)-1].GetSession() != nil {
				t.Fatal("failed append sent successful final frame")
			}
			got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil || got.PendingApproval == nil || got.ComputeState != v1.ComputeState_COMPUTE_LIVE {
				t.Fatalf("append failure projection=%v %v", got, err)
			}
			for _, rec := range routingRecords(t, store.Session(uid)) {
				if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Lifecycle != nil && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
					t.Fatalf("failed append invented event: %+v", rec.Event)
				}
			}
			b.beforeSnapshot = nil
			recovered, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
			if err != nil || recovered.ComputeState != v1.ComputeState_COMPUTE_COLD || !proto.Equal(recovered.PendingApproval, got.PendingApproval) {
				t.Fatalf("explicit retry=%v %v", recovered, err)
			}
		})
	}
}

func TestPublicApprovalDecisionOverlapUsesSharedGuard(t *testing.T) {
	store := openStore(t, ":memory:")
	r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	b.beforeSnapshot = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Exec did not reach guarded snapshot")
	}
	before, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	ref := before.PendingApproval
	if ref == nil {
		t.Fatal("no committed request before snapshot")
	}
	_, err = c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(false)})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping decision=%v", err)
	}
	if _, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping Resume=%v", err)
	}
	if _, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid}); status.Code(err) != codes.Aborted {
		t.Fatalf("overlapping Suspend=%v", err)
	}
	after, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || after.LastSeq != before.LastSeq || !proto.Equal(after.PendingApproval, before.PendingApproval) {
		t.Fatalf("overlap changed authority=%v %v", after, err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Deliberately malformed raw peer, following harnesswire's existing Connect fixtures. The
// public host must expose actual ACK/EOF errors while retaining the committed request.
type approvalAckPeer struct {
	v1.UnimplementedHarnessServer
	mode string
}

func (approvalAckPeer) Describe(context.Context, *v1.DescribeRequest) (*v1.HarnessDescriptor, error) {
	return &v1.HarnessDescriptor{Id: "gate", Version: "v1", Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}, nil
}
func (p approvalAckPeer) Connect(stream v1.Harness_ConnectServer) error {
	start, err := stream.Recv()
	if err != nil {
		return err
	}
	call := publicGateCall("call")
	if err := stream.Send(wire.EventToProto(api.Event{ExecutionID: start.GetExecutionId(), Kind: api.EventToolCall, ToolCall: &call})); err != nil {
		return err
	}
	park, err := stream.Recv()
	if err != nil {
		return err
	}
	if p.mode == "missing" {
		return nil
	}
	ref := proto.Clone(park.GetPark()).(*v1.ApprovalRef)
	if p.mode == "mismatch" {
		ref.RequestSeq++
	}
	if err := stream.Send(&v1.Event{ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_PARKED, Body: &v1.Event_Parked{Parked: ref}}); err != nil {
		return err
	}
	if p.mode == "extra END" {
		return stream.Send(&v1.Event{ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_END})
	}
	return nil
}
func TestPublicApprovalBadAcknowledgementIsErrorNotPark(t *testing.T) {
	for _, mode := range []string{"missing", "mismatch", "extra END"} {
		t.Run(mode, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			v1.RegisterHarnessServer(server, approvalAckPeer{mode: mode})
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(server.Stop)
			b := remote.New(lis.Addr().String())
			t.Cleanup(func() { _ = b.Close() })
			r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(b, nil)})
			if err != nil {
				t.Fatal(err)
			}
			store := openStore(t, ":memory:")
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
			if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "park acknowledgement") {
				t.Fatalf("bad ACK=%v", err)
			}
			if frames[len(frames)-1].GetSession() != nil {
				t.Fatal("bad ACK sent success frame")
			}
			got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil || got.PendingApproval == nil || got.ComputeState == v1.ComputeState_COMPUTE_COLD {
				t.Fatalf("lost pending request after bad ACK: %v %v", got, err)
			}
			for _, rec := range routingRecords(t, store.Session(uid)) {
				if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Kind == api.EventParked || rec.Event.Lifecycle != nil && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
					t.Fatalf("bad ACK fabricated record: %+v", rec.Event)
				}
			}
		})
	}
}
