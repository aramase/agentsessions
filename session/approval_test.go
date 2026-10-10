package session_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// Real local Harness.Connect exercises park ACK/EOF, placement snapshot and SQL projection.
type publicGateHarness struct {
	runs    atomic.Int32
	starts  chan api.Start
	version string
	second  bool
}

func (h *publicGateHarness) Describe(context.Context) (api.Descriptor, error) {
	version := h.version
	if version == "" {
		version = "v1"
	}
	return api.Descriptor{ID: "gate", Version: version, Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}
func (h *publicGateHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	if h.starts != nil {
		h.starts <- *start
	}
	result, err := sink.ToolCall(ctx, publicGateCall("call"))
	if err != nil {
		return err
	}
	if h.second {
		if _, err := sink.ToolCall(ctx, publicGateCall("second")); err != nil {
			return err
		}
	}
	return sink.Output(ctx, result.ID+":"+string(result.Code))
}
func publicGateCall(id string) api.ToolCall {
	return api.ToolCall{ID: id, Tool: "write", IdempotencyKey: "shared-key", Mediation: api.MediationRequiresApproval, Args: map[string]any{"path": "file"}}
}

type publicGateBackend struct {
	*local.Backend
	describes, creates, restores, snapshots atomic.Int32
	snapshotErr                             error
	beforeSnapshot                          func()
}

func (b *publicGateBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	b.describes.Add(1)
	return b.Backend.Describe(ctx)
}
func (b *publicGateBackend) Create(ctx context.Context, spec *api.SessionSpec) (api.Incarnation, error) {
	b.creates.Add(1)
	return b.Backend.Create(ctx, spec)
}
func (b *publicGateBackend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	b.restores.Add(1)
	return b.Backend.Restore(ctx, ref)
}
func (b *publicGateBackend) Snapshot(ctx context.Context, inc api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	b.snapshots.Add(1)
	if b.beforeSnapshot != nil {
		b.beforeSnapshot()
	}
	if b.snapshotErr != nil {
		return api.SnapshotRef{}, b.snapshotErr
	}
	return b.Backend.Snapshot(ctx, inc, kind)
}
func (b *publicGateBackend) ioCounts() [4]int32 {
	return [4]int32{b.describes.Load(), b.creates.Load(), b.restores.Load(), b.snapshots.Load()}
}
func publicGateRegistry(t *testing.T, h *publicGateHarness, executor controller.ToolFunc) (*placement.Registry, *publicGateBackend) {
	t.Helper()
	b := &publicGateBackend{Backend: local.New(h)}
	t.Cleanup(func() { _ = b.Close() })
	opts := []placement.Option{}
	if executor != nil {
		opts = append(opts, placement.WithToolExecutor(executor))
	}
	r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(b, nil, opts...)})
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}
func seedPublicGate(t *testing.T, log eventlog.Store, harness string, cut api.EventKind) {
	t.Helper()
	f, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	call := publicGateCall("call")
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: harness, HarnessVersion: "v1", InputCount: proto.Int64(0), Config: []byte("original"), ResumeFromSeq: 7}},
		{Kind: api.EventToolCall, ToolCall: &call},
		{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}},
	}
	for i, ev := range events {
		ev.ExecutionID = "execution"
		if _, err := log.Append(int64(i), f, ev); err != nil {
			t.Fatal(err)
		}
		if ev.Kind == cut {
			return
		}
	}
}
func publicDecision(uid string) *v1.ApproveRequest {
	return &v1.ApproveRequest{Session: uid, ExecutionId: "execution", ToolCallId: "call", RequestSeq: 3, Approved: proto.Bool(false)}
}
func collectPublicExec(c v1.SessionsClient, req *v1.ExecRequest) ([]*v1.ExecUpdate, error) {
	stream, err := c.Exec(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var frames []*v1.ExecUpdate
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, frame)
	}
}

// Dropping required presence or trusting a zero scalar would silently deny an approval.
func TestPublicApproveRequiredFieldsAndUnknownUID(t *testing.T) {
	store := openStore(t, ":memory:")
	r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
	svc := session.NewService(store, r)
	c := serveRegistry(t, store, r)
	cases := []struct {
		name   string
		change func(*v1.ApproveRequest)
		code   codes.Code
	}{
		{"session", func(q *v1.ApproveRequest) { q.Session = "" }, codes.InvalidArgument},
		{"execution", func(q *v1.ApproveRequest) { q.ExecutionId = "" }, codes.InvalidArgument},
		{"call", func(q *v1.ApproveRequest) { q.ToolCallId = "" }, codes.InvalidArgument},
		{"zero sequence", func(q *v1.ApproveRequest) { q.RequestSeq = 0 }, codes.InvalidArgument},
		{"negative sequence", func(q *v1.ApproveRequest) { q.RequestSeq = -1 }, codes.InvalidArgument},
		{"absent decision", func(q *v1.ApproveRequest) { q.Approved = nil }, codes.InvalidArgument},
		{"unknown UID", func(q *v1.ApproveRequest) {}, codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := publicDecision("unknown")
			tc.change(q)
			for _, approve := range []func(context.Context, *v1.ApproveRequest) (*v1.ApproveResponse, error){svc.Approve, func(ctx context.Context, q *v1.ApproveRequest) (*v1.ApproveResponse, error) { return c.Approve(ctx, q) }} {
				if _, err := approve(t.Context(), q); status.Code(err) != tc.code {
					t.Fatalf("Approve=%v want %v", err, tc.code)
				}
			}
		})
	}
	if b.ioCounts() != ([4]int32{}) {
		t.Fatal("decision validation used compute")
	}
	list, err := c.ListSessions(t.Context(), &v1.ListSessionsRequest{})
	if err != nil || len(list.Sessions) != 0 {
		t.Fatalf("unknown decision created a session: %v %v", list, err)
	}
}

func TestPublicApproveDecisionOnlyExactRetryAndStatus(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "deny", true: "approve"}[approved], func(t *testing.T) {
			store := openStore(t, ":memory:")
			r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			seedPublicGate(t, store.Session(uid), "unserved-recorded", api.EventApprovalRequest)
			q := publicDecision(uid)
			q.Approved = proto.Bool(approved)
			q.Reason = "reviewed"
			q.Identity = &v1.IdentityRef{Principal: "human", Issuer: "issuer", Subject: "subject"}
			response, err := c.Approve(t.Context(), q)
			if err != nil {
				t.Fatal(err)
			}
			decision := response.GetDecision()
			if decision.GetSeq() != 4 || decision.GetEvent().GetKind() != v1.EventKind_EVENT_APPROVAL_RESULT || decision.GetEvent().GetApprovalResult().GetApproved() != approved || !proto.Equal(decision.GetEvent().GetActor(), q.Identity) {
				t.Fatalf("decision=%v", decision)
			}
			if response.GetSession().GetLastSeq() != 4 || response.GetSession().PendingApproval != nil || response.GetSession().ExecState == v1.ExecState_EXEC_AWAITING {
				t.Fatalf("decided session=%v", response.Session)
			}
			retry, err := c.Approve(t.Context(), q)
			if err != nil || !proto.Equal(retry.GetDecision(), decision) {
				t.Fatalf("exact retry=%v %v", retry, err)
			}
			for _, change := range []func(*v1.ApproveRequest){func(q *v1.ApproveRequest) { q.Reason = "changed" }, func(q *v1.ApproveRequest) { q.Identity.Subject = "other" }, func(q *v1.ApproveRequest) { q.Approved = proto.Bool(!approved) }, func(q *v1.ApproveRequest) { q.RequestSeq++ }, func(q *v1.ApproveRequest) { q.ToolCallId = "wrong" }, func(q *v1.ApproveRequest) { q.ExecutionId = "wrong" }} {
				changed := proto.Clone(q).(*v1.ApproveRequest)
				change(changed)
				if _, err := c.Approve(t.Context(), changed); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("changed decision=%v", err)
				}
			}
			if b.ioCounts() != ([4]int32{}) {
				t.Fatalf("decision routed to compute: %v", b.ioCounts())
			}
			if head, _ := store.Session(uid).Head(); head != 4 {
				t.Fatalf("retry changed head=%d", head)
			}
		})
	}
}

// A decision is not a receipt. Both approve and deny keep new input blocked until recovery.
func TestPublicApprovalParkDiscoveryReopenRecoveryAndFrames(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "deny", true: "approve"}[approved], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store := openStore(t, path)
			h := &publicGateHarness{starts: make(chan api.Start, 10)}
			var effects atomic.Int32
			r, b := publicGateRegistry(t, h, func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
				effects.Add(1)
				if scope.SessionUID == "" || call.IdempotencyKey != "shared-key" {
					t.Error("missing session-scoped identity")
				}
				return api.ToolResult{Output: map[string]any{"ok": true}}, nil
			})
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}, Config: []byte("original"), ResumeFromSeq: 7})
			if err != nil {
				t.Fatalf("park should be OK EOF: %v", err)
			}
			if len(frames) < 3 || frames[0].GetSession().GetLastSeq() != 0 {
				t.Fatalf("initial frame=%v", frames)
			}
			paused := frames[len(frames)-1].GetSession()
			if paused == nil || paused.GetMetadata().GetUid() != uid || paused.GetExecState() != v1.ExecState_EXEC_AWAITING || paused.GetComputeState() != v1.ComputeState_COMPUTE_COLD || paused.GetPendingApproval().GetRequestSeq() <= 0 {
				t.Fatalf("final pause frame=%v", paused)
			}
			records := routingRecords(t, store.Session(uid))
			if paused.GetLastSeq() != int64(len(records)) {
				t.Fatalf("final cursor=%d journal=%d", paused.LastSeq, len(records))
			}
			for _, rec := range records {
				if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError {
					t.Fatalf("park fabricated %s", rec.Event.Kind)
				}
			}
			if effects.Load() != 0 {
				t.Fatal("effect before decision")
			}
			// Reopen through a fresh service: discovery is journal authority, not a service flag.
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openStore(t, path)
			c = serveRegistry(t, store, r)
			got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
			if err != nil || !proto.Equal(got.GetPendingApproval(), paused.GetPendingApproval()) {
				t.Fatalf("reopened Get=%v %v", got, err)
			}
			listed, err := c.ListSessions(t.Context(), &v1.ListSessionsRequest{})
			if err != nil || len(listed.Sessions) != 1 || !proto.Equal(listed.Sessions[0].PendingApproval, paused.PendingApproval) {
				t.Fatalf("reopened List=%v %v", listed, err)
			}
			beforeIO := b.ioCounts()
			before := routingRecords(t, store.Session(uid))
			query, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if err != nil || !proto.Equal(query.PendingApproval, paused.PendingApproval) || query.LastSeq != paused.LastSeq || query.ComputeState != paused.ComputeState {
				t.Fatalf("pending Resume=%v %v", query, err)
			}
			noinput, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "caller-must-not-reroute", Config: []byte("wrong"), ResumeFromSeq: 999})
			if err != nil || len(noinput) != 2 || !proto.Equal(noinput[0].GetSession(), noinput[1].GetSession()) {
				t.Fatalf("pending noinput=%v %v", noinput, err)
			}
			if b.ioCounts() != beforeIO || !reflect.DeepEqual(before, routingRecords(t, store.Session(uid))) {
				t.Fatal("pending query used compute or mutated log")
			}
			for _, decided := range []bool{false, true} {
				if decided {
					ref := paused.PendingApproval
					q := &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(approved)}
					if _, err := c.Approve(t.Context(), q); err != nil {
						t.Fatal(err)
					}
				}
				_, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "unserved", Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "new"))}})
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("new input decided=%v: %v", decided, err)
				}
				if b.ioCounts() != beforeIO {
					t.Fatal("owned input block depends on compute")
				}
			}
			recovered, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "ignore", Config: []byte("wrong"), ResumeFromSeq: 999})
			if err != nil {
				t.Fatalf("decided recovery=%v", err)
			}
			current := recovered[len(recovered)-1].GetSession()
			if current == nil || current.PendingApproval != nil || current.ComputeState != v1.ComputeState_COMPUTE_LIVE {
				t.Fatalf("final recovery=%v", current)
			}
			if current.LastSeq <= paused.LastSeq || current.LastSeq != int64(len(routingRecords(t, store.Session(uid)))) {
				t.Fatal("recovery final cursor wrong")
			}
			first, second := <-h.starts, <-h.starts
			if first.ExecutionID != second.ExecutionID || string(second.Config) != "original" || second.ResumeFromSeq != 7 || len(second.Inputs) != 1 || second.Inputs[0].Text() != "write" {
				t.Fatalf("recovery rewrote invocation: first=%+v second=%+v", first, second)
			}
			wantEffects := int32(0)
			if approved {
				wantEffects = 1
			}
			if effects.Load() != wantEffects {
				t.Fatalf("effects=%d want %d", effects.Load(), wantEffects)
			}
			ref := paused.PendingApproval
			retry, err := c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(approved)})
			if err != nil || retry.Decision.GetSeq() <= ref.RequestSeq || retry.Session.LastSeq != current.LastSeq {
				t.Fatalf("historical retry=%v %v", retry, err)
			}
			if _, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "new"))}}); err != nil {
				t.Fatalf("receipt did not unlock new turn: %v", err)
			}
			beforeRetry := routingRecords(t, store.Session(uid))
			afterTurn, err := c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(approved)})
			if err != nil || !proto.Equal(afterTurn.GetDecision(), retry.GetDecision()) || afterTurn.GetSession().GetPendingApproval().GetExecutionId() == ref.ExecutionId || !reflect.DeepEqual(beforeRetry, routingRecords(t, store.Session(uid))) {
				t.Fatalf("exact retry after newer turn changed decision/current gate: %v %v", afterTurn, err)
			}
		})
	}
}

func TestPublicApprovalRepairAndRecoveryCASDeadline(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest} {
		t.Run(string(cut), func(t *testing.T) {
			store := openStore(t, ":memory:")
			r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			seedPublicGate(t, store.Session(uid), "gate", cut)
			before := routingRecords(t, store.Session(uid))
			for _, q := range []*v1.ExecRequest{{Session: uid, ExpectedLastSeq: proto.Int64(0)}, {Session: uid, DeadlineUnix: time.Now().Add(-time.Minute).Unix()}} {
				_, err := collectPublicExec(c, q)
				want := codes.Aborted
				if q.DeadlineUnix != 0 {
					want = codes.DeadlineExceeded
				}
				if status.Code(err) != want {
					t.Fatalf("recovery precondition=%v want %v", err, want)
				}
				if !reflect.DeepEqual(before, routingRecords(t, store.Session(uid))) {
					t.Fatal("failed recovery wrote repair")
				}
			}
			frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, ExpectedLastSeq: proto.Int64(int64(len(before)))})
			if err != nil {
				t.Fatal(err)
			}
			final := frames[len(frames)-1].GetSession()
			if final.GetLastSeq() != 3 || final.GetPendingApproval().GetRequestSeq() != 3 || final.GetExecState() != v1.ExecState_EXEC_AWAITING || final.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
				t.Fatalf("repair/query=%v", final)
			}
			if b.ioCounts() != ([4]int32{}) {
				t.Fatal("repair used compute")
			}
			if cut == api.EventToolCall && (len(frames) != 3 || frames[1].GetRecord().GetEvent().GetKind() != v1.EventKind_EVENT_APPROVAL_REQUEST) {
				t.Fatalf("repair observer frames=%v", frames)
			}
		})
	}
}

func TestPublicApprovalSnapshotFailureRetainsRequestAndSuspendRetry(t *testing.T) {
	store := openStore(t, ":memory:")
	r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	b.snapshotErr = errors.New("snapshot unavailable")
	frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))}})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "snapshot unavailable") {
		t.Fatalf("snapshot error=%v", err)
	}
	if frames[len(frames)-1].GetSession() != nil {
		t.Fatal("failed handoff fabricated successful final pause")
	}
	got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || got.PendingApproval == nil || got.ComputeState == v1.ComputeState_COMPUTE_COLD {
		t.Fatalf("failed snapshot state=%v %v", got, err)
	}
	b.snapshotErr = nil
	suspended, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
	if err != nil || suspended.ComputeState != v1.ComputeState_COMPUTE_COLD || !proto.Equal(suspended.PendingApproval, got.PendingApproval) {
		t.Fatalf("retry Suspend=%v %v", suspended, err)
	}
}

func TestPublicApprovalMissingExecutorRecoverableAndInheritedDecisionRefused(t *testing.T) {
	store := openStore(t, ":memory:")
	r, b := publicGateRegistry(t, &publicGateHarness{}, nil)
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	seedPublicGate(t, store.Session(uid), "gate", api.EventApprovalRequest)
	child := store.Session("child")
	if err := controller.Fork(store.Session(uid), child, 3); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSession(sqlitelog.SessionMeta{UID: "child", Harness: "gate"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(t.Context(), publicDecision("child")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("inherited decision=%v", err)
	}
	if _, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: "child"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("inherited resume=%v", err)
	}
	if _, err := collectPublicExec(c, &v1.ExecRequest{Session: "child"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("inherited no-input recovery=%v", err)
	}
	frames, err := collectPublicExec(c, &v1.ExecRequest{Session: "child", Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "explicit new turn"))}})
	if err != nil || frames[len(frames)-1].GetSession().GetPendingApproval().GetExecutionId() == "execution" || frames[len(frames)-1].GetSession().GetPendingApproval() == nil {
		t.Fatalf("inherited child cannot explicitly start its own gate: %v %v", frames, err)
	}
	beforeSnapshots := b.snapshots.Load()
	q := publicDecision(uid)
	q.Approved = proto.Bool(true)
	if _, err := c.Approve(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no tool executor") {
		t.Fatalf("missing executor=%v", err)
	}
	got, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || got.PendingApproval != nil || got.LastSeq != 4 {
		t.Fatalf("recoverable decision state=%v %v", got, err)
	}
	if b.snapshots.Load() != beforeSnapshots {
		t.Fatal("missing executor snapshotted")
	}
}

func TestPublicApprovalSuspendRecordedRouteAndUnservedPrecondition(t *testing.T) {
	for _, served := range []bool{false, true} {
		t.Run(map[bool]string{false: "unserved", true: "recorded override"}[served], func(t *testing.T) {
			store := openStore(t, ":memory:")
			a := &publicGateBackend{Backend: local.New(&publicGateHarness{})}
			b := &publicGateBackend{Backend: local.New(&publicGateHarness{})}
			t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
			entries := map[string]*placement.Placer{"default": placement.New(a, nil)}
			if served {
				entries["recorded"] = placement.New(b, nil)
			}
			r, err := placement.NewRegistry("default", entries)
			if err != nil {
				t.Fatal(err)
			}
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			seedPublicGate(t, store.Session(uid), "recorded", api.EventApprovalRequest)
			_, err = c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
			if !served {
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("unserved Suspend=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if a.ioCounts() != ([4]int32{}) {
				t.Fatal("Suspend fell back to stored default")
			}
			want := int32(0)
			if served {
				want = 1
			}
			if b.snapshots.Load() != want {
				t.Fatalf("recorded snapshots=%d want %d", b.snapshots.Load(), want)
			}
		})
	}
}
