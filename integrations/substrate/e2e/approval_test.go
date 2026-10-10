package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/integrations/substrate/e2e/internal/approvalfixture"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

const (
	approvalTemplate       = "approval-harness"
	approvalMemoryTemplate = "approval-memory"
)

// TestApprovalOnGVisor drives public Sessions over gRPC; every actor call uses real substrate
// Control and direct PodIP Harness.Connect. No live result is claimed by an offline skip.
func TestApprovalOnGVisor(t *testing.T) {
	f := newFixture(t, uniqueUID("e2e-approval"))
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	desc, err := (approvalfixture.Harness{}).Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	backend := f.backend(approvalTemplate, desc)
	runApprovalSuite(t, ctx, backend, func(uid string) { stopQuietly(t, backend, uid) }, true)
}

// This template actually serves the memory descriptor on a microVM, rather than giving the
// production counter approval behavior or pretending a local backend captures RAM.
func TestApprovalMemoryRefusalOnMicroVM(t *testing.T) {
	f := newFixture(t, uniqueUID("e2e-approval-memory"))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	desc, err := (approvalfixture.Harness{Memory: true}).Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	backend := f.backend(approvalMemoryTemplate, desc)
	effects := approvalEffects(t)
	runtime := &approvalRuntime{Backend: backend}
	store := journal(t)
	var executorCalls atomic.Int32
	client, closeClient := serveApprovalSessions(t, store, approvalRegistry(t, runtime, func(ctx context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		executorCalls.Add(1)
		return effects.Execute(ctx, scope, call)
	}))
	defer closeClient()
	uid := createApprovalSession(t, ctx, client)
	defer stopQuietly(t, backend, uid)
	// Provision only this owned actor to inspect its actual descriptor before the public turn.
	inc, err := backend.Create(ctx, &api.SessionSpec{SessionUID: uid})
	if err != nil {
		t.Fatal(err)
	}
	har, closeHar, err := approvalDial(inc.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeHar() }()
	served, err := har.Describe(ctx)
	if err != nil || !reflect.DeepEqual(served, desc) {
		t.Fatalf("served memory descriptor=%+v want=%+v: %v", served, desc, err)
	}
	_, err = approvalExec(ctx, client, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "original-input"))}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("memory gate=%v want FailedPrecondition", err)
	}
	start := approvalRecord(t, store.Session(uid), api.EventExecutionStart)
	if start.Event.ExecutionStart.Harness != "approval-fixture" || start.Event.ExecutionStart.HarnessVersion != "1" {
		t.Fatalf("memory turn did not reach the served harness: %+v", start)
	}
	_ = approvalRecord(t, store.Session(uid), api.EventInput)
	records := approvalRecords(t, store.Session(uid))
	for _, r := range records {
		switch r.Event.Kind {
		case api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult, api.EventToolResult:
			t.Fatalf("memory refusal wrote unsafe %s", r.Event.Kind)
		}
	}
	approvalEffectCount(t, effects, uid, 0)
	if runtime.snapshots.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatal("memory refusal attempted approval snapshot or executor")
	}
	current, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
	if err != nil || current.PendingApproval != nil {
		t.Fatalf("memory session=%v %v", current, err)
	}
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
}

func runApprovalSuite(t *testing.T, ctx context.Context, backend placement.Backend, cleanup func(string), live bool) {
	t.Helper()
	effects := approvalEffects(t)
	runtime := &approvalRuntime{Backend: backend}
	// Two approved sessions deliberately reuse the fixture's same key on this one host/executor.
	for _, tc := range []struct {
		name      string
		approved  bool
		rpcResume bool
	}{{"approved", true, true}, {"denied", false, false}, {"same-key-second-session", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			runApprovalSession(t, ctx, runtime, effects, cleanup, live, tc.approved, tc.rpcResume)
		})
	}
}

func runApprovalSession(t *testing.T, ctx context.Context, runtime *approvalRuntime, effects *approvalfixture.Effects, cleanup func(string), live, approved, rpcResume bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var executorCalls atomic.Int32
	execute := func(ctx context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		executorCalls.Add(1)
		return effects.Execute(ctx, scope, call)
	}
	client, closeClient := serveApprovalSessions(t, store, approvalRegistry(t, runtime, execute))
	defer func() { closeClient() }()
	uid := createApprovalSession(t, ctx, client)
	defer cleanup(uid)
	frames, err := approvalExec(ctx, client, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "original-input"))}, Config: []byte("original-config"), ResumeFromSeq: 17})
	if err != nil {
		t.Fatalf("park must ACK and finish OK EOF: %v", err)
	}
	paused := approvalFinalSession(t, frames)
	if frames[0].GetSession().GetMetadata().GetUid() != uid || paused.GetMetadata().GetUid() != uid || paused.LastSeq != int64(len(approvalRecords(t, store.Session(uid)))) {
		t.Fatalf("public initial/final session frames=%v", frames)
	}
	if paused.ExecState != v1.ExecState_EXEC_AWAITING || paused.ComputeState != v1.ComputeState_COMPUTE_COLD || paused.PendingApproval == nil {
		t.Fatalf("park=%v", paused)
	}
	call := approvalRecord(t, store.Session(uid), api.EventToolCall)
	request := approvalRecord(t, store.Session(uid), api.EventApprovalRequest)
	suspend := approvalRecord(t, store.Session(uid), api.EventLifecycle)
	ref := paused.PendingApproval
	if ref.ExecutionId != call.Event.ExecutionID || ref.ToolCallId != "fixture-call" || ref.RequestSeq != request.Seq || request.Event.Approval.ToolCallID != call.Event.ToolCall.ID {
		t.Fatalf("actionable tuple=%v call=%+v request=%+v", ref, call, request)
	}
	if call.Event.ToolCall.Tool != "fixture-effect" || call.Event.ToolCall.IdempotencyKey != "shared-key" || call.Event.ToolCall.Mediation != api.MediationRequiresApproval || !reflect.DeepEqual(call.Event.ToolCall.Args, map[string]any{"input": "original-input", "config": "original-config", "cursor": float64(17)}) {
		t.Fatalf("original call=%+v", call)
	}
	if suspend.Event.Lifecycle.Kind != api.LifecycleSuspend || suspend.Event.Lifecycle.Snapshot == nil || suspend.Event.Lifecycle.Snapshot.Local != uid {
		t.Fatalf("auto suspend=%+v", suspend)
	}
	if live && (suspend.Event.Lifecycle.Snapshot.ExternalURI == "" || !suspend.Event.Lifecycle.Snapshot.Memory) {
		t.Fatalf("missing real external snapshot=%+v", suspend)
	}
	for _, r := range approvalRecords(t, store.Session(uid)) {
		switch r.Event.Kind {
		case api.EventEnd, api.EventError, api.EventToolResult, api.EventOutput, api.EventParked:
			t.Fatalf("park fabricated %s", r.Event.Kind)
		}
	}
	approvalEffectCount(t, effects, uid, 0)
	if live {
		if state, err := runtime.Status(ctx, api.Incarnation{ID: uid}); err != nil || state != api.ComputeCold {
			t.Fatalf("real actor status=%s %v", state, err)
		}
	}

	// Stop the old service and reopen SQLite with a NEW Registry/Service, retaining the real runtime
	// and Control connection. No service flag or registry guard can be the pending authority.
	closeClient()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store = reopened
	client, closeClient = serveApprovalSessions(t, store, approvalRegistry(t, runtime, execute))
	got, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
	if err != nil || !proto.Equal(got.PendingApproval, ref) || got.ComputeState != v1.ComputeState_COMPUTE_COLD || got.ExecState != v1.ExecState_EXEC_AWAITING {
		t.Fatalf("reopened Get=%v %v", got, err)
	}
	listed, err := client.ListSessions(ctx, &v1.ListSessionsRequest{})
	if err != nil || len(listed.Sessions) != 1 || !proto.Equal(listed.Sessions[0].PendingApproval, ref) {
		t.Fatalf("reopened List=%v %v", listed, err)
	}
	beforeIO := runtime.counts()
	before := approvalRecords(t, store.Session(uid))
	queried, err := client.Resume(ctx, &v1.ResumeRequest{Session: uid})
	if err != nil || !proto.Equal(queried, got) {
		t.Fatalf("pending Resume=%v %v", queried, err)
	}
	noinput, err := approvalExec(ctx, client, &v1.ExecRequest{Session: uid, Harness: "do-not-reroute", Config: []byte("wrong"), ResumeFromSeq: 999})
	if err != nil || !proto.Equal(approvalFinalSession(t, noinput), got) {
		t.Fatalf("pending noinput=%v %v", noinput, err)
	}
	if runtime.counts() != beforeIO || !reflect.DeepEqual(before, approvalRecords(t, store.Session(uid))) {
		t.Fatal("pending query used compute or changed journal/start/prompt")
	}
	assertApprovalInputBlocked(t, ctx, client, uid)
	approvalEffectCount(t, effects, uid, 0)

	if rpcResume {
		testApprovalInheritedOpenFork(t, ctx, client, store, runtime, effects, uid, request.Seq, cleanup)
	}
	beforeIO = runtime.counts()
	decisionRequest := &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(approved), Reason: "fixture-reviewed"}
	response, err := client.Approve(ctx, decisionRequest)
	if err != nil {
		t.Fatal(err)
	}
	decision := approvalRecord(t, store.Session(uid), api.EventApprovalResult)
	if response.GetDecision().GetSeq() != decision.Seq || decision.Event.ExecutionID != ref.ExecutionId || decision.Event.ApprovalResult.ToolCallID != ref.ToolCallId || decision.Event.ApprovalResult.RequestSeq != ref.RequestSeq || decision.Event.ApprovalResult.Approved != approved {
		t.Fatalf("decision=%v", response)
	}
	if runtime.counts() != beforeIO || executorCalls.Load() != 0 {
		t.Fatal("decision RPC used compute or executor")
	}
	approvalEffectCount(t, effects, uid, 0)
	assertApprovalInputBlocked(t, ctx, client, uid)
	retry, err := client.Approve(ctx, decisionRequest)
	if err != nil || !proto.Equal(response.Decision, retry.Decision) {
		t.Fatalf("decision retry=%v %v", retry, err)
	}
	if runtime.counts() != beforeIO || executorCalls.Load() != 0 {
		t.Fatal("decision retry or blocked input used compute or executor")
	}
	if rpcResume {
		testApprovalInheritedOpenFork(t, ctx, client, store, runtime, effects, uid, decision.Seq, cleanup)
	}

	if rpcResume {
		current, err := client.Resume(ctx, &v1.ResumeRequest{Session: uid})
		if err != nil || current.PendingApproval != nil || current.ComputeState != v1.ComputeState_COMPUTE_LIVE {
			t.Fatalf("Resume recovery=%v %v", current, err)
		}
	} else {
		recovered, err := approvalExec(ctx, client, &v1.ExecRequest{Session: uid, Harness: "ignore", Config: []byte("wrong"), ResumeFromSeq: 999})
		if err != nil {
			t.Fatalf("noinput recovery=%v", err)
		}
		current := approvalFinalSession(t, recovered)
		if current.PendingApproval != nil || current.ComputeState != v1.ComputeState_COMPUTE_LIVE {
			t.Fatalf("recovery=%v", current)
		}
	}
	receipt := approvalRecord(t, store.Session(uid), api.EventToolResult)
	wantCode := api.ToolResultCodeApprovalDenied
	wantCount := 0
	if approved {
		wantCode = api.ToolResultCodeUnspecified
		wantCount = 1
	}
	if receipt.Event.ExecutionID != ref.ExecutionId || receipt.Event.Result.ID != ref.ToolCallId || receipt.Event.Result.Code != wantCode || receipt.Event.Result.IsError == approved || receipt.Event.Result.ApprovalRequestSeq != ref.RequestSeq || receipt.Event.Result.ApprovalDecisionSeq != decision.Seq {
		t.Fatalf("correlated receipt=%+v", receipt)
	}
	output := approvalRecord(t, store.Session(uid), api.EventOutput).Event.Message.Text()
	var gotOutput map[string]any
	if err := json.Unmarshal([]byte(output), &gotOutput); err != nil {
		t.Fatal(err)
	}
	if gotOutput["input"] != "original-input" || gotOutput["config"] != "original-config" || gotOutput["cursor"] != float64(17) || gotOutput["code"] != float64(wantCode) || gotOutput["request_seq"] != float64(ref.RequestSeq) || gotOutput["decision_seq"] != float64(decision.Seq) {
		t.Fatalf("original invocation lost: %s", output)
	}
	if approved {
		if receipt.Event.Result.Output["session"] != uid || receipt.Event.Result.Output["key"] != "shared-key" {
			t.Fatalf("wrong effect namespace: %+v", receipt)
		}
	}
	_ = approvalRecord(t, store.Session(uid), api.EventEnd)
	approvalEffectCount(t, effects, uid, wantCount)
	if executorCalls.Load() != int32(wantCount) {
		t.Fatalf("executor calls=%d want %d", executorCalls.Load(), wantCount)
	}
	if _, err := client.Resume(ctx, &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("completed Resume=%v", err)
	}
	approvalEffectCount(t, effects, uid, wantCount)

	// Public Replay only audits. Controller Replay actually runs the completed turn on the same
	// wire-backed actor and must serve recorded decision+receipt with no executor or new prompt.
	inc, err := runtime.Backend.Create(ctx, &api.SessionSpec{SessionUID: uid})
	if err != nil {
		t.Fatal(err)
	}
	har, closeHar, err := approvalDial(inc.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeHar() }()
	served, err := har.Describe(ctx)
	if err != nil || served.ID != "approval-fixture" || served.Version != "1" || served.Capabilities.Resumability != api.ResumabilityStatelessReplay {
		t.Fatalf("served descriptor=%+v %v", served, err)
	}
	var replayEffects atomic.Int32
	c, err := controller.New(store.Session(uid), nil, controller.WithSessionUID(uid), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
		replayEffects.Add(1)
		return api.ToolResult{}, errors.New("replay must not execute")
	}))
	if err != nil {
		t.Fatal(err)
	}
	replayBefore := approvalRecords(t, store.Session(uid))
	if out, err := c.Replay(ctx, har); err != nil || !reflect.DeepEqual(out, []string{output}) {
		t.Fatalf("actor controller Replay=%v %v", out, err)
	}
	if replayEffects.Load() != 0 || !reflect.DeepEqual(replayBefore, approvalRecords(t, store.Session(uid))) {
		t.Fatal("completed replay invoked executor or changed prompt/intent/journal")
	}
	approvalEffectCount(t, effects, uid, wantCount)
	if rpcResume {
		testApprovalCompletedFork(t, ctx, client, store, effects, uid, receipt.Seq, cleanup)
	}
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
	// The durable receipt unlocks explicit new input, rather than retaining a permanent session gate.
	frames, err = approvalExec(ctx, client, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "new-input"))}})
	if err != nil || approvalFinalSession(t, frames).GetPendingApproval().GetExecutionId() == ref.ExecutionId || approvalFinalSession(t, frames).PendingApproval == nil {
		t.Fatalf("receipt did not unlock new input: %v %v", frames, err)
	}
	approvalEffectCount(t, effects, uid, wantCount)
	if executorCalls.Load() != int32(wantCount) {
		t.Fatalf("completed recovery/replay/new input reexecuted tool: calls=%d want %d", executorCalls.Load(), wantCount)
	}
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
}

// This is an observing decorator, not a fake: every counted call delegates to the real runtime.
// Status observations are uncounted so they cannot obscure a decision-only no-compute assertion.
type approvalRuntime struct {
	placement.Backend
	describes, creates, restores, snapshots atomic.Int32
}

func (b *approvalRuntime) Describe(ctx context.Context) (api.Descriptor, error) {
	b.describes.Add(1)
	return b.Backend.Describe(ctx)
}
func (b *approvalRuntime) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	b.creates.Add(1)
	return b.Backend.Create(ctx, s)
}
func (b *approvalRuntime) Restore(ctx context.Context, r api.SnapshotRef) (api.Incarnation, error) {
	b.restores.Add(1)
	return b.Backend.Restore(ctx, r)
}
func (b *approvalRuntime) Snapshot(ctx context.Context, i api.Incarnation, k api.SnapshotKind) (api.SnapshotRef, error) {
	b.snapshots.Add(1)
	return b.Backend.Snapshot(ctx, i, k)
}
func (b *approvalRuntime) counts() [4]int32 {
	return [4]int32{b.describes.Load(), b.creates.Load(), b.restores.Load(), b.snapshots.Load()}
}

func approvalRegistry(t *testing.T, b placement.Backend, execute controller.ToolFunc) *placement.Registry {
	t.Helper()
	r, err := placement.NewRegistry("approval-fixture", map[string]*placement.Placer{"approval-fixture": placement.New(b, nil, placement.WithToolExecutor(execute))})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func serveApprovalSessions(t *testing.T, store *sqlitelog.Store, r *placement.Registry) (v1.SessionsClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterSessionsServer(server, session.NewService(store, r))
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	return v1.NewSessionsClient(conn), func() { _ = conn.Close(); server.Stop() }
}
func approvalDial(address string) (api.Harness, func() error, error) {
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if sock, ok := strings.CutPrefix(address, "unix://"); ok {
		address = "passthrough:///approval-fixture"
		options = append(options, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}))
	}
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, nil, err
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}
func approvalEffects(t *testing.T) *approvalfixture.Effects {
	t.Helper()
	e, err := approvalfixture.OpenEffects(filepath.Join(t.TempDir(), "effects.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func approvalEffectCount(t *testing.T, e *approvalfixture.Effects, uid string, want int) {
	t.Helper()
	if n, err := e.Count(uid); err != nil || n != want {
		t.Fatalf("session %s effects=%d want %d: %v", uid, n, want, err)
	}
}
func createApprovalSession(t *testing.T, ctx context.Context, c v1.SessionsClient) string {
	t.Helper()
	s, err := c.CreateSession(ctx, &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return s.GetMetadata().GetUid()
}
func approvalExec(ctx context.Context, c v1.SessionsClient, req *v1.ExecRequest) ([]*v1.ExecUpdate, error) {
	stream, err := c.Exec(ctx, req)
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
func approvalFinalSession(t *testing.T, frames []*v1.ExecUpdate) *v1.Session {
	t.Helper()
	if len(frames) == 0 || frames[len(frames)-1].GetSession() == nil {
		t.Fatalf("missing final session: %v", frames)
	}
	return frames[len(frames)-1].GetSession()
}
func approvalRecords(t *testing.T, log *sqlitelog.Log) []eventlog.Record {
	t.Helper()
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	return records
}
func approvalRecord(t *testing.T, log *sqlitelog.Log, kind api.EventKind) eventlog.Record {
	t.Helper()
	for _, r := range approvalRecords(t, log) {
		if r.Event.Kind == kind {
			return r
		}
	}
	t.Fatalf("missing %s", kind)
	return eventlog.Record{}
}
func assertApprovalInputBlocked(t *testing.T, ctx context.Context, c v1.SessionsClient, uid string) {
	t.Helper()
	_, err := approvalExec(ctx, c, &v1.ExecRequest{Session: uid, Harness: "unserved", Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "blocked"))}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("new input=%v want FailedPrecondition", err)
	}
}

func testApprovalInheritedOpenFork(t *testing.T, ctx context.Context, c v1.SessionsClient, store *sqlitelog.Store, b *approvalRuntime, e *approvalfixture.Effects, parent string, seq int64, cleanup func(string)) {
	t.Helper()
	fork, err := c.Fork(ctx, &v1.ForkRequest{Session: parent, AtSeq: seq, Count: 1})
	if err != nil || len(fork.GetChildren()) != 1 {
		t.Fatalf("open Fork=%v %v", fork, err)
	}
	uid := fork.Children[0].GetMetadata().GetUid()
	defer cleanup(uid)
	beforeIO := b.counts()
	before := approvalRecords(t, store.Session(uid))
	if _, err := c.Resume(ctx, &v1.ResumeRequest{Session: uid}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("inherited Resume=%v", err)
	}
	if _, err := approvalExec(ctx, c, &v1.ExecRequest{Session: uid}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("inherited noinput=%v", err)
	}
	if b.counts() != beforeIO || !reflect.DeepEqual(before, approvalRecords(t, store.Session(uid))) {
		t.Fatal("inherited refusal used runtime or changed journal")
	}
	approvalEffectCount(t, e, uid, 0)
	// Explicit new input is the documented escape into a child-owned execution/approval namespace.
	frames, err := approvalExec(ctx, c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "child-new"))}})
	if err != nil {
		t.Fatal(err)
	}
	pending := approvalFinalSession(t, frames).PendingApproval
	if pending == nil || pending.RequestSeq <= seq {
		t.Fatalf("child gate=%v", pending)
	}
	approvalEffectCount(t, e, uid, 0)
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
}
func testApprovalCompletedFork(t *testing.T, ctx context.Context, c v1.SessionsClient, store *sqlitelog.Store, e *approvalfixture.Effects, parent string, seq int64, cleanup func(string)) {
	t.Helper()
	fork, err := c.Fork(ctx, &v1.ForkRequest{Session: parent, AtSeq: seq, Count: 1})
	if err != nil || len(fork.GetChildren()) != 1 {
		t.Fatalf("receipt Fork=%v %v", fork, err)
	}
	uid := fork.Children[0].GetMetadata().GetUid()
	defer cleanup(uid)
	if _, err := c.Resume(ctx, &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("completed inherited receipt Resume=%v", err)
	}
	approvalEffectCount(t, e, uid, 0)
	receipt := approvalRecord(t, store.Session(uid), api.EventToolResult)
	if receipt.Event.Result.Output["session"] != parent {
		t.Fatalf("inherited receipt changed namespace: %+v", receipt)
	}
	_ = approvalRecord(t, store.Session(uid), api.EventEnd)
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
}
