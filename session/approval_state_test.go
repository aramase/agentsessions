package session

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

func approvalStore(t *testing.T, path string) *sqlitelog.Store {
	t.Helper()
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func appendApprovalEvents(t *testing.T, store *sqlitelog.Store, uid string, events ...api.Event) []eventlog.Record {
	t.Helper()
	log := store.Session(uid)
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		record, err := log.Append(head, fence, event)
		if err != nil {
			t.Fatal(err)
		}
		head = record.Seq
	}
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func assertApprovalSession(t *testing.T, got *v1.Session, state v1.ExecState, seq int64, compute v1.ComputeState) {
	t.Helper()
	if got.GetExecState() != state {
		t.Errorf("exec_state = %v, want %v", got.GetExecState(), state)
	}
	if got.GetLastSeq() != seq {
		t.Errorf("last_seq = %d, want %d", got.GetLastSeq(), seq)
	}
	if got.GetComputeState() != compute {
		t.Errorf("compute_state = %v, want %v", got.GetComputeState(), compute)
	}
}

func assertApprovalJournal(t *testing.T, store *sqlitelog.Store, uid string, before []eventlog.Record) {
	t.Helper()
	after, err := store.Session(uid).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("query changed journal, including cursor, hashes or fences: before=%+v after=%+v", before, after)
	}
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatalf("journal chain: %v", err)
	}
}

func TestQueryApprovalStateUnresolved(t *testing.T) {
	store := approvalStore(t, ":memory:")
	const uid = "approval"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid}); err != nil {
		t.Fatal(err)
	}
	before := appendApprovalEvents(t, store, uid,
		api.Event{Kind: api.EventExecutionStart, ExecutionID: "execution", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1)}},
		api.Event{Kind: api.EventInput, ExecutionID: "execution", Message: api.TextMessage("user", "write")},
		api.Event{Kind: api.EventToolCall, ExecutionID: "execution", ToolCall: &api.ToolCall{ID: "call", Tool: "write", Mediation: api.MediationRequiresApproval}},
		api.Event{Kind: api.EventApprovalRequest, ExecutionID: "execution", Approval: &api.ApprovalRequest{ToolCallID: "call"}},
	)
	svc := NewService(store, nil)
	got, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	assertApprovalSession(t, got, v1.ExecState_EXEC_AWAITING, 4, v1.ComputeState_COMPUTE_LIVE)
	listed, err := svc.ListSessions(t.Context(), &v1.ListSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetSessions()) != 1 || listed.GetSessions()[0].GetMetadata().GetUid() != uid {
		t.Fatalf("list = %v, want only %s", listed, uid)
	}
	assertApprovalSession(t, listed.GetSessions()[0], v1.ExecState_EXEC_AWAITING, 4, v1.ComputeState_COMPUTE_LIVE)
	assertApprovalJournal(t, store, uid, before)
}

func approvalRequest(execution, call string) api.Event {
	return api.Event{Kind: api.EventApprovalRequest, ExecutionID: execution, Approval: &api.ApprovalRequest{ToolCallID: call}}
}

func approvalDecision(execution, call string, approved bool) api.Event {
	return api.Event{Kind: api.EventApprovalResult, ExecutionID: execution, ApprovalResult: &api.ApprovalResult{ToolCallID: call, Approved: approved}}
}

func approvalStart(execution string) api.Event {
	return api.Event{Kind: api.EventExecutionStart, ExecutionID: execution, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0)}}
}

func approvalLifecycle(kind api.LifecycleKind, execution string) api.Event {
	return api.Event{Kind: api.EventLifecycle, ExecutionID: execution, Lifecycle: &api.Lifecycle{Kind: kind}}
}

func TestReducerApprovalState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		events  []api.Event
		through int64
		want    bool
	}{
		{"empty", nil, 0, false},
		{"plain completed", []api.Event{{Kind: api.EventInput, ExecutionID: "a"}, {Kind: api.EventEnd, ExecutionID: "a"}}, 2, false},
		{"nonterminal", []api.Event{approvalStart("a"), {Kind: api.EventInput, ExecutionID: "a"}}, 2, false},
		{"error only", []api.Event{{Kind: api.EventError, ExecutionID: "a"}}, 1, false},
		{"ID-less fallback", []api.Event{{Kind: api.EventInput}, approvalRequest("", "call")}, 2, false},
		{"modern unanswered", []api.Event{approvalStart("a"), approvalRequest("a", "call")}, 2, true},
		{"markerless unanswered", []api.Event{{Kind: api.EventInput, ExecutionID: "a"}, approvalRequest("a", "call")}, 2, true},
		{"approve", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "call", true)}, 2, false},
		{"deny", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "call", false)}, 2, false},
		{"wrong execution", []api.Event{approvalStart("old"), approvalRequest("a", "call"), approvalDecision("old", "call", true)}, 3, true},
		{"wrong call", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "other", true)}, 2, true},
		{"nil decision", []api.Event{approvalRequest("a", "call"), {Kind: api.EventApprovalResult, ExecutionID: "a"}}, 2, true},
		{"malformed decision body", []api.Event{approvalRequest("a", "call"), {Kind: api.EventApprovalResult, ExecutionID: "a", Approval: &api.ApprovalRequest{ToolCallID: "call"}}}, 2, true},
		{"ID-less decision", []api.Event{approvalRequest("a", "call"), approvalDecision("", "call", true)}, 2, true},
		{"empty decision call", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "", true)}, 2, true},
		{"decision before request", []api.Event{approvalDecision("a", "call", true), approvalRequest("a", "call")}, 2, true},
		{"two requests one decision", []api.Event{approvalRequest("a", "one"), approvalRequest("a", "two"), approvalDecision("a", "one", true)}, 3, true},
		{"END", []api.Event{approvalRequest("a", "call"), {Kind: api.EventEnd, ExecutionID: "a", End: &api.HarnessEnd{State: "COMPLETED"}}}, 2, false},
		{"request after END stays terminal", []api.Event{{Kind: api.EventEnd, ExecutionID: "a"}, approvalRequest("a", "call")}, 2, false},
		{"ERROR does not finish", []api.Event{approvalRequest("a", "call"), {Kind: api.EventError, ExecutionID: "a"}}, 2, true},
		{"SUSPEND", []api.Event{approvalRequest("a", "call"), approvalLifecycle(api.LifecycleSuspend, "")}, 2, true},
		{"RESUME", []api.Event{approvalRequest("a", "call"), approvalLifecycle(api.LifecycleResume, "")}, 2, true},
		{"FORK", []api.Event{approvalRequest("a", "call"), approvalLifecycle(api.LifecycleFork, "")}, 2, true},
		{"new execution supersedes unresolved", []api.Event{approvalRequest("old", "call"), approvalStart("new")}, 2, false},
		{"late old request does not select old", []api.Event{approvalStart("old"), approvalStart("new"), approvalRequest("old", "call")}, 3, false},
		{"late old END does not finish new", []api.Event{approvalStart("old"), approvalRequest("new", "call"), {Kind: api.EventEnd, ExecutionID: "old"}}, 3, true},
		{"late old decision does not resolve new", []api.Event{approvalRequest("old", "call"), approvalRequest("new", "call"), approvalDecision("old", "call", false)}, 3, true},
		{"decision beyond cutoff", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "call", true)}, 1, true},
		{"request beyond cutoff", []api.Event{approvalStart("a"), approvalRequest("a", "call")}, 1, false},
		{"new execution beyond cutoff", []api.Event{approvalRequest("a", "call"), approvalStart("new")}, 1, true},
		{"zero cutoff", []api.Event{approvalRequest("a", "call")}, 0, false},
		{"same call reissued", []api.Event{approvalRequest("a", "call"), approvalDecision("a", "call", true), approvalRequest("a", "call")}, 3, true},
		{"nil request", []api.Event{{Kind: api.EventApprovalRequest, ExecutionID: "a"}}, 1, false},
		{"malformed request body", []api.Event{{Kind: api.EventApprovalRequest, ExecutionID: "a", ToolCall: &api.ToolCall{ID: "call"}}}, 1, false},
		{"wrong request kind", []api.Event{{Kind: api.EventInput, ExecutionID: "a", Approval: &api.ApprovalRequest{ToolCallID: "call"}}}, 1, false},
		{"empty execution ID", []api.Event{approvalRequest("", "call")}, 1, false},
		{"empty call ID", []api.Event{approvalRequest("a", "")}, 1, false},
		{"unseen lifecycle ID cannot select newer execution", []api.Event{approvalRequest("a", "call"), approvalLifecycle(api.LifecycleSuspend, "unseen")}, 2, true},
		{"lifecycle alone cannot produce awaiting", []api.Event{{Kind: api.EventLifecycle, ExecutionID: "unseen", Approval: &api.ApprovalRequest{ToolCallID: "call"}}}, 1, false},
		{"lifecycle does not mark execution seen", []api.Event{approvalLifecycle(api.LifecycleResume, "new"), approvalStart("old"), approvalRequest("new", "call")}, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := make([]eventlog.Record, 0, len(tc.events))
			for i, event := range tc.events {
				records = append(records, eventlog.Record{Seq: int64(i + 1), Event: event})
			}
			if got := approvalAwaiting(records, tc.through); got != tc.want {
				t.Fatalf("approvalAwaiting = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestQueryApprovalStateRestartAndPagination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store := approvalStore(t, path)
	cases := []struct {
		uid     string
		events  []api.Event
		state   v1.ExecState
		seq     int64
		compute v1.ComputeState
	}{
		{"pending", []api.Event{approvalStart("shared"), approvalRequest("shared", "shared-call"), approvalLifecycle(api.LifecycleSuspend, "")}, v1.ExecState_EXEC_AWAITING, 3, v1.ComputeState_COMPUTE_COLD},
		{"approved", []api.Event{approvalStart("shared"), approvalRequest("shared", "shared-call"), approvalDecision("shared", "shared-call", true)}, v1.ExecState_EXEC_COMPLETED, 3, v1.ComputeState_COMPUTE_LIVE},
		{"empty", nil, v1.ExecState_EXEC_PENDING, 0, v1.ComputeState_COMPUTE_NONE},
		{"ID-less", []api.Event{approvalRequest("", "shared-call")}, v1.ExecState_EXEC_COMPLETED, 1, v1.ComputeState_COMPUTE_LIVE},
		{"markerless", []api.Event{{Kind: api.EventInput, ExecutionID: "shared", Message: api.TextMessage("user", "write")}, approvalRequest("shared", "shared-call")}, v1.ExecState_EXEC_AWAITING, 2, v1.ComputeState_COMPUTE_LIVE},
		{"plain interrupted", []api.Event{{Kind: api.EventInput, ExecutionID: "shared", Message: api.TextMessage("user", "write")}}, v1.ExecState_EXEC_COMPLETED, 1, v1.ComputeState_COMPUTE_LIVE},
		{"error only", []api.Event{{Kind: api.EventError, ExecutionID: "shared", Err: &api.Error{Description: "interrupted"}}}, v1.ExecState_EXEC_COMPLETED, 1, v1.ComputeState_COMPUTE_LIVE},
	}
	before := make(map[string][]eventlog.Record)
	for _, tc := range cases {
		if err := store.PutSession(sqlitelog.SessionMeta{UID: tc.uid}); err != nil {
			t.Fatal(err)
		}
		if len(tc.events) > 0 {
			before[tc.uid] = appendApprovalEvents(t, store, tc.uid, tc.events...)
		} else {
			records, err := store.Session(tc.uid).Read(1)
			if err != nil {
				t.Fatal(err)
			}
			before[tc.uid] = records
		}
	}
	for _, phase := range []string{"original service", "reopened service"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "reopened service" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store = approvalStore(t, path)
			}
			svc := NewService(store, nil)
			want := make(map[string]int)
			for i, tc := range cases {
				want[tc.uid] = i
				got, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: tc.uid})
				if err != nil {
					t.Fatal(err)
				}
				assertApprovalSession(t, got, tc.state, tc.seq, tc.compute)
			}
			seen := make(map[string]bool)
			token := ""
			for page := 0; ; page++ {
				if page > len(cases) {
					t.Fatal("pagination did not terminate")
				}
				listed, err := svc.ListSessions(t.Context(), &v1.ListSessionsRequest{PageSize: 2, PageToken: token})
				if err != nil {
					t.Fatal(err)
				}
				for _, got := range listed.GetSessions() {
					uid := got.GetMetadata().GetUid()
					i, ok := want[uid]
					if !ok || seen[uid] {
						t.Fatalf("unexpected or repeated session %q", uid)
					}
					seen[uid] = true
					tc := cases[i]
					assertApprovalSession(t, got, tc.state, tc.seq, tc.compute)
				}
				token = listed.GetNextPageToken()
				if token == "" {
					break
				}
			}
			if len(seen) != len(cases) {
				t.Fatalf("saw %d sessions, want %d", len(seen), len(cases))
			}
			for _, tc := range cases {
				assertApprovalJournal(t, store, tc.uid, before[tc.uid])
			}
		})
	}
}

func TestQueryApprovalStateFork(t *testing.T) {
	store := approvalStore(t, ":memory:")
	backend := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = backend.Close() })
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(backend, echoagent.Model)})
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, registry)
	parent, err := svc.CreateSession(t.Context(), &v1.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	assertApprovalSession(t, parent, v1.ExecState_EXEC_PENDING, 0, v1.ComputeState_COMPUTE_NONE)
	uid := parent.GetMetadata().GetUid()
	before := appendApprovalEvents(t, store, uid,
		api.Event{Kind: api.EventExecutionStart, ExecutionID: "parent-execution", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1)}},
		api.Event{Kind: api.EventInput, ExecutionID: "parent-execution", Message: api.TextMessage("user", "write")},
		api.Event{Kind: api.EventToolCall, ExecutionID: "parent-execution", ToolCall: &api.ToolCall{ID: "call", Tool: "write", Mediation: api.MediationRequiresApproval}},
		approvalRequest("parent-execution", "call"),
		approvalDecision("parent-execution", "call", false),
	)
	for _, tc := range []struct {
		name  string
		kind  api.EventKind
		state v1.ExecState
		seq   int64
	}{
		{"before decision", api.EventApprovalRequest, v1.ExecState_EXEC_AWAITING, 5},
		{"after decision", api.EventApprovalResult, v1.ExecState_EXEC_COMPLETED, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var atSeq int64
			for _, record := range before {
				if record.Event.Kind == tc.kind {
					atSeq = record.Seq
				}
			}
			if atSeq == 0 {
				t.Fatalf("missing fork event %s", tc.kind)
			}
			forked, err := svc.Fork(t.Context(), &v1.ForkRequest{Session: uid, AtSeq: atSeq, Count: 1})
			if err != nil {
				t.Fatal(err)
			}
			if len(forked.GetChildren()) != 1 {
				t.Fatalf("fork returned %d children, want 1", len(forked.GetChildren()))
			}
			child := forked.GetChildren()[0]
			assertApprovalSession(t, child, v1.ExecState_EXEC_COMPLETED, tc.seq, v1.ComputeState_COMPUTE_COLD)
			childUID := child.GetMetadata().GetUid()
			prefix, err := store.Session(childUID).Read(1)
			if err != nil {
				t.Fatal(err)
			}
			got, err := svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: childUID})
			if err != nil {
				t.Fatal(err)
			}
			assertApprovalSession(t, got, tc.state, tc.seq, v1.ComputeState_COMPUTE_COLD)
			listed, err := svc.ListSessions(t.Context(), &v1.ListSessionsRequest{})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, sess := range listed.GetSessions() {
				if sess.GetMetadata().GetUid() == childUID {
					found = true
					assertApprovalSession(t, sess, tc.state, tc.seq, v1.ComputeState_COMPUTE_COLD)
				}
			}
			if !found {
				t.Fatal("fork child not listed")
			}
			assertApprovalJournal(t, store, childUID, prefix)
			if tc.kind == api.EventApprovalRequest {
				// A child-owned turn supersedes copied evidence; no permission or effect is inferred.
				owned := appendApprovalEvents(t, store, childUID, approvalStart("child-execution"))
				got, err = svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: childUID})
				if err != nil {
					t.Fatal(err)
				}
				assertApprovalSession(t, got, v1.ExecState_EXEC_COMPLETED, 6, v1.ComputeState_COMPUTE_LIVE)
				assertApprovalJournal(t, store, childUID, owned)
				owned = appendApprovalEvents(t, store, childUID, approvalRequest("child-execution", "call"), approvalDecision("parent-execution", "call", true))
				got, err = svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: childUID})
				if err != nil {
					t.Fatal(err)
				}
				assertApprovalSession(t, got, v1.ExecState_EXEC_AWAITING, 8, v1.ComputeState_COMPUTE_LIVE)
				assertApprovalJournal(t, store, childUID, owned)
				owned = appendApprovalEvents(t, store, childUID, approvalDecision("child-execution", "call", false))
				got, err = svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: childUID})
				if err != nil {
					t.Fatal(err)
				}
				assertApprovalSession(t, got, v1.ExecState_EXEC_COMPLETED, 9, v1.ComputeState_COMPUTE_LIVE)
				assertApprovalJournal(t, store, childUID, owned)
			}
		})
	}
	assertApprovalJournal(t, store, uid, before)
}

func TestQueryApprovalStateAtReturnedCursor(t *testing.T) {
	store := approvalStore(t, ":memory:")
	const uid = "cursor"
	appendApprovalEvents(t, store, uid, approvalStart("a"), approvalRequest("a", "call"))
	info, err := store.SessionInfo(uid)
	if err != nil {
		t.Fatal(err)
	}
	before := appendApprovalEvents(t, store, uid, approvalDecision("a", "call", true), approvalLifecycle(api.LifecycleSuspend, ""))
	svc := NewService(store, nil)
	got, err := svc.querySessionProto(info)
	if err != nil {
		t.Fatal(err)
	}
	assertApprovalSession(t, got, v1.ExecState_EXEC_AWAITING, 2, v1.ComputeState_COMPUTE_LIVE)
	got, err = svc.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	assertApprovalSession(t, got, v1.ExecState_EXEC_COMPLETED, 4, v1.ComputeState_COMPUTE_COLD)
	assertApprovalJournal(t, store, uid, before)
}

func TestQueryApprovalStateReadFailure(t *testing.T) {
	store := approvalStore(t, ":memory:")
	appendApprovalEvents(t, store, "closed", approvalRequest("a", "call"))
	info, err := store.SessionInfo("closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, nil)
	_, err = svc.querySessionProto(info)
	if status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), `session "closed"`) || !strings.Contains(status.Convert(err).Message(), "approval state") {
		t.Fatalf("query read failure = %v, want Internal with session and approval-state context", err)
	}
	// Empty journals need no read, even if the store is unavailable.
	got, err := svc.querySessionProto(sqlitelog.SessionInfo{SessionMeta: sqlitelog.SessionMeta{UID: "empty"}, ComputeState: api.ComputeNone})
	if err != nil {
		t.Fatal(err)
	}
	assertApprovalSession(t, got, v1.ExecState_EXEC_PENDING, 0, v1.ComputeState_COMPUTE_NONE)
}
