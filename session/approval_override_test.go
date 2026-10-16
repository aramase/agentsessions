package session_test

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
	"github.com/agent-primitives/sessions/runtime/remote"
	"github.com/agent-primitives/sessions/runtime/substrate"
)

// An override must not park only B while A still holds the session's live memory.
func TestApprovalOverrideRefusesLiveMemorySession(t *testing.T) {
	checkMemoryOverrideRefusal(t, "Exec")
}

func TestResumeMemoryOverrideRefusal(t *testing.T) {
	for _, operation := range []string{"Resume", "recovering Exec"} {
		t.Run(operation, func(t *testing.T) { checkMemoryOverrideRefusal(t, operation) })
	}
}

func checkMemoryOverrideRefusal(t *testing.T, operation string) {
	t.Helper()
	store := openStore(t, ":memory:")
	ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
	desc, err := (memHarness{}).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	memory := substrate.New(ctl, "space", substrate.ObjectRef{Name: "memory"}, desc)
	h := &publicGateHarness{}
	override := &publicGateBackend{Backend: local.New(h)}
	t.Cleanup(func() { _ = override.Close() })
	r, err := placement.NewRegistry("memory", map[string]*placement.Placer{
		"memory": placement.New(memory, nil, placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
			return memHarness{}, func() error { return nil }, nil
		})),
		"override": placement.New(override, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	if operation == "Exec" {
		if _, err := collectPublicExec(c, &v1.ExecRequest{Session: uid}); err != nil {
			t.Fatalf("initial memory turn: %v", err)
		}
	} else {
		// v0.1.2's no-input memory turn created A and wrote only END: no start marker
		// or execution ID. A later main writer can append a named override to that prefix.
		if _, err := memory.Create(t.Context(), &api.SessionSpec{SessionUID: uid}); err != nil {
			t.Fatal(err)
		}
		log := store.Session(uid)
		fence, err := log.NewFence()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := log.Append(0, fence, api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}}); err != nil {
			t.Fatal(err)
		}
	}
	actor := substrate.ActorRef{Atespace: "space", Name: uid}
	if info, err := ctl.GetActor(t.Context(), actor); err != nil || info.Status != substrate.StatusRunning {
		t.Fatalf("initial actor=%+v, %v", info, err)
	}
	log := store.Session(uid)
	if operation != "Exec" {
		// Main before this refusal admitted B. Its interrupted invocation has no approval
		// records yet: recovery would run B's tail and hit its first gate. v0.1.2 itself
		// predates named start markers and retains the stored-default fallback instead.
		fence, err := log.NewFence()
		if err != nil {
			t.Fatal(err)
		}
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		event := api.Event{Kind: api.EventExecutionStart, ExecutionID: "interrupted-override", ExecutionStart: &api.ExecutionStart{
			Harness: "override", HarnessVersion: "v1", InputCount: proto.Int64(0),
		}}
		if _, err := log.Append(head, fence, event); err != nil {
			t.Fatal(err)
		}
		if operation == "recovering Exec" {
			// No-input Exec also recovers owned approval calls through Registry.Resume.
			call := publicGateCall("call")
			if _, err := log.Append(head+1, fence, api.Event{Kind: api.EventToolCall, ExecutionID: event.ExecutionID, ToolCall: &call}); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || before.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
		t.Fatalf("initial session=%v, %v", before, err)
	}
	records := routingRecords(t, log)
	fence := records[len(records)-1].Fence
	switch operation {
	case "Resume":
		_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	case "recovering Exec":
		_, err = collectPublicExec(c, &v1.ExecRequest{Session: uid})
	default:
		_, err = collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "override"})
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("memory session override: %v, want FailedPrecondition", err)
	}
	if !reflect.DeepEqual(records, routingRecords(t, log)) {
		t.Fatal("refused override changed the journal")
	}
	after, err := c.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
	if err != nil || !proto.Equal(before, after) {
		t.Fatalf("refusal changed session projection: before=%v after=%v err=%v", before, after, err)
	}
	// A fence changes independently of the head; the next token must be its immediate successor.
	if next, err := log.NewFence(); err != nil || next != fence+1 {
		t.Fatalf("refused override advanced the fence: next=%d, previous=%d, err=%v", next, fence, err)
	}
	if h.runs.Load() != 0 || override.creates.Load() != 0 || override.restores.Load() != 0 || override.snapshots.Load() != 0 {
		t.Fatal("refused override ran or changed B's compute")
	}
	if info, err := ctl.GetActor(t.Context(), actor); err != nil || info.Status != substrate.StatusRunning || info.Snapshot != "" {
		t.Fatalf("refusal changed A's live actor: %+v, %v", info, err)
	}
	if operation == "recovering Exec" {
		// Refusal leaves the pre-existing approval intent and its route untouched.
		return
	}
	// With no B approval written, Suspend must retain A's route and checkpoint its memory.
	suspended, err := c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
	if err != nil || suspended.GetComputeState() != v1.ComputeState_COMPUTE_COLD {
		t.Fatalf("Suspend=%v, %v", suspended, err)
	}
	info, err := ctl.GetActor(t.Context(), actor)
	if err != nil || info.Status != substrate.StatusSuspended || info.Snapshot == "" {
		t.Fatalf("Suspend did not checkpoint A: %+v, %v", info, err)
	}
	var snapshot *api.SnapshotRef
	for _, rec := range routingRecords(t, log) {
		if rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
			snapshot = rec.Event.Lifecycle.Snapshot
		}
	}
	if snapshot == nil || !snapshot.Memory || override.snapshots.Load() != 0 {
		t.Fatalf("Suspend snapshot=%+v, B snapshots=%d; want A memory checkpoint", snapshot, override.snapshots.Load())
	}
}

// A remote runtime cannot hold memory; its unavailable descriptor must not block B.
func TestApprovalOverrideWithDownRemoteStoredHarness(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	stored := &countedRemoteOverrideBackend{Backend: remote.New(address)}
	t.Cleanup(func() { _ = stored.Close() })
	h := &publicGateHarness{}
	b := &publicGateBackend{Backend: local.New(h)}
	t.Cleanup(func() { _ = b.Close() })
	r, err := placement.NewRegistry("remote", map[string]*placement.Placer{
		"remote": placement.New(stored, nil), "override": placement.New(b, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := serveRegistry(t, openStore(t, ":memory:"), r)
	uid := mustCreate(t, c)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := c.Exec(ctx, &v1.ExecRequest{Session: uid, Harness: "override"})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainExec(stream); err != nil {
		t.Fatalf("stateless override with down stored harness: %v", err)
	}
	sess, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
	if err != nil || sess.GetHarness() != "remote" || sess.GetExecState() != v1.ExecState_EXEC_AWAITING {
		t.Fatalf("override session=%v, %v; want stored remote with B's pending gate", sess, err)
	}
	// Recovery applies the same override rule but must also leave the down stored peer alone.
	if _, err := c.Resume(ctx, &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("Resume pending stateless override with down stored harness: %v", err)
	}
	if stored.describes.Load() != 0 || h.runs.Load() != 1 || b.snapshots.Load() != 1 {
		t.Fatalf("stored Describe=%d, B runs=%d snapshots=%d", stored.describes.Load(), h.runs.Load(), b.snapshots.Load())
	}
}

type countedRemoteOverrideBackend struct {
	*remote.Backend
	describes atomic.Int32
}

func (b *countedRemoteOverrideBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	b.describes.Add(1)
	return b.Backend.Describe(ctx)
}

// An older interrupted turn that overrode a stateless session onto a memory-tier harness, cut
// after its gated tool call and before the approval request, is refused on recovery before that
// request is repaired: Resume and no-input Exec leave the journal, the fence and both harnesses'
// compute unchanged.
func TestRecoveryRefusesMemoryOverrideBeforeApprovalRepair(t *testing.T) {
	for _, operation := range []string{"Resume", "recovering Exec"} {
		t.Run(operation, func(t *testing.T) {
			store := openStore(t, ":memory:")
			ctl := &actorControl{status: map[string]substrate.ActorStatus{}, snapshots: map[string]string{}}
			desc, err := (memHarness{}).Describe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			memory := substrate.New(ctl, "space", substrate.ObjectRef{Name: "memory"}, desc)
			h := &publicGateHarness{}
			stateless := &publicGateBackend{Backend: local.New(h)}
			t.Cleanup(func() { _ = stateless.Close() })
			r, err := placement.NewRegistry("stateless", map[string]*placement.Placer{
				"stateless": placement.New(stateless, nil),
				"memory": placement.New(memory, nil, placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
					return memHarness{}, func() error { return nil }, nil
				})),
			})
			if err != nil {
				t.Fatal(err)
			}
			c := serveRegistry(t, store, r)
			uid := mustCreate(t, c)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			head, err := log.Head()
			if err != nil {
				t.Fatal(err)
			}
			call := publicGateCall("call")
			for i, ev := range []api.Event{
				{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "memory", HarnessVersion: "v1", InputCount: proto.Int64(0)}},
				{Kind: api.EventToolCall, ToolCall: &call},
			} {
				ev.ExecutionID = "interrupted-override"
				if _, err := log.Append(head+int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			records := routingRecords(t, log)

			if operation == "Resume" {
				_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			} else {
				_, err = collectPublicExec(c, &v1.ExecRequest{Session: uid})
			}
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), placement.ErrStatefulOverride.Error()) {
				t.Fatalf("recovery of a memory-tier override: %v, want FailedPrecondition with ErrStatefulOverride", err)
			}
			if !reflect.DeepEqual(records, routingRecords(t, log)) {
				t.Fatal("refused recovery repaired the approval request")
			}
			if next, err := log.NewFence(); err != nil || next != fence+1 {
				t.Fatalf("refused recovery advanced the fence: next=%d, previous=%d, err=%v", next, fence, err)
			}
			if h.runs.Load() != 0 || stateless.creates.Load() != 0 || stateless.restores.Load() != 0 {
				t.Fatal("refused recovery ran or changed the stateless harness's compute")
			}
			ctl.mu.Lock()
			defer ctl.mu.Unlock()
			if len(ctl.status) != 0 {
				t.Fatalf("refused recovery created or restored a memory actor: %v", ctl.status)
			}
		})
	}
}
