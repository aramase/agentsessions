package session_test

import (
	"context"
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
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// These are the actual public #100 host checks, not fake callbacks. Neither a supplied override
// nor a decision bypasses recorded routing, static collisions or registered-session pins.
func TestPublicApprovalRecoveryRoutingRules(t *testing.T) {
	for _, tc := range []struct {
		name, stored, recorded, row  string
		registered, retired, allowed bool
	}{
		{name: "recorded name registered after turn", stored: "default", recorded: "recorded", row: "recorded", registered: true},
		{name: "stored name registered after turn", stored: "recorded", recorded: "default", row: "recorded", registered: true},
		{name: "recorded static name collision", stored: "default", recorded: "recorded", row: "recorded"},
		{name: "stored static collision does not poison recorded override", stored: "default", recorded: "recorded", row: "default", allowed: true},
		{name: "retired registration permits existing session", stored: "recorded", recorded: "recorded", row: "recorded", registered: true, retired: true, allowed: true},
	} {
		for _, operation := range []string{"Resume", "Exec", "Suspend"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				store := openStore(t, ":memory:")
				const uid = "existing"
				if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: tc.stored}); err != nil {
					t.Fatal(err)
				}
				seedPublicGate(t, store.Session(uid), tc.recorded, api.EventApprovalRequest)
				a := &publicGateBackend{Backend: local.New(&publicGateHarness{})}
				b := &publicGateBackend{Backend: local.New(&publicGateHarness{})}
				t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
				entries := map[string]*placement.Placer{"default": placement.New(a, nil)}
				if !tc.registered {
					entries["recorded"] = placement.New(b, nil)
				}
				r, err := placement.NewRegistry("default", entries)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: tc.row, UID: "reg", Spec: "{}", SpecDigest: "digest"}); err != nil {
					t.Fatal(err)
				}
				if tc.registered {
					if err := r.Add("recorded", placement.New(b, nil)); err != nil {
						t.Fatal(err)
					}
				}
				if tc.retired {
					if _, err := store.RetireHarness(tc.row, "retired"); err != nil {
						t.Fatal(err)
					}
				}
				c := serveRegistry(t, store, r)
				before := routingRecords(t, store.Session(uid))
				var out *v1.Session
				switch operation {
				case "Resume":
					out, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
				case "Exec":
					var frames []*v1.ExecUpdate
					frames, err = collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "caller-override", Config: []byte("new"), ResumeFromSeq: 99})
					if err == nil {
						out = frames[len(frames)-1].GetSession()
					}
				case "Suspend":
					out, err = c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
				}
				want := codes.FailedPrecondition
				if tc.allowed {
					want = codes.OK
				}
				if status.Code(err) != want {
					t.Fatalf("%s=%v want %v", operation, err, want)
				}
				after := routingRecords(t, store.Session(uid))
				if !tc.allowed {
					if !reflect.DeepEqual(before, after) || a.ioCounts() != ([4]int32{}) || b.ioCounts() != ([4]int32{}) {
						t.Fatal("refused callback changed journal/compute")
					}
					if !strings.Contains(err.Error(), "pinned") && !strings.Contains(err.Error(), "static harness") {
						t.Fatalf("host callback cause lost: %v", err)
					}
				} else {
					if out.GetPendingApproval().GetRequestSeq() != 3 || out.GetHarness() != tc.stored || out.GetExecState() != v1.ExecState_EXEC_AWAITING {
						t.Fatalf("allowed existing response=%v", out)
					}
					if operation == "Suspend" {
						if a.ioCounts() != ([4]int32{}) || b.snapshots.Load() != 1 || out.ComputeState != v1.ComputeState_COMPUTE_COLD {
							t.Fatal("Suspend did not use recorded route")
						}
					} else if !reflect.DeepEqual(before, after) || a.ioCounts() != ([4]int32{}) || b.ioCounts() != ([4]int32{}) {
						t.Fatal("undecided existing recovery used IO")
					}
				}
				// Approve intentionally runs no routing check; decisions remain durable even if the
				// host's recovery rules refuse the original route.
				if _, err := c.Approve(t.Context(), publicDecision(uid)); err != nil {
					t.Fatalf("decision depends on host route: %v", err)
				}
				if !tc.allowed {
					decided := routingRecords(t, store.Session(uid))
					switch operation {
					case "Resume":
						_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
					case "Exec":
						_, err = collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "caller-override"})
					case "Suspend":
						_, err = c.Suspend(t.Context(), &v1.SuspendRequest{Session: uid})
					}
					if status.Code(err) != codes.FailedPrecondition || !reflect.DeepEqual(decided, routingRecords(t, store.Session(uid))) || a.ioCounts() != ([4]int32{}) || b.ioCounts() != ([4]int32{}) {
						t.Fatalf("decision bypassed host routing refusal: %v", err)
					}
				}
				if tc.retired {
					if _, err := c.CreateSession(t.Context(), &v1.CreateSessionRequest{Session: &v1.Session{Harness: tc.row}}); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("retirement allowed new session: %v", err)
					}
				}
			})
		}
	}
}

func TestPublicApprovalDecidedRecordedVersionAndDescriptorPin(t *testing.T) {
	for _, tc := range []struct {
		name, version, descriptor string
		allowed                   bool
	}{
		{name: "exact version", version: "v1", allowed: true},
		{name: "version changed", version: "v2"},
		{name: "registration descriptor pin", version: "v1", descriptor: "expected-other-id"},
	} {
		for _, operation := range []string{"Resume", "Exec"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				store := openStore(t, ":memory:")
				if err := store.PutSession(sqlitelog.SessionMeta{UID: "s", Harness: "gate"}); err != nil {
					t.Fatal(err)
				}
				seedPublicGate(t, store.Session("s"), "gate", api.EventApprovalRequest)
				h := &publicGateHarness{version: tc.version, starts: make(chan api.Start, 1)}
				b := &publicGateBackend{Backend: local.New(h)}
				t.Cleanup(func() { _ = b.Close() })
				opts := []placement.Option{}
				if tc.descriptor != "" {
					opts = append(opts, placement.WithDescriptorID(tc.descriptor))
				}
				r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": placement.New(b, nil, opts...)})
				if err != nil {
					t.Fatal(err)
				}
				c := serveRegistry(t, store, r)
				if _, err := c.Approve(t.Context(), publicDecision("s")); err != nil {
					t.Fatal(err)
				}
				before := routingRecords(t, store.Session("s"))
				if operation == "Resume" {
					_, err = c.Resume(t.Context(), &v1.ResumeRequest{Session: "s"})
				} else {
					_, err = collectPublicExec(c, &v1.ExecRequest{Session: "s", Harness: "wrong", Config: []byte("wrong"), ResumeFromSeq: 999})
				}
				want := codes.FailedPrecondition
				if tc.allowed {
					want = codes.OK
				}
				if status.Code(err) != want {
					t.Fatalf("decided recovery=%v want %v", err, want)
				}
				if !tc.allowed {
					if h.runs.Load() != 0 || !reflect.DeepEqual(before, routingRecords(t, store.Session("s"))) {
						t.Fatal("refused version/id wrote execution records or invoked harness")
					}
				} else {
					start := <-h.starts
					if string(start.Config) != "original" || start.ResumeFromSeq != 7 || start.ExecutionID != "execution" || len(start.Inputs) != 0 || b.restores.Load() != 1 {
						t.Fatalf("recorded recovery=%+v restores=%d", start, b.restores.Load())
					}
				}
			})
		}
	}
}

func TestPublicApprovalDecidedStaticOverrideUsesRecordedInvocation(t *testing.T) {
	store := openStore(t, ":memory:")
	a := &publicGateBackend{Backend: local.New(&publicGateHarness{})}
	h := &publicGateHarness{starts: make(chan api.Start, 2)}
	b := &publicGateBackend{Backend: local.New(h)}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	r, err := placement.NewRegistry("default", map[string]*placement.Placer{
		"default": placement.New(a, nil), "recorded": placement.New(b, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	frames, err := collectPublicExec(c, &v1.ExecRequest{
		Session: uid, Harness: "recorded", Config: []byte("original"), ResumeFromSeq: 7,
		Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "write"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	paused := frames[len(frames)-1].GetSession()
	ref := paused.GetPendingApproval()
	if _, err := c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(false)}); err != nil {
		t.Fatal(err)
	}
	// The session's stored static name can collide while its recorded static override still recovers.
	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "default", UID: "reg", Spec: "{}", SpecDigest: "digest"}); err != nil {
		t.Fatal(err)
	}
	after, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Harness: "unserved", Config: []byte("wrong"), ResumeFromSeq: 999})
	if err != nil {
		t.Fatal(err)
	}
	current := after[len(after)-1].GetSession()
	first, recovered := <-h.starts, <-h.starts
	if current.GetHarness() != "default" || current.PendingApproval != nil || recovered.ExecutionID != first.ExecutionID || string(recovered.Config) != "original" || recovered.ResumeFromSeq != 7 || len(recovered.Inputs) != 1 || recovered.Inputs[0].Text() != "write" || a.ioCounts() != ([4]int32{}) || b.restores.Load() != 1 {
		t.Fatalf("static override recovery rewrote route/invocation: current=%v recovered=%+v default IO=%v", current, recovered, a.ioCounts())
	}
}

func TestPublicApprovalSecondGateAndSessionScopedKeys(t *testing.T) {
	store := openStore(t, ":memory:")
	h := &publicGateHarness{second: true}
	var effects atomic.Int32
	var scopes []string
	r, _ := publicGateRegistry(t, h, func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		effects.Add(1)
		scopes = append(scopes, scope.SessionUID+":"+call.IdempotencyKey)
		return api.ToolResult{Output: map[string]any{"ok": true}}, nil
	})
	c := serveRegistry(t, store, r)
	uids := []string{mustCreate(t, c), mustCreate(t, c)}
	for _, uid := range uids {
		frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{{Role: "user"}}})
		if err != nil {
			t.Fatal(err)
		}
		paused := frames[len(frames)-1].GetSession()
		ref := paused.GetPendingApproval()
		q := &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(true)}
		original, err := c.Approve(t.Context(), q)
		if err != nil {
			t.Fatal(err)
		}
		after, err := collectPublicExec(c, &v1.ExecRequest{Session: uid})
		if err != nil {
			t.Fatalf("second gate=%v", err)
		}
		next := after[len(after)-1].GetSession()
		if next.GetPendingApproval().GetToolCallId() != "second" || next.GetPendingApproval().GetRequestSeq() <= ref.RequestSeq || next.ExecState != v1.ExecState_EXEC_AWAITING || next.ComputeState != v1.ComputeState_COMPUTE_COLD {
			t.Fatalf("second pause=%v", next)
		}
		beforeRecovery := original.Session.LastSeq
		recs, err := store.Session(uid).Read(beforeRecovery + 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range recs {
			if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Lifecycle != nil && rec.Event.Lifecycle.Kind == api.LifecycleResume {
				t.Fatalf("second gate fabricated terminal/Resume: %+v", rec.Event)
			}
		}
		// A completed first request remains exactly retryable while the new gate is pending.
		retry, err := c.Approve(t.Context(), q)
		if err != nil || !proto.Equal(retry.GetDecision(), original.Decision) || !proto.Equal(retry.GetSession().PendingApproval, next.PendingApproval) {
			t.Fatalf("old retry changed second gate: %v %v", retry, err)
		}
	}
	if effects.Load() != 2 || !reflect.DeepEqual(scopes, []string{uids[0] + ":shared-key", uids[1] + ":shared-key"}) {
		t.Fatalf("session-scoped effects=%d scope/key=%v", effects.Load(), scopes)
	}
}
