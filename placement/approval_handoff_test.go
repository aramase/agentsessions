package placement_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/substrate"
)

func TestPlacementApprovalHandoffFailuresPreserveCause(t *testing.T) {
	for _, mode := range []string{"request", "close", "snapshot", "append", "CAS", "fenced", "transport", "mismatched park"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New(mode + " failed")
			ctl := newSuspendControl()
			log := &approvalFaultLog{Store: newSuspendStore(t).Session("s")}
			har := placementGateHarness{}
			want := cause
			if mode == "request" {
				log.failKind, log.err = api.EventApprovalRequest, cause
			}
			if mode == "append" {
				log.failSuspend, log.err = true, cause
			}
			if mode == "snapshot" {
				ctl.snapshotErr = cause
			}
			if mode == "transport" || mode == "mismatched park" {
				har.run = func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					_, _ = sink.ToolCall(ctx, placementGateCall())
					if mode == "mismatched park" {
						return &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "wrong", ToolCallID: "c1", RequestSeq: 3}}
					}
					return cause
				}
				if mode == "mismatched park" {
					want = controller.ErrReplayDiverged
				}
			}
			desc, _ := har.Describe(t.Context())
			b := &approvalBackend{Backend: substrate.New(ctl, "space", substrate.ObjectRef{Name: "gate"}, desc)}
			closes := 0
			p := placement.New(b, nil, placement.WithDialer(func(string) (api.Harness, func() error, error) {
				return har, func() error {
					closes++
					if mode == "close" {
						return cause
					}
					return nil
				}, nil
			}))
			if mode == "CAS" || mode == "fenced" {
				ctl.afterSuspend = func() {
					if mode == "fenced" {
						if _, err := log.Store.NewFence(); err != nil {
							t.Fatal(err)
						}
						return
					}
					request := placementApprovalRecord(t, log, api.EventApprovalRequest)
					if _, err := log.Store.Append(request.Seq, request.Fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleCancel}}); err != nil {
						t.Fatal(err)
					}
				}
				want = eventlog.ErrConflict
				if mode == "fenced" {
					want = eventlog.ErrFenced
				}
			}
			_, err := p.Exec(t.Context(), log, "s", nil, 0)
			if !errors.Is(err, want) || errors.Is(err, api.ErrApprovalParked) {
				t.Fatalf("handoff=%v, want operational %v only", err, want)
			}
			if closes != 1 || b.stops != 0 || log.fences != 1 {
				t.Fatalf("close=%d stop=%d fences=%d", closes, b.stops, log.fences)
			}
			state, err := controller.InspectApproval(log, 0)
			if err != nil || state == nil || state.Receipt != nil || state.Decision != nil {
				t.Fatalf("lost awaiting evidence: %+v, %v", state, err)
			}
			if (state.Request == nil) != (mode == "request") {
				t.Fatalf("request=%+v", state.Request)
			}
			for _, rec := range suspendRecords(t, log) {
				if rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle.Kind == api.LifecycleSuspend || rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError {
					t.Fatalf("false handoff record: %+v", rec)
				}
			}
			wantSnapshots := 1
			if mode == "request" || mode == "close" || mode == "transport" || mode == "mismatched park" {
				wantSnapshots = 0
			}
			if b.snapshots != wantSnapshots {
				t.Fatalf("snapshot=%d want=%d", b.snapshots, wantSnapshots)
			}
			// An operational failure releases the local guard. A public Suspend retry keeps the
			// retained actor; no invocation fence refresh is allowed during the automatic attempt.
			ctl.snapshotErr, log.failSuspend, log.failKind = nil, false, ""
			if _, err := p.Suspend(t.Context(), log, "s"); err != nil {
				t.Fatalf("retry Suspend=%v", err)
			}
		})
	}
}

// The registry's decision endpoint must work when the recorded harness is unserved and the
// available route has no usable runtime. It only guards and journals a decision.
func TestRegistryApprovalDecisionOnly(t *testing.T) {
	log := &approvalFaultLog{Store: newSuspendStore(t).Session("s")}
	seedPlacementApproval(t, log.Store, "unserved", api.EventApprovalRequest)
	p, b := newApprovalNoIOPlacer(t)
	r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": p})
	if err != nil {
		t.Fatal(err)
	}
	decision := api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 3, Approved: true, Reason: "reviewed"}
	record, err := r.Approve(t.Context(), log, "s", decision)
	if err != nil || record.Event.Kind != api.EventApprovalResult {
		t.Fatalf("Approve=%+v,%v", record, err)
	}
	retry, err := r.Approve(t.Context(), log, "s", decision)
	if err != nil || !reflect.DeepEqual(record, retry) || log.fences != 1 {
		t.Fatalf("exact retry=%+v,%v fences=%d", retry, err, log.fences)
	}
	decision.Approved = false
	if _, err := r.Approve(t.Context(), log, "s", decision); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
		t.Fatalf("changed retry=%v", err)
	}
	b.assertNoCompute(t)
}

func TestRegistryApprovalSuspendRecordedRetry(t *testing.T) {
	for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest, api.EventApprovalResult} {
		t.Run(string(cut), func(t *testing.T) {
			ctl := newSuspendControl()
			ctl.snapshotErr = errors.New("snapshot unavailable")
			desc, _ := (placementGateHarness{}).Describe(t.Context())
			b := &approvalBackend{Backend: substrate.New(ctl, "space", substrate.ObjectRef{Name: "gate"}, desc)}
			recorded := placement.New(b, nil, placement.WithDialer(func(string) (api.Harness, func() error, error) {
				return placementGateHarness{}, func() error { return nil }, nil
			}))
			def, db := newApprovalNoIOPlacer(t)
			r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": def, "recorded": recorded})
			if err != nil {
				t.Fatal(err)
			}
			log := newSuspendStore(t).Session("s")
			if cut == api.EventToolCall {
				// Provision the real actor then construct the precise crash cut.
				if _, err := b.Backend.Create(t.Context(), &api.SessionSpec{SessionUID: "s"}); err != nil {
					t.Fatal(err)
				}
				seedPlacementApproval(t, log, "recorded", cut)
			} else {
				if _, err := recorded.Exec(t.Context(), log, "s", nil, 0, placement.WithHarness("recorded")); !errors.Is(err, ctl.snapshotErr) {
					t.Fatalf("Exec=%v", err)
				}
				if cut == api.EventApprovalResult {
					request := placementApprovalRecord(t, log, api.EventApprovalRequest)
					if _, err := r.Approve(t.Context(), log, "s", api.ApprovalDecision{ExecutionID: request.Event.ExecutionID, ToolCallID: "c1", RequestSeq: request.Seq, Approved: true}); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctl.snapshotErr = nil
			checked := false
			ref, err := r.Suspend(t.Context(), log, "s", "default", placement.WithResolvedHarnessCheck(func(name string) error {
				checked = true
				if name != "recorded" {
					t.Fatalf("route=%s", name)
				}
				return nil
			}))
			if err != nil || !checked || ref.Local != "s" {
				t.Fatalf("Suspend=%+v,%v check=%v", ref, err, checked)
			}
			db.assertNoCompute(t)
			assertSuspendRef(t, log, ref)
			if b.stops != 0 {
				t.Fatal("Suspend destroyed actor")
			}
		})
	}
}

func TestRegistryApprovalSuspendRefusalBeforeIO(t *testing.T) {
	for _, mode := range []string{"unserved", "pin", "static collision"} {
		t.Run(mode, func(t *testing.T) {
			log := newSuspendStore(t).Session("s")
			seedPlacementApproval(t, log, "recorded", api.EventApprovalRequest)
			p, b := newApprovalNoIOPlacer(t)
			entries := map[string]*placement.Placer{"default": p}
			if mode != "unserved" {
				entries["recorded"] = p
			}
			r, err := placement.NewRegistry("default", entries)
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New(mode)
			if mode == "unserved" {
				cause = placement.ErrRecordedHarnessNotServed
			}
			_, err = r.Suspend(t.Context(), log, "s", "default", placement.WithResolvedHarnessCheck(func(name string) error {
				if name != "recorded" {
					t.Fatalf("check=%s", name)
				}
				return cause
			}))
			if !errors.Is(err, cause) || mode != "unserved" && err != cause {
				t.Fatalf("Suspend=%v want=%v", err, cause)
			}
			b.assertNoCompute(t)
			assertNoApprovalLifecycle(t, log)
		})
	}
}

func TestPlacementApprovalResumeObserverSecondPark(t *testing.T) {
	for _, operation := range []string{"Registry", "Placer"} {
		t.Run(operation, func(t *testing.T) {
			log := &approvalFaultLog{Store: newSuspendStore(t).Session("s")}
			har := placementGateHarness{run: func(ctx context.Context, start *api.Start, sink api.EventSink) error {
				if string(start.Config) != "opaque" || start.ResumeFromSeq != 7 {
					t.Fatalf("lost invocation: %+v", start)
				}
				result, err := sink.ToolCall(ctx, placementGateCall())
				if err != nil {
					return err
				}
				if result.ID != "c1" {
					t.Fatalf("receipt=%+v", result)
				}
				if err := sink.Output(ctx, "first receipt"); err != nil {
					return err
				}
				second := placementGateCall()
				second.ID, second.IdempotencyKey = "c2", "key2"
				_, err = sink.ToolCall(ctx, second)
				return err
			}}
			runtime := local.New(har)
			t.Cleanup(func() { _ = runtime.Close() })
			b := &approvalBackend{Backend: runtime}
			effects := 0
			p := placement.New(b, nil, placement.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
				effects++
				if scope.SessionUID != "s" || call.ID != "c1" || call.IdempotencyKey != "key" {
					t.Fatalf("scope/call=%+v/%+v", scope, call)
				}
				return api.ToolResult{Output: map[string]any{"receipt": "done"}}, nil
			}))
			r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Exec(t.Context(), log, "s", nil, 0, placement.WithHarness("recorded"), placement.WithStart([]byte("opaque"), 7)); !errors.Is(err, api.ErrApprovalParked) {
				t.Fatal(err)
			}
			request := placementApprovalRecord(t, log, api.EventApprovalRequest)
			if _, err := r.Approve(t.Context(), log, "s", api.ApprovalDecision{ExecutionID: request.Event.ExecutionID, ToolCallID: "c1", RequestSeq: request.Seq, Approved: true}); err != nil {
				t.Fatal(err)
			}
			before, _ := log.Head()
			var observed []eventlog.Record
			observer := controller.Observer{OnRecord: func(rec eventlog.Record) { observed = append(observed, rec) }}
			if operation == "Registry" {
				err = r.Resume(t.Context(), log, "s", "recorded", placement.WithRegistryResumeObserver(observer))
			} else {
				err = p.Resume(t.Context(), log, "s", placement.WithResumeHarness("recorded"), placement.WithResumeObserver(observer))
			}
			var park *api.ApprovalParkedError
			if !errors.As(err, &park) || park.Ref.ToolCallID != "c2" || park.Ref.RequestSeq <= request.Seq {
				t.Fatalf("second park=%v", err)
			}
			after, _ := log.Read(before + 1)
			if !reflect.DeepEqual(observed, after) {
				t.Fatalf("observer=%v, committed=%v", observed, after)
			}
			for _, rec := range after {
				if rec.Fence != after[0].Fence || rec.Fence == request.Fence {
					t.Fatal("automatic handoff did not retain recovery fence")
				}
				if rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle.Kind == api.LifecycleResume || rec.Event.Kind == api.EventEnd {
					t.Fatal("second park falsely finished recovery")
				}
			}
			if effects != 1 || b.snapshots != 2 || b.restores != 1 || b.stops != 0 || log.fences != 3 {
				t.Fatalf("effects=%d snapshots=%d restores=%d stops=%d fences=%d", effects, b.snapshots, b.restores, b.stops, log.fences)
			}
			// Denial recovers the second gate without re-driving the completed first effect.
			if _, err := r.Approve(t.Context(), log, "s", api.ApprovalDecision{ExecutionID: park.Ref.ExecutionID, ToolCallID: "c2", RequestSeq: park.Ref.RequestSeq, Approved: false}); err != nil {
				t.Fatal(err)
			}
			observed = nil
			before, _ = log.Head()
			if err := r.Resume(t.Context(), log, "s", "recorded", placement.WithRegistryResumeObserver(observer)); err != nil {
				t.Fatalf("decided recovery=%v", err)
			}
			after, _ = log.Read(before + 1)
			if !reflect.DeepEqual(observed, after) {
				t.Fatalf("successful observer=%v, committed=%v", observed, after)
			}
			if effects != 1 {
				t.Fatalf("completed effect redriven: %d", effects)
			}
		})
	}
}

func TestPlacementApprovalAutoHandoffContention(t *testing.T) {
	for _, outer := range []string{"Exec", "Registry.Resume"} {
		for _, outcome := range []string{"success", "close failure", "snapshot failure", "backend Aborted", "append failure", "head advanced", "fence superseded"} {
			t.Run(outer+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				entered := make(chan string)
				advance := make(chan struct{})
				pause := func(phase string) {
					select {
					case entered <- phase:
					case <-ctx.Done():
						return
					}
					select {
					case <-advance:
					case <-ctx.Done():
					}
				}
				ctl := newSuspendControl()
				log := &approvalFaultLog{Store: newSuspendStore(t).Session("s")}
				cause := errors.New(outcome)
				var want error = api.ErrApprovalParked
				phases := []string{"close", "snapshot", "append"}
				switch outcome {
				case "close failure":
					want, phases = cause, phases[:1]
				case "snapshot failure":
					ctl.snapshotErr = cause
					want, phases = cause, phases[:2]
				case "backend Aborted":
					cause = status.Error(codes.Aborted, "snapshot backend conflict")
					ctl.snapshotErr = cause
					want, phases = cause, phases[:2]
				case "append failure":
					log.failSuspend, log.err, want = true, cause, cause
				case "head advanced":
					want = eventlog.ErrConflict
				case "fence superseded":
					want = eventlog.ErrFenced
				}
				har := placementGateHarness{run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					if _, err := sink.ToolCall(ctx, placementGateCall()); err != nil {
						return err
					}
					second := placementGateCall()
					second.ID, second.IdempotencyKey = "c2", "key2"
					_, err := sink.ToolCall(ctx, second)
					return err
				}}
				desc, err := har.Describe(ctx)
				if err != nil {
					t.Fatal(err)
				}
				b := &approvalBackend{Backend: substrate.New(ctl, "space", substrate.ObjectRef{Name: "gate"}, desc)}
				closes := 0
				recorded := placement.New(b, nil,
					placement.WithDialer(func(string) (api.Harness, func() error, error) {
						return har, func() error {
							closes++
							pause("close")
							if outcome == "close failure" {
								return cause
							}
							return nil
						}, nil
					}),
					placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
						return api.ToolResult{Output: map[string]any{"receipt": "done"}}, nil
					}),
				)
				other, otherBackend := newApprovalNoIOPlacer(t)
				r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": other, "recorded": recorded})
				if err != nil {
					t.Fatal(err)
				}
				if outer == "Registry.Resume" {
					if _, err := b.Backend.Create(ctx, &api.SessionSpec{SessionUID: "s"}); err != nil {
						t.Fatal(err)
					}
					seedPlacementApproval(t, log.Store, "recorded", api.EventApprovalResult)
				}
				b.beforeSnapshot = func() { pause("snapshot") }
				log.beforeSuspend = func() { pause("append") }
				before := mustApprovalHead(t, log)
				type result struct {
					inc api.Incarnation
					err error
				}
				done := make(chan result, 1)
				finished := false
				defer func() {
					cancel()
					if !finished {
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							t.Error("automatic handoff goroutine did not drain")
						}
					}
				}()
				go func() {
					if outer == "Exec" {
						inc, err := recorded.Exec(ctx, log, "s", nil, before, placement.WithHarness("recorded"))
						done <- result{inc: inc, err: err}
					} else {
						done <- result{err: r.Resume(ctx, log, "s", "default")}
					}
				}()
				var captured *controller.ApprovalState
				for _, phase := range phases {
					select {
					case got := <-entered:
						if got != phase {
							t.Fatalf("handoff phase=%s, want %s", got, phase)
						}
					case result := <-done:
						finished = true
						t.Fatalf("handoff returned before %s: %v", phase, result.err)
					case <-ctx.Done():
						t.Fatalf("handoff did not reach %s: %v", phase, ctx.Err())
					}
					if closes != 1 {
						t.Fatalf("%s reached before exactly one dial close: %d", phase, closes)
					}
					state, err := controller.InspectApproval(log.Store, 0)
					if err != nil || state == nil || state.Request == nil || state.Decision != nil || state.Receipt != nil {
						t.Fatalf("handoff lost pending request at %s: %+v, %v", phase, state, err)
					}
					if captured == nil {
						captured = state
					}
					prior := suspendRecords(t, log.Store)
					operations := []struct {
						name string
						run  func() error
					}{
						{"Exec", func() error { _, err := other.Exec(ctx, log, "s", nil, 0); return err }},
						{"Resume", func() error { return other.Resume(ctx, log, "s") }},
						{"Suspend", func() error { _, err := other.Suspend(ctx, log, "s"); return err }},
						{"Registry.Resume", func() error { return r.Resume(ctx, log, "s", "unserved") }},
						{"Registry.Suspend", func() error { _, err := r.Suspend(ctx, log, "s", "unserved"); return err }},
						{"Registry.Approve", func() error { _, err := r.Approve(ctx, log, "s", api.ApprovalDecision{}); return err }},
					}
					for _, op := range operations {
						if err := op.run(); !errors.Is(err, placement.ErrSessionBusy) {
							t.Fatalf("%s during %s/%s=%v, want ErrSessionBusy", op.name, outer, phase, err)
						}
					}
					otherBackend.assertNoCompute(t)
					if !reflect.DeepEqual(prior, suspendRecords(t, log.Store)) {
						t.Fatalf("contention changed the journal at %s", phase)
					}
					if phase == "snapshot" {
						if b.snapshots != 1 || b.stops != 0 {
							t.Fatalf("snapshot IO=%d stop=%d", b.snapshots, b.stops)
						}
						switch outcome {
						case "head advanced":
							if _, err := log.Store.Append(captured.Head, captured.Request.Fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleCancel}}); err != nil {
								t.Fatal(err)
							}
						case "fence superseded":
							if _, err := log.Store.NewFence(); err != nil {
								t.Fatal(err)
							}
						}
					}
					select {
					case advance <- struct{}{}:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				var got result
				select {
				case got = <-done:
					finished = true
				case extra := <-entered:
					t.Fatalf("failed handoff unexpectedly reached %s", extra)
				case <-ctx.Done():
					t.Fatalf("handoff did not finish: %v", ctx.Err())
				}
				if !errors.Is(got.err, want) || errors.Is(got.err, placement.ErrSessionBusy) {
					t.Fatalf("handoff=%v, want %v", got.err, want)
				}
				if outcome == "success" {
					var park *api.ApprovalParkedError
					if !errors.As(got.err, &park) || park.Ref != (api.ApprovalRef{ExecutionID: captured.ExecutionID, ToolCallID: captured.Call.ID, RequestSeq: captured.Request.Seq}) {
						t.Fatalf("successful park lost recorded ref: %v", got.err)
					}
					if outer == "Exec" && got.inc.FenceToken != captured.Request.Fence {
						t.Fatal("returned incarnation lost invocation fence")
					}
				} else if errors.Is(got.err, api.ErrApprovalParked) {
					t.Fatalf("failed handoff reported park: %v", got.err)
				}
				if outcome == "backend Aborted" && (!errors.Is(got.err, cause) || status.Code(got.err) != codes.Aborted) {
					t.Fatalf("backend Aborted cause changed: %v", got.err)
				}
				if closes != 1 || b.stops != 0 || log.fences != 1 {
					t.Fatalf("close=%d stop=%d operation fences=%d", closes, b.stops, log.fences)
				}
				after, err := log.Store.Read(before + 1)
				if err != nil {
					t.Fatal(err)
				}
				suspends := 0
				for _, rec := range after {
					if rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle.Kind == api.LifecycleSuspend {
						suspends++
						if rec.Seq != captured.Head+1 || rec.Fence != captured.Request.Fence || rec.Event.Lifecycle.Snapshot.Local != "s" {
							t.Fatalf("SUSPEND lost captured cursor/fence/session: %+v", rec)
						}
					}
					if rec.Event.Kind == api.EventEnd || rec.Event.Kind == api.EventError || rec.Event.Kind == api.EventParked || rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle.Kind == api.LifecycleResume {
						t.Fatalf("false terminal/transport/resume record: %+v", rec)
					}
				}
				wantSuspends := 0
				if outcome == "success" {
					wantSuspends = 1
				}
				if suspends != wantSuspends {
					t.Fatalf("suspend records=%d, want %d", suspends, wantSuspends)
				}
				if _, err := other.Exec(ctx, log, "s", nil, mustApprovalHead(t, log)); !errors.Is(err, controller.ErrApprovalBlocked) {
					t.Fatalf("guard not released to cross-Placer preflight: %v", err)
				}
				if _, err := r.Approve(ctx, log, "s", api.ApprovalDecision{}); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
					t.Fatalf("guard not released to Registry.Approve: %v", err)
				}
				if err := other.Resume(ctx, log, "s"); !errors.Is(err, api.ErrApprovalParked) {
					t.Fatalf("guard not released to pending Resume: %v", err)
				}
				otherBackend.assertNoCompute(t)
				b.beforeSnapshot, log.beforeSuspend, log.failSuspend, ctl.snapshotErr = nil, nil, false, nil
				if _, err := r.Suspend(ctx, log, "s", "default"); err != nil {
					t.Fatalf("guard not released to recorded-route Suspend retry: %v", err)
				}
				if closes != 1 || b.stops != 0 {
					t.Fatal("public retry reclosed dial or destroyed actor")
				}
			})
		}
	}
}

func TestRegistryApprovalRepairObserver(t *testing.T) {
	log := newSuspendStore(t).Session("s")
	seedPlacementApproval(t, log, "recorded", api.EventToolCall)
	p, b := newApprovalNoIOPlacer(t)
	r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
	if err != nil {
		t.Fatal(err)
	}
	var records []eventlog.Record
	for range 2 {
		if err := r.Resume(t.Context(), log, "s", "recorded", placement.WithRegistryResumeObserver(controller.Observer{OnRecord: func(rec eventlog.Record) { records = append(records, rec) }})); !errors.Is(err, api.ErrApprovalParked) {
			t.Fatal(err)
		}
	}
	if len(records) != 1 || records[0].Event.Kind != api.EventApprovalRequest || records[0].Seq != 3 {
		t.Fatalf("repair observer=%v", records)
	}
	b.assertNoCompute(t)
}
