package placement_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
)

type approvalRepairFenceLog struct {
	eventlog.Store
	beforeFence, afterFence func()
	fences                  int
}

func (l *approvalRepairFenceLog) NewFence() (int64, error) {
	l.fences++
	if hook := l.beforeFence; hook != nil {
		l.beforeFence = nil
		hook()
	}
	fence, err := l.Store.NewFence()
	if err == nil {
		if hook := l.afterFence; hook != nil {
			l.afterFence = nil
			hook()
		}
	}
	return fence, err
}

func TestPlacementApprovalRepairCapturedCASAcrossRegistries(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, operation := range []string{"Placer", "Registry"} {
			for _, advancement := range []string{"request", "decision", "receipt", "completed", "new execution", "fence superseded"} {
				t.Run(backend+"/"+operation+"/"+advancement, func(t *testing.T) {
					var log eventlog.Store = eventlog.AsStore(eventlog.New())
					if backend == "sqlite" {
						log = newSuspendStore(t).Session("s")
					}
					seedPlacementApproval(t, log, "recorded", api.EventToolCall)
					pa, ba := newApprovalNoIOPlacer(t)
					pb, bb := newApprovalNoIOPlacer(t)
					ra, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": pa})
					if err != nil {
						t.Fatal(err)
					}
					rb, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": pb})
					if err != nil {
						t.Fatal(err)
					}
					var afterB []eventlog.Record
					advance := func() {
						if advancement == "fence superseded" {
							if _, err := log.NewFence(); err != nil {
								t.Fatal(err)
							}
						} else {
							err := rb.Resume(t.Context(), log, "s", "recorded")
							var park *api.ApprovalParkedError
							if !errors.As(err, &park) || park.Ref != (api.ApprovalRef{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 3}) {
								t.Fatalf("Registry B repair=%v", err)
							}
							if advancement != "request" {
								decision, err := rb.Approve(t.Context(), log, "s", api.ApprovalDecision{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 3, Approved: false})
								if err != nil {
									t.Fatal(err)
								}
								if advancement != "decision" {
									receipt, err := log.Append(decision.Seq, decision.Fence, api.Event{ExecutionID: "e1", Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1", IsError: true, Code: api.ToolResultCodeApprovalDenied, ApprovalRequestSeq: 3, ApprovalDecisionSeq: decision.Seq}})
									if err != nil {
										t.Fatal(err)
									}
									if advancement != "receipt" {
										end, err := log.Append(receipt.Seq, receipt.Fence, api.Event{ExecutionID: "e1", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
										if err != nil {
											t.Fatal(err)
										}
										if advancement == "new execution" {
											fence, err := log.NewFence()
											if err != nil {
												t.Fatal(err)
											}
											zero := int64(0)
											if _, err := log.Append(end.Seq, fence, api.Event{ExecutionID: "e2", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "recorded", HarnessVersion: "v1", InputCount: &zero}}); err != nil {
												t.Fatal(err)
											}
										}
									}
								}
							}
						}
						afterB = suspendRecords(t, log)
					}
					query := &approvalRepairFenceLog{Store: log}
					want := eventlog.ErrConflict
					if advancement == "fence superseded" {
						query.afterFence, want = advance, eventlog.ErrFenced
					} else {
						query.beforeFence = advance
					}
					var observed []eventlog.Record
					observer := controller.Observer{OnRecord: func(rec eventlog.Record) { observed = append(observed, rec) }}
					var panicValue any
					func() {
						defer func() { panicValue = recover() }()
						if operation == "Placer" {
							err = pa.Resume(t.Context(), query, "s", placement.WithResumeObserver(observer))
						} else {
							err = ra.Resume(t.Context(), query, "s", "recorded", placement.WithRegistryResumeObserver(observer))
						}
					}()
					if panicValue != nil {
						t.Errorf("Registry A repair panicked after Registry B %s: %v", advancement, panicValue)
					}
					if err != want || errors.Is(err, api.ErrApprovalParked) {
						t.Errorf("Registry A repair=%v, want exact %v", err, want)
					}
					if afterB == nil || !reflect.DeepEqual(afterB, suspendRecords(t, log)) || len(observed) != 0 {
						t.Error("stale repair appended/observed beyond Registry B's journal authority")
					}
					if query.fences != 1 {
						t.Fatalf("Registry A minted %d fences, want one", query.fences)
					}
					ba.assertNoCompute(t)
					bb.assertNoCompute(t)
					if _, err := ra.Approve(t.Context(), log, "s", api.ApprovalDecision{}); !errors.Is(err, controller.ErrInvalidApprovalDecision) {
						t.Fatalf("failed repair retained Registry A guard: %v", err)
					}
				})
			}
		}
	}
}

type approvalFaultLog struct {
	eventlog.Store
	failKind      api.EventKind
	failSuspend   bool
	err           error
	fences        int
	beforeSuspend func()
}

func (l *approvalFaultLog) NewFence() (int64, error) { l.fences++; return l.Store.NewFence() }
func (l *approvalFaultLog) Append(head, fence int64, ev api.Event) (eventlog.Record, error) {
	if l.beforeSuspend != nil && ev.Kind == api.EventLifecycle && ev.Lifecycle.Kind == api.LifecycleSuspend {
		l.beforeSuspend()
	}
	if ev.Kind == l.failKind || l.failSuspend && ev.Kind == api.EventLifecycle && ev.Lifecycle.Kind == api.LifecycleSuspend {
		return eventlog.Record{}, l.err
	}
	return l.Store.Append(head, fence, ev)
}
func newApprovalNoIOPlacer(t *testing.T) (*placement.Placer, *approvalBackend) {
	t.Helper()
	runtime := local.New(placementGateHarness{})
	t.Cleanup(func() { _ = runtime.Close() })
	b := &approvalBackend{Backend: runtime}
	return placement.New(b, nil, placement.WithDialer(func(string) (api.Harness, func() error, error) {
		t.Fatal("pending approval dialed harness")
		return nil, nil, nil
	})), b
}

// Pending approval must short-circuit before p.resume's admission/Restore/dial/Run path.
func TestPlacementApprovalPendingResumeNoCompute(t *testing.T) {
	for _, operation := range []string{"Placer", "Registry"} {
		for _, cut := range []api.EventKind{api.EventToolCall, api.EventApprovalRequest} {
			t.Run(operation+"/"+string(cut), func(t *testing.T) {
				log := &approvalFaultLog{Store: newSuspendStore(t).Session("s")}
				seedPlacementApproval(t, log.Store, "recorded", cut)
				p, b := newApprovalNoIOPlacer(t)
				r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
				if err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if operation == "Placer" {
						err = p.Resume(t.Context(), log, "s")
					} else {
						err = r.Resume(t.Context(), log, "s", "recorded")
					}
					var park *api.ApprovalParkedError
					if !errors.As(err, &park) || !errors.Is(err, api.ErrApprovalParked) || park.Ref != (api.ApprovalRef{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 3}) {
						t.Fatalf("Resume=%v park=%+v", err, park)
					}
					b.assertNoCompute(t)
				}
				records := suspendRecords(t, log)
				if len(records) != 3 {
					t.Fatalf("duplicate repair or lifecycle writes: %v", records)
				}
				wantFences := 0
				if cut == api.EventToolCall {
					wantFences = 1
				}
				if log.fences != wantFences {
					t.Fatalf("pending query minted %d fences, want %d", log.fences, wantFences)
				}
				assertNoApprovalLifecycle(t, log)
			})
		}
	}
}

func TestPlacementApprovalInheritedResumeBeforeProvision(t *testing.T) {
	store := newSuspendStore(t)
	parent, child := store.Session("parent"), store.Session("child")
	seedPlacementApproval(t, parent, "recorded", api.EventApprovalRequest)
	if err := controller.Fork(parent, child, 3); err != nil {
		t.Fatal(err)
	}
	log := &approvalFaultLog{Store: child}
	p, b := newApprovalNoIOPlacer(t)
	r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
	if err != nil {
		t.Fatal(err)
	}
	before := suspendRecords(t, log)
	for range 2 {
		if err := r.Resume(t.Context(), log, "child", "recorded"); !errors.Is(err, controller.ErrInheritedToolIntent) {
			t.Fatalf("Resume=%v", err)
		}
		b.assertNoCompute(t)
		if log.fences != 0 || !reflect.DeepEqual(before, suspendRecords(t, log)) {
			t.Fatal("inherited refusal fenced or wrote")
		}
	}
}

func TestPlacementApprovalRepairFailureNoCompute(t *testing.T) {
	for _, cause := range []error{eventlog.ErrConflict, eventlog.ErrFenced, errors.New("request store unavailable")} {
		t.Run(cause.Error(), func(t *testing.T) {
			log := &approvalFaultLog{Store: newSuspendStore(t).Session("s"), failKind: api.EventApprovalRequest, err: cause}
			seedPlacementApproval(t, log.Store, "recorded", api.EventToolCall)
			p, b := newApprovalNoIOPlacer(t)
			r, err := placement.NewRegistry("recorded", map[string]*placement.Placer{"recorded": p})
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := r.Resume(t.Context(), log, "s", "recorded"); !errors.Is(err, cause) || errors.Is(err, api.ErrApprovalParked) {
					t.Fatalf("repair failure=%v", err)
				}
				b.assertNoCompute(t)
				if head, _ := log.Head(); head != 2 {
					t.Fatalf("failed repair head=%d", head)
				}
			}
			log.failKind = ""
			if err := r.Resume(t.Context(), log, "s", "recorded"); !errors.Is(err, api.ErrApprovalParked) {
				t.Fatalf("repair retry=%v", err)
			}
		})
	}
}

func TestPlacementApprovalPendingResolvedCheckWithoutDescribe(t *testing.T) {
	for _, checkCause := range []error{nil, errors.New("registered descriptor pin mismatch"), errors.New("registered name collides with static harness")} {
		log := newSuspendStore(t).Session("s")
		seedPlacementApproval(t, log, "registered", api.EventApprovalRequest)
		p, b := newApprovalNoIOPlacer(t)
		r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": p})
		if err != nil {
			t.Fatal(err)
		}
		registered, registeredBackend := newApprovalNoIOPlacer(t)
		if err := r.Add("registered", registered); err != nil {
			t.Fatal(err)
		}
		checked := 0
		err = r.Resume(t.Context(), log, "s", "default", placement.WithResolvedHarnessCheck(func(name string) error {
			checked++
			if name != "registered" {
				t.Fatalf("check=%s", name)
			}
			return checkCause // nil models the host's existing-session exception after retirement.
		}))
		if checkCause != nil && err != checkCause || checkCause == nil && !errors.Is(err, api.ErrApprovalParked) {
			t.Fatalf("check cause=%v, Resume=%v", checkCause, err)
		}
		if checked != 1 {
			t.Fatalf("check calls=%d", checked)
		}
		b.assertNoCompute(t)
		registeredBackend.assertNoCompute(t)
	}
}

// A served default must not conceal an unserved recorded route, even for a no-compute park query.
func TestPlacementApprovalPendingUnservedNoFallback(t *testing.T) {
	log := newSuspendStore(t).Session("s")
	seedPlacementApproval(t, log, "unserved", api.EventApprovalRequest)
	p, b := newApprovalNoIOPlacer(t)
	r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": p})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Resume(t.Context(), log, "s", "default"); !errors.Is(err, placement.ErrRecordedHarnessNotServed) {
		t.Fatalf("Resume=%v", err)
	}
	b.assertNoCompute(t)
}
