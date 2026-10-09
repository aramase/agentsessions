package controller_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/sqlitelog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func identityLog(t *testing.T, events ...api.Event) eventlog.Store {
	t.Helper()
	log := eventlog.AsStore(eventlog.New())
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events {
		if _, err := log.Append(int64(i), fence, event); err != nil {
			t.Fatal(err)
		}
	}
	return log
}

type identityHarness struct {
	desc     api.Descriptor
	describe func(context.Context) (api.Descriptor, error)
	run      func(context.Context, *api.Start, api.EventSink) error
}

func (h identityHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	if h.describe != nil {
		return h.describe(ctx)
	}
	return h.desc, nil
}

func (h identityHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.run != nil {
		return h.run(ctx, start, sink)
	}
	return sink.Output(ctx, "reply")
}

func TestResumeInvocationSelectsLastFirstSeenExecution(t *testing.T) {
	log := identityLog(t,
		api.Event{ExecutionID: "old", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0)}},
		api.Event{ExecutionID: "pending", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Config: []byte("opaque"), ResumeFromSeq: 37, InputCount: inputCount(1), Harness: "alias", HarnessVersion: "build-sha"}},
		api.Event{ExecutionID: "pending", Kind: api.EventInput, Message: api.TextMessage("user", "hello")},
		// A later record from an earlier execution must not redirect recovery.
		api.Event{ExecutionID: "old", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork}},
	)
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := controller.ResumeInvocation(log)
	if err != nil {
		t.Fatal(err)
	}
	if invocation == nil || !bytes.Equal(invocation.Config, []byte("opaque")) || invocation.ResumeFromSeq != 37 || invocation.InputCount == nil || *invocation.InputCount != 1 || invocation.Harness != "alias" || invocation.HarnessVersion != "build-sha" {
		t.Fatalf("selected invocation = %#v", invocation)
	}
	invocation.Config[0] = 'X'
	*invocation.InputCount = 99
	again, err := controller.ResumeInvocation(log)
	if err != nil || again == nil || !bytes.Equal(again.Config, []byte("opaque")) || again.InputCount == nil || *again.InputCount != 1 {
		t.Fatalf("caller mutation changed stored invocation: %#v, %v", again, err)
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("query changed journal: %v", err)
	}
}

func TestResumeInvocationNoMarkerOrNoPendingTurn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
	}{
		{"empty", nil},
		{"lifecycle only", []api.Event{{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend}}}},
		{"markerless", []api.Event{{ExecutionID: "legacy", Kind: api.EventInput, Message: api.TextMessage("user", "hello")}}},
		{"complete", []api.Event{
			{ExecutionID: "done", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0)}},
			{ExecutionID: "done", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		}},
		{"last first-seen complete", []api.Event{
			{ExecutionID: "older-pending", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0)}},
			{ExecutionID: "latest", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: inputCount(0)}},
			{ExecutionID: "latest", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invocation, err := controller.ResumeInvocation(identityLog(t, tc.events...))
			if err != nil || invocation != nil {
				t.Fatalf("ResumeInvocation = %#v, %v; want nil, nil", invocation, err)
			}
		})
	}
}

func TestExecRecordsAuthoritativeHarnessIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		desc     api.Descriptor
		route    string
		wantName string
	}{
		{"standalone", api.Descriptor{ID: "harness", Version: "build-sha"}, "", "harness"},
		{"registry alias", api.Descriptor{ID: "descriptor-id", Version: "build-sha"}, "resolved-alias", "resolved-alias"},
		{"unversioned", api.Descriptor{ID: "harness"}, "", "harness"},
		{"all defaults", api.Descriptor{}, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := identityLog(t)
			c, err := controller.New(log, nil, controller.WithHarness(tc.route))
			if err != nil {
				t.Fatal(err)
			}
			descriptions := 0
			har := identityHarness{describe: func(ctx context.Context) (api.Descriptor, error) {
				descriptions++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 10*time.Second {
					t.Error("Describe has no bounded deadline")
				}
				return tc.desc, nil
			}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				records, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				var marker *api.ExecutionStart
				for _, record := range records {
					if record.Event.Kind == api.EventExecutionStart {
						marker = record.Event.ExecutionStart
					}
				}
				if marker == nil || marker.Harness != tc.wantName || marker.HarnessVersion != tc.desc.Version {
					t.Fatalf("identity not durable before Run: %#v", marker)
				}
				return sink.Output(ctx, "reply")
			}}
			if err := c.Exec(t.Context(), har, nil, 0); err != nil {
				t.Fatal(err)
			}
			if descriptions != 1 {
				t.Fatalf("Describe calls = %d; want 1", descriptions)
			}
		})
	}
}

func TestRecoveryHarnessIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marker *api.ExecutionStart
		desc   api.Descriptor
		route  string
		want   error
	}{
		{"exact version", &api.ExecutionStart{Harness: "h", HarnessVersion: "v1"}, api.Descriptor{ID: "h", Version: "v1"}, "", nil},
		{"different version", &api.ExecutionStart{Harness: "h", HarnessVersion: "v1"}, api.Descriptor{ID: "h", Version: "v2"}, "", controller.ErrHarnessVersionMismatch},
		{"served empty version", &api.ExecutionStart{Harness: "h", HarnessVersion: "v1"}, api.Descriptor{ID: "h"}, "", controller.ErrHarnessVersionMismatch},
		{"opaque exact version", &api.ExecutionStart{Harness: "h", HarnessVersion: " v1 "}, api.Descriptor{ID: "h", Version: "v1"}, "", controller.ErrHarnessVersionMismatch},
		{"recorded empty version", &api.ExecutionStart{Harness: "h"}, api.Descriptor{ID: "h", Version: "v2"}, "", nil},
		{"both versions empty", &api.ExecutionStart{Harness: "h"}, api.Descriptor{ID: "h"}, "", nil},
		{"name mismatch unversioned", &api.ExecutionStart{Harness: "h"}, api.Descriptor{ID: "other"}, "", controller.ErrHarnessMismatch},
		{"name mismatch versioned", &api.ExecutionStart{Harness: "h", HarnessVersion: "v1"}, api.Descriptor{ID: "other", Version: "v1"}, "", controller.ErrHarnessMismatch},
		{"alias overrides descriptor", &api.ExecutionStart{Harness: "alias", HarnessVersion: "v1"}, api.Descriptor{ID: "different-id", Version: "v1"}, "alias", nil},
		{"alias does not override version", &api.ExecutionStart{Harness: "alias", HarnessVersion: "v1"}, api.Descriptor{ID: "different-id", Version: "v2"}, "alias", controller.ErrHarnessVersionMismatch},
		{"wrong route overrides matching ID", &api.ExecutionStart{Harness: "h"}, api.Descriptor{ID: "h"}, "other-route", controller.ErrHarnessMismatch},
		{"missing recorded name still checks version", &api.ExecutionStart{HarnessVersion: "v1"}, api.Descriptor{ID: "anything", Version: "v2"}, "", controller.ErrHarnessVersionMismatch},
		{"missing name matching version", &api.ExecutionStart{HarnessVersion: "v1"}, api.Descriptor{ID: "anything", Version: "v1"}, "", nil},
		{"empty marker identity", &api.ExecutionStart{}, api.Descriptor{ID: "anything", Version: "v2"}, "", nil},
		{"markerless", nil, api.Descriptor{ID: "anything", Version: "v2"}, "", nil},
	} {
		for _, completed := range []bool{false, true} {
			op := "resume"
			if completed {
				op = "replay"
			}
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				events := []api.Event{}
				if tc.marker != nil {
					marker := *tc.marker
					marker.InputCount = inputCount(0)
					events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventExecutionStart, ExecutionStart: &marker})
				} else {
					events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventInput, Message: api.TextMessage("user", "legacy")})
				}
				if completed {
					events = append(events,
						api.Event{ExecutionID: "turn", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
						api.Event{ExecutionID: "turn", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
					)
				}
				log := identityLog(t, events...)
				models, tools, runs := 0, 0, 0
				c, err := controller.New(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
					models++
					return api.ModelResponse{}, nil
				}, controller.WithHarness(tc.route), controller.WithSessionUID("session"), controller.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					tools++
					return api.ToolResult{ID: "t1"}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				before, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				har := identityHarness{describe: func(context.Context) (api.Descriptor, error) {
					if tc.marker == nil || tc.marker.Harness == "" && tc.marker.HarnessVersion == "" {
						t.Fatal("legacy identity introduced a Describe dependency")
					}
					return tc.desc, nil
				}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
					runs++
					if tc.want != nil {
						_, _ = sink.Model(ctx, api.ModelRequest{})
						_, _ = sink.ToolCall(ctx, api.ToolCall{ID: "t1", Tool: "effect", IdempotencyKey: "once", Mediation: api.MediationControllerMediated})
					}
					return sink.Output(ctx, "reply")
				}}
				if completed {
					_, err = c.Replay(t.Context(), har)
				} else {
					var resumed bool
					resumed, err = c.Resume(t.Context(), har)
					if resumed != (tc.want == nil) {
						t.Errorf("resumed = %v; want %v", resumed, tc.want == nil)
					}
				}
				if !errors.Is(err, tc.want) {
					t.Fatalf("%s error = %v; want %v", op, err, tc.want)
				}
				if tc.want != nil {
					after, readErr := log.Read(1)
					if readErr != nil || !reflect.DeepEqual(before, after) || runs != 0 || models != 0 || tools != 0 {
						t.Fatalf("mismatch had effects: runs/models/tools=%d/%d/%d, read=%v", runs, models, tools, readErr)
					}
				} else if runs != 1 {
					t.Fatalf("matching harness runs = %d; want 1", runs)
				}
			})
		}
	}
}

func TestReplayPreflightsAllCompletedHarnessIdentities(t *testing.T) {
	for _, want := range []error{controller.ErrHarnessMismatch, controller.ErrHarnessVersionMismatch} {
		t.Run(want.Error(), func(t *testing.T) {
			second := &api.ExecutionStart{Harness: "h", HarnessVersion: "v2", InputCount: inputCount(0)}
			if want == controller.ErrHarnessMismatch {
				second.Harness, second.HarnessVersion = "different", "v1"
			}
			log := identityLog(t,
				api.Event{ExecutionID: "first", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "h", HarnessVersion: "v1", InputCount: inputCount(0)}},
				api.Event{ExecutionID: "first", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
				api.Event{ExecutionID: "first", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
				api.Event{ExecutionID: "second", Kind: api.EventExecutionStart, ExecutionStart: second},
				api.Event{ExecutionID: "second", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
				api.Event{ExecutionID: "second", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
			)
			c, err := controller.New(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			runs := 0
			har := identityHarness{desc: api.Descriptor{ID: "h", Version: "v1"}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
				runs++
				return sink.Output(ctx, "reply")
			}}
			if _, err := c.Replay(t.Context(), har); !errors.Is(err, want) {
				t.Fatalf("Replay error = %v; want %v", err, want)
			}
			if runs != 0 {
				t.Fatalf("earlier execution ran before later mismatch: %d runs", runs)
			}
		})
	}
}

func TestResumeCompleteDoesNotCheckNewHarnessIdentity(t *testing.T) {
	log := identityLog(t,
		api.Event{ExecutionID: "done", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "old", HarnessVersion: "v1", InputCount: inputCount(0)}},
		api.Event{ExecutionID: "done", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	)
	c, err := controller.New(log, nil, controller.WithHarness("new"))
	if err != nil {
		t.Fatal(err)
	}
	har := identityHarness{describe: func(context.Context) (api.Descriptor, error) {
		t.Fatal("completed Resume called Describe")
		return api.Descriptor{}, nil
	}, run: func(context.Context, *api.Start, api.EventSink) error { t.Fatal("completed Resume ran"); return nil }}
	if resumed, err := c.Resume(t.Context(), har); resumed || err != nil {
		t.Fatalf("completed Resume = %v, %v", resumed, err)
	}
}

func TestHarnessDescribeFailureHasNoExecutionEffects(t *testing.T) {
	for _, op := range []string{"exec", "resume", "replay"} {
		for _, canceled := range []bool{false, true} {
			t.Run(op+"/canceled-"+fmt.Sprint(canceled), func(t *testing.T) {
				events := []api.Event{}
				if op != "exec" {
					events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "h", HarnessVersion: "v1", InputCount: inputCount(0)}})
					if op == "replay" {
						events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
					}
				}
				log := identityLog(t, events...)
				c, err := controller.New(log, nil)
				if err != nil {
					t.Fatal(err)
				}
				before, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				want := errors.New("Describe unavailable")
				ctx := t.Context()
				if canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					want = context.Canceled
				}
				har := identityHarness{describe: func(ctx context.Context) (api.Descriptor, error) {
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > 10*time.Second {
						t.Error("Describe not bounded")
					}
					if canceled {
						<-ctx.Done()
						return api.Descriptor{}, ctx.Err()
					}
					return api.Descriptor{}, want
				}, run: func(context.Context, *api.Start, api.EventSink) error {
					t.Fatal("Run after failed Describe")
					return nil
				}}
				switch op {
				case "exec":
					err = c.Exec(ctx, har, nil, 0)
				case "resume":
					var resumed bool
					resumed, err = c.Resume(ctx, har)
					if resumed {
						t.Error("failed Describe reported resumed")
					}
				case "replay":
					_, err = c.Replay(ctx, har)
				}
				if !errors.Is(err, controller.ErrHarnessDescribeFailed) || !errors.Is(err, want) {
					t.Fatalf("%s error = %v; want ErrHarnessDescribeFailed and original cause %v", op, err, want)
				}
				after, readErr := log.Read(1)
				if readErr != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("failed Describe changed journal: %v", readErr)
				}
			})
		}
	}
}

func TestHarnessDescribeFailurePreservesGRPCStatus(t *testing.T) {
	log := identityLog(t)
	c, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	cause := status.Error(codes.Unavailable, "endpoint unavailable")
	har := identityHarness{describe: func(context.Context) (api.Descriptor, error) { return api.Descriptor{}, cause }}
	err = c.Exec(t.Context(), har, nil, 0)
	if !errors.Is(err, controller.ErrHarnessDescribeFailed) || !errors.Is(err, cause) || status.Code(err) != codes.Unavailable {
		t.Fatalf("Describe error lost boundary or original gRPC cause: %v", err)
	}
}

func TestHarnessRunFailureIsNotDescribeFailure(t *testing.T) {
	for _, op := range []string{"exec", "resume", "replay"} {
		t.Run(op, func(t *testing.T) {
			events := []api.Event{}
			if op != "exec" {
				events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "h", HarnessVersion: "v1", InputCount: inputCount(0)}})
				if op == "replay" {
					events = append(events, api.Event{ExecutionID: "turn", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
				}
			}
			log := identityLog(t, events...)
			c, err := controller.New(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := errors.New("Run unavailable")
			har := identityHarness{desc: api.Descriptor{ID: "h", Version: "v1"}, run: func(context.Context, *api.Start, api.EventSink) error { return want }}
			switch op {
			case "exec":
				err = c.Exec(t.Context(), har, nil, 0)
			case "resume":
				var resumed bool
				resumed, err = c.Resume(t.Context(), har)
				if !resumed {
					t.Error("Run failure did not report attempted recovery")
				}
			case "replay":
				_, err = c.Replay(t.Context(), har)
			}
			if !errors.Is(err, want) || errors.Is(err, controller.ErrHarnessDescribeFailed) {
				t.Fatalf("%s Run error = %v; want original Run cause without Describe classification", op, err)
			}
		})
	}
}

func TestHarnessPreflightOnlyChecksSelectedExecutions(t *testing.T) {
	log := identityLog(t,
		api.Event{ExecutionID: "done", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "old-alias", HarnessVersion: "v1", InputCount: inputCount(0)}},
		api.Event{ExecutionID: "done", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
		api.Event{ExecutionID: "done", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		api.Event{ExecutionID: "pending", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "new-alias", HarnessVersion: "v2", InputCount: inputCount(0)}},
	)
	replay, err := controller.New(log, nil, controller.WithHarness("old-alias"))
	if err != nil {
		t.Fatal(err)
	}
	har := identityHarness{desc: api.Descriptor{ID: "descriptor-id", Version: "v1"}}
	if outputs, err := replay.Replay(t.Context(), har); err != nil || !reflect.DeepEqual(outputs, []string{"reply"}) {
		t.Fatalf("Replay checked unfinished turn: %v, %v", outputs, err)
	}
	resume, err := controller.New(log, nil, controller.WithHarness("new-alias"))
	if err != nil {
		t.Fatal(err)
	}
	har.desc.Version = "v2"
	if resumed, err := resume.Resume(t.Context(), har); err != nil || !resumed {
		t.Fatalf("Resume checked completed turn: %v, %v", resumed, err)
	}
}

func TestReplayDescribesHarnessOnceBeforeAllRuns(t *testing.T) {
	log := identityLog(t,
		api.Event{ExecutionID: "first", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "h", HarnessVersion: "v1", InputCount: inputCount(0)}},
		api.Event{ExecutionID: "first", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
		api.Event{ExecutionID: "first", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		api.Event{ExecutionID: "second", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "h", HarnessVersion: "v1", InputCount: inputCount(0)}},
		api.Event{ExecutionID: "second", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
		api.Event{ExecutionID: "second", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	)
	c, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	descriptions, runs := 0, 0
	har := identityHarness{describe: func(context.Context) (api.Descriptor, error) {
		descriptions++
		if runs != 0 {
			t.Error("Describe ran after a Run")
		}
		return api.Descriptor{ID: "h", Version: "v1"}, nil
	}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		runs++
		return sink.Output(ctx, "reply")
	}}
	if outputs, err := c.Replay(t.Context(), har); err != nil || !reflect.DeepEqual(outputs, []string{"reply", "reply"}) || runs != 2 || descriptions != 1 {
		t.Fatalf("Replay = %v, %v; runs=%d descriptions=%d", outputs, err, runs, descriptions)
	}
}

func TestHarnessIdentityResumeAfterSQLiteReopenAndFork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	log := store.Session("parent")
	c, err := controller.New(log, nil, controller.WithHarness("registry-alias"), controller.WithStart([]byte("opaque"), 17))
	if err != nil {
		t.Fatal(err)
	}
	interruption := errors.New("interrupted")
	har := identityHarness{desc: api.Descriptor{ID: "descriptor-id", Version: "v1"}, run: func(ctx context.Context, _ *api.Start, sink api.EventSink) error {
		if err := sink.Output(ctx, "reply"); err != nil {
			return err
		}
		return interruption
	}}
	if err := c.Exec(t.Context(), har, nil, 0); !errors.Is(err, interruption) {
		t.Fatalf("Exec = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log = store.Session("parent")
	invocation, err := controller.ResumeInvocation(log)
	if err != nil || invocation == nil || invocation.Harness != "registry-alias" || invocation.HarnessVersion != "v1" || !bytes.Equal(invocation.Config, []byte("opaque")) || invocation.ResumeFromSeq != 17 {
		t.Fatalf("reopened invocation = %#v, %v", invocation, err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	child := store.Session("child")
	if err := controller.Fork(log, child, head); err != nil {
		t.Fatal(err)
	}
	for _, target := range []eventlog.Store{log, child} {
		resume, err := controller.New(target, nil, controller.WithHarness("registry-alias"))
		if err != nil {
			t.Fatal(err)
		}
		before, err := target.Read(1)
		if err != nil {
			t.Fatal(err)
		}
		har.desc.Version = "v2"
		if resumed, err := resume.Resume(t.Context(), har); resumed || !errors.Is(err, controller.ErrHarnessVersionMismatch) {
			t.Fatalf("wrong-version Resume = %v, %v", resumed, err)
		}
		after, err := target.Read(1)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("version mismatch changed SQLite journal: %v", err)
		}
		har.desc.Version = "v1"
		har.run = func(ctx context.Context, start *api.Start, sink api.EventSink) error {
			if !bytes.Equal(start.Config, []byte("opaque")) || start.ResumeFromSeq != 17 {
				t.Fatal("changed reconstructed start")
			}
			if err := sink.Output(ctx, "reply"); err != nil {
				return err
			}
			return sink.Output(ctx, "continued")
		}
		if resumed, err := resume.Resume(t.Context(), har); err != nil || !resumed {
			t.Fatalf("matching Resume = %v, %v", resumed, err)
		}
		if outputs, err := resume.Outputs(); err != nil || !reflect.DeepEqual(outputs, []string{"reply", "continued"}) {
			t.Fatalf("outputs = %v, %v", outputs, err)
		}
		if err := target.Verify(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHarnessDescribePreservesEarlierDeadline(t *testing.T) {
	log := identityLog(t)
	c, err := controller.New(log, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	har := identityHarness{describe: func(ctx context.Context) (api.Descriptor, error) {
		if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
			t.Errorf("Describe extended caller deadline: %v", got)
		}
		<-ctx.Done()
		return api.Descriptor{}, ctx.Err()
	}, run: func(context.Context, *api.Start, api.EventSink) error {
		t.Fatal("timed out Describe reached Run")
		return nil
	}}
	if err := c.Exec(ctx, har, nil, 0); !errors.Is(err, controller.ErrHarnessDescribeFailed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec = %v; want Describe failure retaining deadline cause", err)
	}
	if head, err := log.Head(); err != nil || head != 0 {
		t.Fatalf("timed out Describe changed journal head = %d, %v", head, err)
	}
}

func TestResumeInvocationRejectsInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  *int64
		inputs []*api.Message
		want   error
	}{
		{"missing count", nil, nil, controller.ErrInvalidExecutionLog},
		{"negative", inputCount(-1), nil, controller.ErrInvalidExecutionLog},
		{"missing input", inputCount(1), nil, controller.ErrIncompleteInvocation},
		{"excess input", inputCount(0), []*api.Message{api.TextMessage("user", "hello")}, controller.ErrInvalidExecutionLog},
		{"missing payload", inputCount(1), []*api.Message{nil}, controller.ErrInvalidExecutionLog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := []api.Event{{ExecutionID: "pending", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: tc.count}}}
			for _, message := range tc.inputs {
				events = append(events, api.Event{ExecutionID: "pending", Kind: api.EventInput, Message: message})
			}
			log := identityLog(t, events...)
			invocation, err := controller.ResumeInvocation(log)
			if invocation != nil || !errors.Is(err, tc.want) {
				t.Fatalf("ResumeInvocation = %#v, %v; want nil, %v", invocation, err, tc.want)
			}
			c, err := controller.New(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			har := checkedStartHarness{executionConfigHarness{}, func(*api.Start) { t.Fatal("invalid invocation ran") }}
			if resumed, err := c.Resume(context.Background(), har); resumed || !errors.Is(err, tc.want) {
				t.Fatalf("Resume disagrees with query = %v, %v", resumed, err)
			}
		})
	}
}
