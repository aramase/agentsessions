package controller_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
)

// A legacy replay projection is not the pending recovery invocation: recovery uses the last
// INPUT and treats END or ERROR as finished. The routing query must select that same invocation.
func TestResumeInvocationV012SelectionMatchesRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resumed     bool
		modern      bool
		input       string
		history     int
		errorDetail string
	}{
		{name: "completed"},
		{name: "model-error"},
		{name: "upgraded-resume"},
		{name: "crashed-input", resumed: true, input: "hello"},
		{name: "crashed-output", resumed: true, input: "hello"},
		{name: "crashed-model", resumed: true, input: "hello", errorDetail: "recorded completion missing"},
		{name: "multi-turn-crashed-output", resumed: true, input: "second", history: 4},
		{name: "mixed-interrupted", resumed: true, modern: true, input: "new", history: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, original := v012Log(t, tc.name)
			invocation, err := controller.ResumeInvocation(log)
			if err != nil {
				t.Fatal(err)
			}
			if tc.modern {
				if invocation == nil || invocation.Config != nil || invocation.ResumeFromSeq != 0 || invocation.InputCount == nil || *invocation.InputCount != 1 || invocation.Harness != "" || invocation.HarnessVersion != "" {
					t.Fatalf("old modern marker did not retain default identity: %#v", invocation)
				}
			} else if invocation != nil {
				t.Fatalf("markerless legacy query = %#v; want session-default routing", invocation)
			}
			if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, original) {
				t.Fatalf("routing query changed old records: %v", err)
			}

			modelCalls, runs := 0, 0
			c, err := v012Controller(log, func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
				modelCalls++
				return echoagent.Model(ctx, req)
			}, controller.WithHarness("session-default"), controller.WithStart([]byte("replacement"), 99))
			if err != nil {
				t.Fatal(err)
			}
			har := identityHarness{
				describe: func(context.Context) (api.Descriptor, error) {
					t.Fatal("legacy identity introduced Describe/name/version checks")
					return api.Descriptor{}, nil
				},
				run: func(ctx context.Context, start *api.Start, sink api.EventSink) error {
					runs++
					if !tc.resumed {
						t.Fatal("completed legacy turn re-entered harness")
					}
					if !reflect.DeepEqual(messageTexts(start.Inputs), []string{tc.input}) || len(start.History) != tc.history || start.Config != nil || start.ResumeFromSeq != 0 {
						t.Fatalf("wrong recovery invocation: %#v", start)
					}
					return (echoagent.Harness{}).Run(ctx, start, sink)
				},
			}
			resumed, err := c.Resume(t.Context(), har)
			if resumed != tc.resumed {
				t.Fatalf("Resume = %v, %v; want resumed=%v", resumed, err, tc.resumed)
			}
			if tc.errorDetail == "" {
				if err != nil {
					t.Fatalf("Resume = %v; want no error", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.errorDetail) {
				t.Fatalf("Resume = %v; want detail %q", err, tc.errorDetail)
			}
			wantRuns, wantModels := 0, 0
			if tc.resumed {
				wantRuns = 1
			}
			if tc.name == "crashed-input" {
				wantModels = 1
			}
			if runs != wantRuns || modelCalls != wantModels {
				t.Fatalf("recovery runs/models = %d/%d; want %d/%d", runs, modelCalls, wantRuns, wantModels)
			}
			if !tc.resumed {
				if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, original) {
					t.Fatalf("completed recovery changed old records: %v", err)
				}
			}
		})
	}
}

func TestResumeInvocationInputlessLegacyIsNoPendingTurn(t *testing.T) {
	for _, event := range []api.Event{
		{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		{Kind: api.EventError, Err: &api.Error{Description: "finished without input"}},
	} {
		t.Run(string(event.Kind), func(t *testing.T) {
			log := identityLog(t, event)
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if invocation, err := controller.ResumeInvocation(log); invocation != nil || err != nil {
				t.Fatalf("inputless legacy query = %#v, %v; want nil, nil", invocation, err)
			}
			// A no-op must not even require a session UID to manufacture legacy identity.
			c, err := controller.New(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			har := identityHarness{describe: func(context.Context) (api.Descriptor, error) {
				t.Fatal("no pending legacy turn called Describe")
				return api.Descriptor{}, nil
			}, run: func(context.Context, *api.Start, api.EventSink) error {
				t.Fatal("no pending legacy turn ran harness")
				return nil
			}}
			if resumed, err := c.Resume(t.Context(), har); resumed || err != nil {
				t.Fatalf("inputless legacy Resume = %v, %v; want no-op", resumed, err)
			}
			if after, err := log.Read(1); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("inputless legacy no-op changed records: %v", err)
			}
		})
	}
}

func TestReplayPreflightsModernVersionBeforeLegacyRun(t *testing.T) {
	log, original := v012Log(t, "completed")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range []api.Event{
		{ExecutionID: "modern", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "alias", HarnessVersion: "v2", InputCount: inputCount(0)}},
		{ExecutionID: "modern", Kind: api.EventOutput, Message: api.TextMessage("assistant", "reply")},
		{ExecutionID: "modern", Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
	} {
		if _, err := log.Append(int64(len(original)+i), fence, event); err != nil {
			t.Fatal(err)
		}
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	c, err := v012Controller(log, nil, controller.WithHarness("alias"))
	if err != nil {
		t.Fatal(err)
	}
	har := identityHarness{desc: api.Descriptor{ID: "descriptor-id", Version: "v1"}, run: func(context.Context, *api.Start, api.EventSink) error {
		t.Fatal("legacy prefix ran before incompatible modern version was preflighted")
		return nil
	}}
	if _, err := c.Replay(t.Context(), har); !errors.Is(err, controller.ErrHarnessVersionMismatch) {
		t.Fatalf("Replay = %v; want version mismatch", err)
	}
	if after, err := log.Read(1); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed preflight changed mixed journal: %v", err)
	}
}

func TestExecRejectsInvalidLegacyPrefixBeforeDescribe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
	}{
		{"unsupported legacy kind", []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "old")}, {Kind: api.EventApprovalRequest}}},
		{"ambiguous upgrade", []api.Event{{Kind: api.EventInput, Message: api.TextMessage("user", "old")}, {ExecutionID: "modern", Kind: api.EventInput, Message: api.TextMessage("user", "new")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := identityLog(t, tc.events...)
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			c, err := v012Controller(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			har := identityHarness{describe: func(context.Context) (api.Descriptor, error) {
				t.Fatal("invalid legacy prefix reached Describe")
				return api.Descriptor{}, nil
			}, run: func(context.Context, *api.Start, api.EventSink) error {
				t.Fatal("invalid legacy prefix ran harness")
				return nil
			}}
			if err := c.Exec(t.Context(), har, nil, int64(len(tc.events))); !errors.Is(err, controller.ErrInvalidExecutionLog) {
				t.Fatalf("Exec = %v; want invalid log", err)
			}
			if after, err := log.Read(1); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected Exec changed journal: %v", err)
			}
		})
	}
}
