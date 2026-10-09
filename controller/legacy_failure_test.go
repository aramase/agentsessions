package controller_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
)

type failingLegacyHarness struct {
	echoagent.Harness
	failure error
}

func (h failingLegacyHarness) Run(context.Context, *api.Start, api.EventSink) error {
	return h.failure
}

type masksLegacyToolFailureHarness struct {
	echoagent.Harness
	usageErr error
	failure  error
}

func (h *masksLegacyToolFailureHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, _ = sink.ToolCall(ctx, recordedToolCall())
	h.usageErr = sink.Usage(ctx, api.Usage{})
	return h.failure
}

func TestLegacyToolEvidenceFailurePrecedesUsageAndHarnessError(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "Replay", true: "Resume"}[resume], func(t *testing.T) {
			log, _ := v012Log(t, "crashed-output")
			c, err := v012Controller(log, echoagent.Model)
			if err != nil {
				t.Fatal(err)
			}
			har := &masksLegacyToolFailureHarness{failure: errors.New("masking harness error")}
			if resume {
				_, err = c.Resume(t.Context(), har)
			} else {
				_, err = c.Replay(t.Context(), har)
			}
			if !errors.Is(err, controller.ErrReplayDiverged) || har.usageErr != err {
				t.Fatalf("fatal evidence was masked by legacy accounting/harness error: run=%v usage=%v", err, har.usageErr)
			}
		})
	}
}

func TestLegacyResumeFailurePersistsIDlessErrorAndOriginalCause(t *testing.T) {
	injected := errors.New("original harness failure")
	for _, path := range []string{"harness error", "unconsumed effects", "fatal tool evidence"} {
		t.Run(path, func(t *testing.T) {
			log, original := v012Log(t, "crashed-output")
			var har api.Harness = failingLegacyHarness{failure: injected}
			want := injected
			switch path {
			case "unconsumed effects":
				har = &callHarness{}
				want = controller.ErrReplayDiverged
			case "fatal tool evidence":
				har = handledToolErrorHarness{call: recordedToolCall()}
				want = controller.ErrReplayDiverged
			}
			var observed []eventlog.Record
			c, err := v012Controller(log, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
				t.Fatal("failed recovery invoked model")
				return api.ModelResponse{}, nil
			}, controller.WithObserver(controller.Observer{OnRecord: func(record eventlog.Record) {
				observed = append(observed, record)
			}}))
			if err != nil {
				t.Fatal(err)
			}
			resumed, runErr := c.Resume(t.Context(), har)
			if !resumed || !errors.Is(runErr, want) {
				t.Fatalf("Resume = %v, %v; want original cause %v", resumed, runErr, want)
			}
			after, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(original)+1 || !reflect.DeepEqual(after[:len(original)], original) {
				t.Fatalf("failure did not durably append one ERROR: %#v", after)
			}
			tail := after[len(original)].Event
			if tail.Kind != api.EventError || tail.ExecutionID != "" || tail.Err == nil || tail.Err.Description != runErr.Error() {
				t.Fatalf("wrong failure record: %#v", tail)
			}
			if !reflect.DeepEqual(observed, after[len(original):]) {
				t.Fatal("observer missed committed ERROR")
			}
			if resumed, err := c.Resume(t.Context(), har); resumed || err != nil {
				t.Fatalf("ERROR-finished retry = %v, %v; want no-op", resumed, err)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
