package placement_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

type describeBoundaryHarness struct {
	api.Harness
	describe func(context.Context) (api.Descriptor, error)
}

func (h describeBoundaryHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return h.describe(ctx)
}

func TestControllerDescribeBoundaryUsesAdmissionClassification(t *testing.T) {
	for _, operation := range []string{"Exec", "Resume"} {
		for _, tc := range []struct {
			name      string
			peerErr   error
			cancel    bool
			deadline  bool
			want      error
			wantCause error
			notCause  error
		}{
			{name: "unavailable", peerErr: status.Error(codes.Unavailable, "peer unavailable"), want: placement.ErrHarnessUnavailable},
			{name: "peer deadline", peerErr: status.Error(codes.DeadlineExceeded, "peer deadline"), want: placement.ErrHarnessUnavailable},
			{name: "internal describe timeout", peerErr: fmt.Errorf("peer timeout: %w", context.DeadlineExceeded), want: placement.ErrHarnessUnavailable},
			{name: "caller cancelled overrides peer timeout", peerErr: fmt.Errorf("peer timeout: %w", context.DeadlineExceeded), cancel: true, want: placement.ErrAdmissionInterrupted, wantCause: context.Canceled, notCause: context.DeadlineExceeded},
			{name: "caller deadline", peerErr: status.Error(codes.Unavailable, "peer unavailable"), deadline: true, want: placement.ErrAdmissionInterrupted, wantCause: context.DeadlineExceeded},
			{name: "unsupported Describe", peerErr: status.Error(codes.Unimplemented, "no Describe"), want: controller.ErrHarnessDescribeFailed},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				store, err := sqlitelog.Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				log := store.Session("describe-boundary")
				if operation == "Resume" {
					fence, err := log.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := log.Append(0, fence, api.Event{Kind: api.EventExecutionStart, ExecutionID: "pending", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0), Harness: "alias", HarnessVersion: "v1"}}); err != nil {
						t.Fatal(err)
					}
				}
				before := toolRecords(t, log)
				base := &identityToolHarness{version: "v1"}
				backend := local.New(base)
				t.Cleanup(func() { _ = backend.Close() })
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.deadline {
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithTimeout(ctx, 50*time.Millisecond)
					defer deadlineCancel()
				}
				var closes, models, tools atomic.Int32
				h := describeBoundaryHarness{Harness: base, describe: func(dctx context.Context) (api.Descriptor, error) {
					if tc.cancel {
						cancel()
					}
					if tc.deadline {
						<-dctx.Done()
					}
					return api.Descriptor{}, tc.peerErr
				}}
				p := placement.New(backend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
					models.Add(1)
					return api.ModelResponse{}, nil
				}, placement.WithDialer(func(string) (api.Harness, func() error, error) {
					return h, func() error { closes.Add(1); return nil }, nil
				}), placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
					tools.Add(1)
					return api.ToolResult{}, nil
				}))
				if operation == "Exec" {
					_, err = p.Exec(ctx, log, "describe-boundary", nil, 0, placement.WithHarness("alias"))
				} else {
					err = p.Resume(ctx, log, "describe-boundary", placement.WithResumeHarness("alias"))
				}
				if !errors.Is(err, tc.want) || tc.wantCause != nil && !errors.Is(err, tc.wantCause) || tc.notCause != nil && errors.Is(err, tc.notCause) {
					t.Fatalf("error=%v want=%v cause=%v excluded=%v", err, tc.want, tc.wantCause, tc.notCause)
				}
				if tc.want == controller.ErrHarnessDescribeFailed && (errors.Is(err, placement.ErrHarnessUnavailable) || errors.Is(err, placement.ErrAdmissionInterrupted)) {
					t.Fatalf("non-outage Describe error misclassified: %v", err)
				}
				if base.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || closes.Load() != 1 || !reflect.DeepEqual(before, toolRecords(t, log)) {
					t.Fatalf("Describe failure ran effects, changed records, or leaked connection: Run/model/tool/close=%d/%d/%d/%d", base.runs.Load(), models.Load(), tools.Load(), closes.Load())
				}
			})
		}
	}
}
