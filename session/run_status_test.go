package session_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

type runStatusHarness struct {
	model bool
	runs  atomic.Int32
}

func (*runStatusHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "run-status", Version: "v1"}, nil
}

func (h *runStatusHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	h.runs.Add(1)
	if h.model {
		_, err := sink.Model(ctx, api.ModelRequest{Model: "model"})
		return err
	}
	return fmt.Errorf("Run failed: %w", status.Error(codes.Unavailable, "mid-turn unavailable"))
}

// Describe outages are retryable; a Run or model failure has already entered the turn and retains
// the existing Internal code and ERROR journal record, even if its transport status is Unavailable.
func TestRunAndModelUnavailableKeepExistingClassification(t *testing.T) {
	for _, operation := range []string{"Exec", "Resume"} {
		for _, model := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/model=%v", operation, model), func(t *testing.T) {
				store := openStore(t, ":memory:")
				const uid = "run-status"
				if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
					t.Fatal(err)
				}
				log := store.Session(uid)
				if operation == "Resume" {
					fence, err := log.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := log.Append(0, fence, api.Event{Kind: api.EventExecutionStart, ExecutionID: "pending", ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0), Harness: "alias-a", HarnessVersion: "v1"}}); err != nil {
						t.Fatal(err)
					}
				}
				before := routingRecords(t, log)
				h := &runStatusHarness{model: model}
				backend := local.New(h)
				t.Cleanup(func() { _ = backend.Close() })
				var models atomic.Int32
				p := placement.New(backend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
					models.Add(1)
					return api.ModelResponse{}, fmt.Errorf("model failed: %w", status.Error(codes.Unavailable, "mid-turn unavailable"))
				})
				client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{"alias-a": p}))
				var err error
				if operation == "Exec" {
					stream, execErr := client.Exec(t.Context(), &v1.ExecRequest{Session: uid})
					err = execErr
					if err == nil {
						err = drainExec(stream)
					}
				} else {
					_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
				}
				if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "mid-turn unavailable") {
					t.Fatalf("mid-turn status changed: %v, want Internal", err)
				}
				after := routingRecords(t, log)
				failure := routingEvent(t, after, api.EventError).Err
				if len(after) <= len(before) || len(before) > 0 && !reflect.DeepEqual(before, after[:len(before)]) || failure == nil || !strings.Contains(failure.Description, "mid-turn unavailable") {
					t.Fatalf("mid-turn failure did not journal ERROR: %+v", after)
				}
				wantModels := int32(0)
				if model {
					wantModels = 1
				}
				if h.runs.Load() != 1 || models.Load() != wantModels {
					t.Fatalf("unexpected execution count: Run/model=%d/%d want 1/%d", h.runs.Load(), models.Load(), wantModels)
				}
			})
		}
	}
}
