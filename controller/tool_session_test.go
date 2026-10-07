package controller_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/sqlitelog"
)

type sessionToolHarness struct{}

func (sessionToolHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "session-tool"}, nil
}

func (sessionToolHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	result, err := sink.ToolCall(ctx, sessionToolCall())
	if err != nil {
		return err
	}
	return sink.Output(ctx, fmt.Sprint(result.Output["receipt"]))
}

func sessionToolCall() api.ToolCall {
	return api.ToolCall{ID: "call", Tool: "charge", Mediation: api.MediationControllerMediated, IdempotencyKey: "shared-key"}
}

// Losing WithSessionUID at execTool must fail both live delivery and interrupted re-drive.
// Direct controller users that omit the binding retain an empty executor session identity.
func TestToolExecutorReceivesConfiguredSessionUID(t *testing.T) {
	for _, tc := range []struct {
		name, uid, receipt string
		resume             bool
	}{
		{"live configured", "configured-session", "receipt:configured-session", false},
		{"resume configured", "configured-session", "receipt:configured-session", true},
		{"live unconfigured", "", "receipt:", false},
		{"resume unconfigured", "", "receipt:", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("journal-session")
			if tc.resume {
				fence, err := log.NewFence()
				if err != nil {
					t.Fatal(err)
				}
				call := sessionToolCall()
				for i, ev := range []api.Event{
					{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0)}},
					{Kind: api.EventToolCall, ToolCall: &call},
				} {
					ev.ExecutionID = "interrupted"
					if _, err := log.Append(int64(i), fence, ev); err != nil {
						t.Fatal(err)
					}
				}
			}
			opts := []controller.Option{controller.WithToolExecutor(func(_ context.Context, uid string, call api.ToolCall) (api.ToolResult, error) {
				if uid != tc.uid || call.ID != "call" || call.Tool != "charge" || call.Mediation != api.MediationControllerMediated || call.IdempotencyKey != "shared-key" {
					return api.ToolResult{}, fmt.Errorf("executor session = %q, want %q; call = %+v", uid, tc.uid, call)
				}
				return api.ToolResult{Output: map[string]any{"receipt": "receipt:" + uid}}, nil
			})}
			if tc.uid != "" {
				opts = append(opts, controller.WithSessionUID(tc.uid))
			}
			c, err := controller.New(log, echoModel, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if tc.resume {
				if resumed, err := c.Resume(t.Context(), sessionToolHarness{}); err != nil || !resumed {
					t.Fatalf("Resume = %v, %v", resumed, err)
				}
			} else if err := c.Exec(t.Context(), sessionToolHarness{}, nil, 0); err != nil {
				t.Fatal(err)
			}
			if outputs, err := c.Outputs(); err != nil || !reflect.DeepEqual(outputs, []string{tc.receipt}) {
				t.Fatalf("Outputs = %v, %v; want [%s]", outputs, err, tc.receipt)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
