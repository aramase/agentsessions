package conformance_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
)

type toolRequestHarness struct{ call api.ToolCall }

func (toolRequestHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "tool-request", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h toolRequestHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, err := sink.ToolCall(ctx, h.call)
	return err
}

// Identity checks must hold across Harness.Connect using only the proto contract, before either
// serving a recorded result or re-driving a terminal intent from the durable sqlite journal.
func TestWireToolRequestChangesFailReplayAndRecovery(t *testing.T) {
	for _, change := range []struct {
		name   string
		change func(*api.ToolCall)
	}{
		{"id", func(c *api.ToolCall) { c.ID = "changed" }},
		{"name", func(c *api.ToolCall) { c.Tool = "write" }},
		{"args", func(c *api.ToolCall) { c.Args["count"] = 3 }},
		{"mediation", func(c *api.ToolCall) { c.Mediation = api.MediationRequiresApproval }},
		{"key", func(c *api.ToolCall) { c.IdempotencyKey = "changed" }},
	} {
		for _, path := range []string{"replay", "resume result", "resume intent"} {
			t.Run(path+"/"+change.name, func(t *testing.T) {
				store, _ := openFile(t)
				defer store.Close()
				log := store.Session("s")
				interrupted := errors.New("interrupted")
				c, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					if path == "resume intent" {
						return api.ToolResult{}, interrupted
					}
					return api.ToolResult{Output: map[string]any{"receipt": "recorded"}}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				call := api.ToolCall{ID: "call-1", Tool: "read", Args: map[string]any{"count": 2}, Mediation: api.MediationControllerMediated, IdempotencyKey: "key-1"}
				err = c.Exec(t.Context(), wireHarnessFrom(t, toolRequestHarness{call: call}), []api.Message{*api.TextMessage("user", "read")}, 0)
				if path != "resume intent" && err != nil || path == "resume intent" && !errors.Is(err, interrupted) {
					t.Fatalf("fixture: %v", err)
				}
				if path == "resume result" {
					// Fork at the result, not a hard-coded sequence: EXECUTION_START precedes input.
					recs, err := log.Read(1)
					if err != nil {
						t.Fatal(err)
					}
					child := store.Session("child")
					var atSeq int64
					for _, rec := range recs {
						if rec.Event.Kind == api.EventToolResult {
							atSeq = rec.Seq
						}
					}
					if atSeq == 0 {
						t.Fatal("fixture has no result")
					}
					if err := controller.Fork(log, child, atSeq); err != nil {
						t.Fatal(err)
					}
					log = child
				}
				attempts := 0
				fresh, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
				if err != nil {
					t.Fatal(err)
				}
				change.change(&call)
				h := wireHarnessFrom(t, toolRequestHarness{call: call})
				if path == "replay" {
					_, err = fresh.Replay(t.Context(), h)
				} else {
					_, err = fresh.Resume(t.Context(), h)
				}
				if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 || fresh.ToolInvocations() != 0 || fresh.ModelInvocations() != 0 {
					t.Fatalf("remote divergence accepted: attempts=%d err=%v", attempts, err)
				}
				if err := log.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
