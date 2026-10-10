package approvalfixture

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

// Losing config/input/cursor on recovery must change the call identity or output and fail.
func TestHarnessApprovalRecovery(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied", true: "approved"}[approved], func(t *testing.T) {
			effects, err := OpenEffects(filepath.Join(t.TempDir(), "effects.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = effects.Close() }()
			log := eventlog.AsStore(eventlog.New())
			c, err := controller.New(log, nil, controller.WithSessionUID("session-a"), controller.WithToolExecutor(effects.Execute), controller.WithStart([]byte("original-config"), 17))
			if err != nil {
				t.Fatal(err)
			}
			h := Harness{}
			err = c.Exec(t.Context(), h, []api.Message{*api.TextMessage("user", "original-input")}, 0)
			if !errors.Is(err, api.ErrApprovalParked) {
				t.Fatalf("park=%v", err)
			}
			records, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			var request eventlog.Record
			for _, r := range records {
				if r.Event.Kind == api.EventToolCall {
					want := api.ToolCall{ID: "fixture-call", Tool: "fixture-effect", IdempotencyKey: "shared-key", Mediation: api.MediationRequiresApproval, Args: map[string]any{"config": "original-config", "input": "original-input", "cursor": float64(17)}}
					if !reflect.DeepEqual(*r.Event.ToolCall, want) {
						t.Fatalf("call=%+v want=%+v", r.Event.ToolCall, want)
					}
				}
				if r.Event.Kind == api.EventApprovalRequest {
					request = r
				}
			}
			if n, err := effects.Count("session-a"); err != nil || n != 0 {
				t.Fatalf("before approval count=%d %v", n, err)
			}
			decision, err := controller.Approve(log, api.ApprovalDecision{ExecutionID: request.Event.ExecutionID, ToolCallID: "fixture-call", RequestSeq: request.Seq, Approved: approved})
			if err != nil {
				t.Fatal(err)
			}
			c, err = controller.New(log, nil, controller.WithSessionUID("session-a"), controller.WithToolExecutor(effects.Execute))
			if err != nil {
				t.Fatal(err)
			}
			if resumed, err := c.Resume(t.Context(), h); !resumed || err != nil {
				t.Fatalf("resume=%v %v", resumed, err)
			}
			records, err = log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			var output string
			for _, r := range records {
				if r.Event.Kind == api.EventOutput {
					output = r.Event.Message.Text()
				}
			}
			var got map[string]any
			if err := json.Unmarshal([]byte(output), &got); err != nil {
				t.Fatal(err)
			}
			wantCode := float64(1)
			wantCount := 0
			if approved {
				wantCode = 0
				wantCount = 1
			}
			if got["config"] != "original-config" || got["input"] != "original-input" || got["cursor"] != float64(17) || got["code"] != wantCode || got["request_seq"] != float64(request.Seq) || got["decision_seq"] != float64(decision.Seq) {
				t.Fatalf("output=%s", output)
			}
			if n, err := effects.Count("session-a"); err != nil || n != wantCount {
				t.Fatalf("effects=%d want %d: %v", n, wantCount, err)
			}
			if out, err := c.Replay(t.Context(), h); err != nil || !reflect.DeepEqual(out, []string{output}) {
				t.Fatalf("replay=%v %v", out, err)
			}
			if n, err := effects.Count("session-a"); err != nil || n != wantCount {
				t.Fatalf("replay effects=%d want %d: %v", n, wantCount, err)
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMemoryDescriptorRefusesApprovalBeforeIntent(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	c, err := controller.New(log, nil, controller.WithSessionUID("memory"))
	if err != nil {
		t.Fatal(err)
	}
	h := Harness{Memory: true}
	desc, err := h.Describe(t.Context())
	if err != nil || desc.Capabilities.Resumability != api.ResumabilityRequiresMemorySnapshot {
		t.Fatalf("descriptor=%+v %v", desc, err)
	}
	if err := c.Exec(t.Context(), h, nil, 0); !errors.Is(err, controller.ErrApprovalUnavailable) {
		t.Fatalf("memory refusal=%v", err)
	}
	records, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Event.Kind == api.EventToolCall || r.Event.Kind == api.EventApprovalRequest || r.Event.Kind == api.EventToolResult {
			t.Fatalf("unsafe event=%s", r.Event.Kind)
		}
	}
}
