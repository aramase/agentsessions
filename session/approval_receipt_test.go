package session_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
)

// A gated executor failure is returned as a durable receipt that the harness can feed
// to the model over the actual bridge, not a ToolCall transport error that ends the turn.
type publicReceiptHarness struct{}

func (publicReceiptHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (&publicGateHarness{}).Describe(ctx)
}

func (publicReceiptHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	result, err := sink.ToolCall(ctx, publicGateCall("call"))
	if err != nil {
		return err
	}
	_, err = sink.Model(ctx, api.ModelRequest{Model: "test-model", Messages: []api.Message{
		{Role: "tool", Parts: []api.Part{{ToolResult: &result}}},
	}})
	return err
}

func TestPublicApprovalExecutorFailureReceiptContinuesModel(t *testing.T) {
	store := openStore(t, ":memory:")
	b := local.New(publicReceiptHarness{})
	t.Cleanup(func() { _ = b.Close() })
	var effects, models atomic.Int32
	p := placement.New(b, func(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		if len(req.Messages) != 1 || len(req.Messages[0].Parts) != 1 {
			t.Errorf("model did not receive the tool receipt: %+v", req)
		} else if receipt := req.Messages[0].Parts[0].ToolResult; receipt == nil || receipt.ID != "call" || !receipt.IsError || receipt.Code != api.ToolResultCodeExecutorError || receipt.Error != "executor offline" || receipt.ApprovalRequestSeq <= 0 || receipt.ApprovalDecisionSeq <= receipt.ApprovalRequestSeq {
			t.Errorf("model receipt lost failure/correlation: %+v", receipt)
		}
		return api.ModelResponse{Message: *api.TextMessage("assistant", "handled executor failure")}, nil
	}, placement.WithToolExecutor(func(_ context.Context, scope controller.ToolCallContext, _ api.ToolCall) (api.ToolResult, error) {
		effects.Add(1)
		if scope.SessionUID == "" {
			t.Error("executor missing session scope")
		}
		return api.ToolResult{}, errors.New("executor offline")
	}))
	r, err := placement.NewRegistry("gate", map[string]*placement.Placer{"gate": p})
	if err != nil {
		t.Fatal(err)
	}
	c := serveRegistry(t, store, r)
	uid := mustCreate(t, c)
	frames, err := collectPublicExec(c, &v1.ExecRequest{Session: uid})
	if err != nil {
		t.Fatal(err)
	}
	ref := frames[len(frames)-1].GetSession().GetPendingApproval()
	decision, err := c.Approve(t.Context(), &v1.ApproveRequest{Session: uid, ExecutionId: ref.ExecutionId, ToolCallId: ref.ToolCallId, RequestSeq: ref.RequestSeq, Approved: proto.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if err != nil {
		t.Fatalf("handled executor receipt became RPC error: %v", err)
	}
	if current.PendingApproval != nil || effects.Load() != 1 || models.Load() != 1 {
		t.Fatalf("completed recovery=%v effects=%d models=%d", current, effects.Load(), models.Load())
	}
	records := routingRecords(t, store.Session(uid))
	receipt := routingEvent(t, records, api.EventToolResult).Result
	if receipt.Code != api.ToolResultCodeExecutorError || !receipt.IsError || receipt.ApprovalRequestSeq != ref.RequestSeq || receipt.ApprovalDecisionSeq != decision.Decision.Seq {
		t.Fatalf("durable failure receipt=%+v", receipt)
	}
	if output := routingEvent(t, records, api.EventOutput).Message; output == nil || output.Text() != "handled executor failure" {
		t.Fatalf("model continuation output=%+v", output)
	}
	for _, record := range records {
		if record.Event.Kind == api.EventError {
			t.Fatalf("handled receipt journaled ERROR: %+v", record)
		}
	}
}
