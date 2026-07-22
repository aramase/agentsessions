package wire_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/wire"
)

// ts is a fixed instant that survives a timestamppb round-trip (UTC, no monotonic reading).
var ts = time.Unix(1_700_000_000, 123456789).UTC()

// richMessage exercises every Part variant so the content model round-trips in full.
func richMessage() *api.Message {
	return &api.Message{
		Role: "assistant",
		Parts: []api.Part{
			{Text: &api.TextPart{Text: "hello"}},
			{File: &api.FilePart{MIME: "image/png", Bytes: []byte{1, 2, 3}, Name: "img", Digest: "sha256:aa"}},
			{File: &api.FilePart{MIME: "text/plain", URI: "blob://x", Digest: "sha256:bb"}},
			{Data: map[string]any{
				"s":      "v",
				"n":      float64(42),
				"b":      true,
				"nested": map[string]any{"x": float64(1.5)},
				"arr":    []any{"a", float64(2)},
			}},
			{Reasoning: &api.ReasoningPart{
				Provider:      "anthropic",
				ModelID:       "claude-x",
				Opaque:        []byte("sig-bytes"),
				OpaqueDigest:  "sha256:cc",
				ItemID:        "rs_1",
				Ordinal:       2,
				ValidityScope: "exec-1",
				Summary:       []api.Part{{Text: &api.TextPart{Text: "brief"}}},
			}},
		},
	}
}

func TestEventRoundTrip(t *testing.T) {
	actor := api.IdentityRef{Principal: "agent://a", Issuer: "entra", Subject: "sub-1"}
	cases := map[string]api.Event{
		"input": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventInput,
			Message: api.TextMessage("user", "drive"), Actor: actor,
		},
		"output_rich": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventOutput,
			Message: richMessage(), Actor: actor,
		},
		"model_call": {
			Kind: api.EventModelCall,
			ModelCall: &api.ModelCall{
				Model: "azure-openai/gpt-x", Params: map[string]string{"temperature": "0"},
				InputHash: "sha256:dd", ID: "mc-1",
			},
		},
		"tool_call": {
			Kind: api.EventToolCall,
			ToolCall: &api.ToolCall{
				ID: "t1", Tool: "search", Args: map[string]any{"q": "k8s"},
				Mediation: api.MediationControllerMediated, IdempotencyKey: "idem-1",
			},
		},
		"tool_result": {
			Kind: api.EventToolResult,
			Result: &api.ToolResult{
				ID: "t1", Output: map[string]any{"hits": float64(3)},
				OutputURI: "blob://out", OutputDigest: "sha256:ee", IsError: false,
			},
		},
		"approval_request": {Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "t1", Reason: "policy"}},
		"approval_result":  {Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "t1", Approved: true, Reason: "ok"}},
		"usage":            {Kind: api.EventUsage, Usage: &api.Usage{Model: "gpt-x", InputTokens: 10, OutputTokens: 20, ReasoningTokens: 5}},
		"lifecycle_fork":   {Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: "parent@7"}},
		"end":              {Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		"end_failed":       {Kind: api.EventEnd, End: &api.HarnessEnd{State: "FAILED", Error: &api.Error{Code: 13, Description: "boom"}}},
		"error":            {Kind: api.EventError, Err: &api.Error{Code: 2, Description: "unknown"}},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			got := wire.EventFromProto(wire.EventToProto(ev))
			if !reflect.DeepEqual(ev, got) {
				t.Fatalf("round-trip mismatch\n want: %#v\n got:  %#v", ev, got)
			}
		})
	}
}

// TestHashStableAcrossWire proves that a proto round-trip (EventToProto→EventFromProto) yields
// an Event that hashes to the SAME value in a Go log — i.e. the Go conversion is lossless for
// the hashed bytes, so record/replay identity (I5) holds across the gRPC wire for a Go host.
//
// NOTE: this is Go↔Go stability of the wire CONVERSION only. The event log's neutral,
// cross-implementation hash is defined in package canon (JCS over proto3-JSON, contract §7) and
// eventlog.hashRecord is wired to it in m0-eventlog. Until then eventlog hashes Go encoding/json,
// so this test asserts conversion determinism, not language-neutral verifiability.
func TestHashStableAcrossWire(t *testing.T) {
	events := []api.Event{
		{Kind: api.EventInput, Message: api.TextMessage("user", "drive")},
		{Kind: api.EventModelCall, ModelCall: &api.ModelCall{Model: "m", Params: map[string]string{"a": "b"}, InputHash: "h", ID: "c1"}},
		{Kind: api.EventOutput, Message: richMessage()},
	}
	for i, ev := range events {
		direct := eventlog.New()
		r1, err := direct.Append(0, direct.NewFence(), ev)
		if err != nil {
			t.Fatalf("case %d direct append: %v", i, err)
		}
		roundtripped := wire.EventFromProto(wire.EventToProto(ev))
		viaWire := eventlog.New()
		r2, err := viaWire.Append(0, viaWire.NewFence(), roundtripped)
		if err != nil {
			t.Fatalf("case %d wire append: %v", i, err)
		}
		if r1.Hash != r2.Hash {
			t.Fatalf("case %d hash diverged across wire: %s != %s", i, r1.Hash, r2.Hash)
		}
	}
}
