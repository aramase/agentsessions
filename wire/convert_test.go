package wire_test

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
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

// toolCallMessage is an assistant message asking for two tool calls, as a model output carries it.
func toolCallMessage() *api.Message {
	return &api.Message{
		Role: "assistant",
		Parts: []api.Part{
			{Text: &api.TextPart{Text: "checking"}},
			{ToolCall: &api.ToolCall{ID: "c1", Tool: "get_weather", Args: map[string]any{
				"city": "Paris", "days": float64(2), "units": map[string]any{"temp": "C"}, "tags": []any{"a", true},
			}}},
			{ToolCall: &api.ToolCall{ID: "c2", Tool: "now"}},
		},
	}
}

// toolResultMessage feeds results back to the model, with content that nests other part kinds.
func toolResultMessage() *api.Message {
	return &api.Message{
		Role: "tool",
		Parts: []api.Part{
			{ToolResult: &api.ToolResult{ID: "c1", Output: map[string]any{"temp": float64(21)}, Content: []api.Part{
				{Text: &api.TextPart{Text: "sunny"}},
				{Data: map[string]any{"temp": float64(21)}},
				{File: &api.FilePart{MIME: "image/png", URI: "blob://map", Digest: "sha256:ff"}},
			}}},
			{ToolResult: &api.ToolResult{ID: "c2", IsError: true, Error: "clock unavailable"}},
		},
	}
}

func TestEventRoundTrip(t *testing.T) {
	actor := api.IdentityRef{Principal: "agent://a", Issuer: "entra", Subject: "sub-1"}
	cases := map[string]api.Event{
		"execution_start": {
			ExecutionID: "e1", Kind: api.EventExecutionStart,
			ExecutionStart: &api.ExecutionStart{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740993, InputCount: proto.Int64(2)},
		},
		"execution_start_cursor_only": {
			ExecutionID: "e1", Kind: api.EventExecutionStart,
			ExecutionStart: &api.ExecutionStart{ResumeFromSeq: -7, InputCount: proto.Int64(1)},
		},
		"execution_start_inputless": {
			ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0)},
		},
		"execution_start_legacy_absent_count": {
			ExecutionID: "e1", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{},
		},
		"input": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventInput,
			Message: api.TextMessage("user", "drive"), Actor: actor,
		},
		"output_rich": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventOutput,
			Message: richMessage(), Actor: actor,
		},
		"output_tool_calls": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventOutput,
			Message: toolCallMessage(), Actor: actor,
		},
		"input_tool_results": {
			ExecutionID: "e1", SchemaVersion: 1, Timestamp: ts, Kind: api.EventInput,
			Message: toolResultMessage(), Actor: actor,
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
		"tool_result_content": {
			Kind: api.EventToolResult,
			Result: &api.ToolResult{ID: "t1", Content: []api.Part{
				{Text: &api.TextPart{Text: "3 hits"}},
				{Data: map[string]any{"hits": float64(3)}},
			}},
		},
		"approval_request": {Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "t1", Reason: "policy"}},
		"approval_result":  {Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "t1", Approved: true, Reason: "ok"}},
		"usage":            {Kind: api.EventUsage, Usage: &api.Usage{Model: "gpt-x", InputTokens: 10, OutputTokens: 20, ReasoningTokens: 5}},
		"lifecycle_fork":   {Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork, Detail: "parent@7"}},
		"lifecycle_suspend": {Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{
			Kind: api.LifecycleSuspend, Snapshot: &api.SnapshotRef{Local: "sess-x"},
		}},
		"lifecycle_suspend_memory": {Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{
			Kind: api.LifecycleSuspend, Snapshot: &api.SnapshotRef{Local: "actor-1", ExternalURI: "gcs://snap/x", Memory: true, Sealed: true},
		}},
		"end":        {Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}},
		"end_failed": {Kind: api.EventEnd, End: &api.HarnessEnd{State: "FAILED", Error: &api.Error{Code: 13, Description: "boom"}}},
		"error":      {Kind: api.EventError, Err: &api.Error{Code: 2, Description: "unknown"}},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			blob, err := proto.Marshal(wire.EventToProto(ev))
			if err != nil {
				t.Fatal(err)
			}
			var decoded v1.Event
			if err := proto.Unmarshal(blob, &decoded); err != nil {
				t.Fatal(err)
			}
			got := wire.EventFromProto(&decoded)
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
// NOTE: this is Go↔Go stability of the wire CONVERSION only — it asserts the conversion is
// lossless for the hashed bytes, not that a non-Go implementation computes the same chain. The
// event log's neutral, cross-implementation hash is defined in package canon (JCS over
// proto3-JSON, contract §7) and is used by both the in-memory and sqlite logs.
func TestHashStableAcrossWire(t *testing.T) {
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Config: []byte{0, 255, ' ', '\n', '\t'}, ResumeFromSeq: 9007199254740993, InputCount: proto.Int64(2)}},
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(0)}},
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{}},
		{Kind: api.EventInput, Message: api.TextMessage("user", "drive")},
		{Kind: api.EventModelCall, ModelCall: &api.ModelCall{Model: "m", Params: map[string]string{"a": "b"}, InputHash: "h", ID: "c1"}},
		{Kind: api.EventOutput, Message: richMessage()},
		{Kind: api.EventOutput, Message: toolCallMessage()},
		{Kind: api.EventInput, Message: toolResultMessage()},
		{Kind: api.EventToolResult, Result: toolResultMessage().Parts[0].ToolResult},
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

// TestToolSpecRoundTrip pins descriptor tool conversion at the public wire boundary: every field and
// every mediation survives a trip through the proto form, and an absent proto decodes to the zero
// value (the nil-tolerant getter behavior Describe has always had).
func TestToolSpecRoundTrip(t *testing.T) {
	for _, want := range []api.ToolSpec{
		{Name: "report", Description: "Report local work", Mediation: api.MediationInHarnessReported},
		{Name: "lookup", Description: "Look up a record", Mediation: api.MediationControllerMediated},
		{Name: "charge", Description: "Request a charge", Mediation: api.MediationRequiresApproval},
		{Name: "default", Description: "Use unspecified mediation"},
		{},
	} {
		p := wire.ToolSpecToProto(want)
		if p == nil {
			t.Fatalf("ToolSpecToProto(%#v) = nil", want)
		}
		if got := wire.ToolSpecFromProto(p); !reflect.DeepEqual(got, want) {
			t.Fatalf("ToolSpec round-trip = %#v, want %#v", got, want)
		}
	}

	// The wire form carries the declared mediation, not just the name.
	if got := wire.ToolSpecToProto(api.ToolSpec{Name: "charge", Mediation: api.MediationRequiresApproval}).GetMediation(); got != v1.Mediation_MEDIATION_REQUIRES_APPROVAL {
		t.Fatalf("wire mediation = %v, want %v", got, v1.Mediation_MEDIATION_REQUIRES_APPROVAL)
	}
	if got := wire.ToolSpecFromProto(nil); !reflect.DeepEqual(got, api.ToolSpec{}) {
		t.Fatalf("ToolSpecFromProto(nil) = %#v, want zero value", got)
	}
}

// TestToolPartsOnTheWire pins that the new Part cases are the reused ToolCall / ToolResult messages
// with structured (Struct) arguments, not a JSON string.
func TestToolPartsOnTheWire(t *testing.T) {
	p := wire.MessageToProto(toolCallMessage())
	tc := p.GetParts()[1].GetToolCall()
	if tc == nil {
		t.Fatalf("part 1 = %v, want a tool_call", p.GetParts()[1])
	}
	if tc.GetId() != "c1" || tc.GetTool() != "get_weather" || tc.GetArgs().GetFields()["city"].GetStringValue() != "Paris" {
		t.Fatalf("tool_call = %v", tc)
	}
	if tc.GetMediation() != v1.Mediation_MEDIATION_UNSPECIFIED || tc.GetIdempotencyKey() != "" {
		t.Fatalf("a model output's tool_call carries mediation %v / key %q", tc.GetMediation(), tc.GetIdempotencyKey())
	}
	tr := wire.MessageToProto(toolResultMessage()).GetParts()[0].GetToolResult()
	if tr.GetId() != "c1" || len(tr.GetContent()) != 3 || tr.GetContent()[0].GetText().GetText() != "sunny" {
		t.Fatalf("tool_result = %v", tr)
	}
}

func TestToolDefinitionsRoundTrip(t *testing.T) {
	want := []api.ToolDefinition{
		{Name: "get_weather", Description: "Current weather", InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"city": map[string]any{"type": "string"}},
			"required":   []any{"city"},
		}},
		{Name: "now"},
	}
	blob, err := proto.Marshal(&v1.ModelCall{Tools: wire.ToolDefinitionsToProto(want)})
	if err != nil {
		t.Fatal(err)
	}
	var decoded v1.ModelCall
	if err := proto.Unmarshal(blob, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := wire.ToolDefinitionsFromProto(decoded.GetTools()); !reflect.DeepEqual(got, want) {
		t.Fatalf("tool definitions round-trip = %#v, want %#v", got, want)
	}
	if got := wire.ToolDefinitionsToProto(nil); got != nil {
		t.Fatalf("ToolDefinitionsToProto(nil) = %v, want nil", got)
	}
	if got := wire.ToolDefinitionsFromProto(nil); got != nil {
		t.Fatalf("ToolDefinitionsFromProto(nil) = %v, want nil", got)
	}
}

func TestToolChoiceRoundTrip(t *testing.T) {
	for _, want := range []*api.ToolChoice{
		nil,
		{},
		{Mode: api.ToolChoiceAuto},
		{Mode: api.ToolChoiceNone},
		{Mode: api.ToolChoiceRequired},
		{Mode: api.ToolChoiceRequired, Name: "get_weather"},
	} {
		p, err := wire.ToolChoiceToProto(want)
		if err != nil {
			t.Fatalf("ToolChoiceToProto(%#v): %v", want, err)
		}
		got, err := wire.ToolChoiceFromProto(p)
		if err != nil {
			t.Fatalf("ToolChoiceFromProto(%v): %v", p, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("tool choice round-trip = %#v, want %#v", got, want)
		}
	}
}

// Proto3 enums are open: a mode this build does not know must not quietly become the provider
// default, which could let the model call a tool the harness ruled out.
func TestToolChoiceRejectsUnknownMode(t *testing.T) {
	if _, err := wire.ToolChoiceFromProto(&v1.ToolChoice{Mode: v1.ToolChoice_Mode(99)}); err == nil {
		t.Fatal("ToolChoiceFromProto accepted an unknown mode")
	}
	if _, err := wire.ToolChoiceToProto(&api.ToolChoice{Mode: "ANY"}); err == nil {
		t.Fatal("ToolChoiceToProto accepted an unknown mode")
	}
}
