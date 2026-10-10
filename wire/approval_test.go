package wire_test

import (
	"context"
	"encoding/hex"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/wire"
)

func TestApprovalContractRoundTrip(t *testing.T) {
	for name, want := range map[string]api.Event{
		"approved": {Kind: api.EventApprovalResult, ExecutionID: "e1", Actor: api.IdentityRef{Principal: "human", Issuer: "oidc", Subject: "u1"},
			ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", Approved: true, Reason: "yes", RequestSeq: 9007199254740993}},
		"denied": {Kind: api.EventApprovalResult, ExecutionID: "e1", ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", Reason: "no", RequestSeq: 42}},
		"parked": {Kind: api.EventParked, ExecutionID: "e1", Parked: &api.ApprovalRef{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 9007199254740993}},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := proto.Marshal(wire.EventToProto(want))
			if err != nil {
				t.Fatal(err)
			}
			var decoded v1.Event
			if err := proto.Unmarshal(b, &decoded); err != nil {
				t.Fatal(err)
			}
			if got := wire.EventFromProto(&decoded); !reflect.DeepEqual(got, want) {
				t.Fatalf("approval round-trip = %#v, want %#v", got, want)
			}
		})
	}
}

func TestToolReceiptRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		code api.ToolResultCode
		wire v1.ToolResult_Code
	}{
		{api.ToolResultCodeUnspecified, v1.ToolResult_CODE_UNSPECIFIED},
		{api.ToolResultCodeApprovalDenied, v1.ToolResult_APPROVAL_DENIED},
		{api.ToolResultCodeExecutorError, v1.ToolResult_EXECUTOR_ERROR},
		{api.ToolResultCode(99), v1.ToolResult_Code(99)},
	} {
		want := api.ToolResult{ID: "c1", Code: tc.code, IsError: tc.code != api.ToolResultCodeUnspecified, Error: "receipt",
			ApprovalRequestSeq: 9007199254740993, ApprovalDecisionSeq: 9007199254740994}
		p := wire.ToolResultToProto(&want)
		if p.GetCode() != tc.wire || p.GetApprovalRequestSeq() != want.ApprovalRequestSeq || p.GetApprovalDecisionSeq() != want.ApprovalDecisionSeq {
			t.Fatalf("wire receipt lost correlation/status: %v", p)
		}
		if got := wire.ToolResultFromProto(p); !reflect.DeepEqual(got, &want) {
			t.Fatalf("receipt round-trip = %#v, want %#v", got, want)
		}
		message := &api.Message{Role: "tool", Parts: []api.Part{{ToolResult: &want}}}
		if got := wire.MessageFromProto(wire.MessageToProto(message)); !reflect.DeepEqual(got, message) {
			t.Fatalf("model-facing receipt round-trip = %#v, want %#v", got, message)
		}
	}
	if wire.ToolResultToProto(nil) != nil || wire.ToolResultFromProto(nil) != nil {
		t.Fatal("nil receipt conversions must remain nil")
	}
}

func TestTransientApprovalExcludedFromWireReceipt(t *testing.T) {
	base := api.ToolResult{ID: "c1", Code: api.ToolResultCodeApprovalDenied, IsError: true, Error: "denied",
		ApprovalRequestSeq: 42, ApprovalDecisionSeq: 43}
	carrier := base
	carrier.Approval = &api.ApprovalResult{ToolCallID: "c1", RequestSeq: 42, Reason: "no"}
	for name, convert := range map[string]func(api.ToolResult) proto.Message{
		"receipt": func(r api.ToolResult) proto.Message { return wire.ToolResultToProto(&r) },
		"event": func(r api.ToolResult) proto.Message {
			return wire.EventToProto(api.Event{Kind: api.EventToolResult, Result: &r})
		},
		"model history": func(r api.ToolResult) proto.Message {
			return wire.MessageToProto(&api.Message{Role: "tool", Parts: []api.Part{{ToolResult: &r}}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if !proto.Equal(convert(base), convert(carrier)) {
				t.Fatal("transient approval leaked into wire receipt/history")
			}
		})
	}
	got := wire.ToolResultFromProto(wire.ToolResultToProto(&carrier))
	if got.Approval != nil || !reflect.DeepEqual(got, &base) {
		t.Fatalf("decoded receipt = %#v, want durable fields only %#v", got, base)
	}
}

func TestApproveDecisionPresenceAndGeneratedStub(t *testing.T) {
	for _, decision := range []*bool{nil, proto.Bool(false), proto.Bool(true)} {
		want := &v1.ApproveRequest{Session: "s1", ExecutionId: "e1", ToolCallId: "c1", RequestSeq: 9007199254740993,
			Approved: decision, Reason: "decision", Identity: &v1.IdentityRef{Principal: "human", Issuer: "oidc", Subject: "u1"}}
		b, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got v1.ApproveRequest
		if err := proto.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&got, want) || (got.Approved == nil) != (decision == nil) {
			t.Fatalf("decision presence/tuple lost: %v, want %v", &got, want)
		}
	}
	// The generated server contract is implemented at the public validation boundary.
	var service v1.SessionsServer = session.NewService(nil, nil)
	_, err := service.Approve(context.Background(), &v1.ApproveRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Approve = %v, want InvalidArgument for an absent decision tuple", err)
	}
}

func TestApprovalContractPreservesLegacyBinaryReceipts(t *testing.T) {
	for _, tc := range []struct {
		event api.Event
		want  string
	}{
		{api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1"}}, "38055a040a026331"},
		{api.Event{Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", Reason: "no"}}, "38076a080a0263311a026e6f"},
	} {
		// Literal protobuf bytes from the pre-contract scalar/oneof tags. New zero fields
		// must not be emitted; a denied durable approved bool still has no explicit presence.
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(wire.EventToProto(tc.event))
		if err != nil || hex.EncodeToString(b) != tc.want {
			t.Fatalf("legacy binary receipt = %x, %v; want %s", b, err, tc.want)
		}
	}
}

func TestApprovalTransportMessagesRoundTrip(t *testing.T) {
	ref := &v1.ApprovalRef{ExecutionId: "e1", ToolCallId: "c1", RequestSeq: 9007199254740993}
	for _, want := range []proto.Message{
		&v1.ControllerFrame{Session: "s1", ExecutionId: "e1", Frame: &v1.ControllerFrame_Park{Park: ref}},
		&v1.Session{Metadata: &v1.ResourceMetadata{Uid: "s1"}, PendingApproval: ref},
		&v1.ApproveResponse{Session: &v1.Session{Metadata: &v1.ResourceMetadata{Uid: "s1"}},
			Decision: &v1.LogRecord{Seq: 43, Event: &v1.Event{ExecutionId: "e1", Kind: v1.EventKind_EVENT_APPROVAL_RESULT,
				Body: &v1.Event_ApprovalResult{ApprovalResult: &v1.ApprovalResult{ToolCallId: "c1", RequestSeq: 42}}}}},
	} {
		b, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		got := want.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(b, got); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("transport round-trip = %v, want %v", got, want)
		}
	}
}
