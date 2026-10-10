package canon_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/wire"
)

func TestApprovalContractPreservesLegacyCanonicalReceipts(t *testing.T) {
	// Independent literal bytes/hashes for the default-valued pre-contract events at 8d97860.
	for name, tc := range map[string]struct {
		event api.Event
		bytes string
		hash  string
	}{
		"tool result": {api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: "c1"}},
			`{"event":{"kind":"EVENT_TOOL_RESULT","result":{"id":"c1"}},"prev_hash":"","seq":"1"}`,
			"2eddf9826b382796691c15da8ee7dd6af7b41dbab55c949f131ed2933e0b0d8e"},
		"approval denial": {api.Event{Kind: api.EventApprovalResult, ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", Reason: "no"}},
			`{"event":{"approval_result":{"reason":"no","tool_call_id":"c1"},"kind":"EVENT_APPROVAL_RESULT"},"prev_hash":"","seq":"1"}`,
			"c36a909b1ea5f243536ff6b4f1770e99e47b35cb1dfb94c2e943b85a42206bea"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, ev := range []api.Event{tc.event, wire.EventFromProto(wire.EventToProto(tc.event))} {
				b, err := canon.Record("", 1, ev)
				if err != nil || string(b) != tc.bytes {
					t.Fatalf("legacy bytes = %s, %v; want %s", b, err, tc.bytes)
				}
				h, err := canon.HashRecord("", 1, ev)
				if err != nil || h != tc.hash {
					t.Fatalf("legacy hash = %s, %v; want %s", h, err, tc.hash)
				}
			}
		})
	}
}

func TestApprovalReceiptCanonicalCorrelation(t *testing.T) {
	event := api.Event{ExecutionID: "e1", Kind: api.EventToolResult,
		Result: &api.ToolResult{ID: "c1", IsError: true, Code: api.ToolResultCodeApprovalDenied,
			ApprovalRequestSeq: 9007199254740993, ApprovalDecisionSeq: 9007199254740994}}
	const want = `{"execution_id":"e1","kind":"EVENT_TOOL_RESULT","result":{"approval_decision_seq":"9007199254740994","approval_request_seq":"9007199254740993","code":"APPROVAL_DENIED","id":"c1","is_error":true}}`
	b, err := canon.Event(event)
	if err != nil || string(b) != want {
		t.Fatalf("receipt canonical bytes = %s, %v; want %s", b, err, want)
	}
	for _, ev := range []api.Event{
		event,
		{Kind: api.EventInput, Message: &api.Message{Role: "tool", Parts: []api.Part{{ToolResult: event.Result}}}},
	} {
		before, err := canon.HashRecord("", 1, ev)
		if err != nil {
			t.Fatal(err)
		}
		event.Result.Approval = &api.ApprovalResult{ToolCallID: "c1", RequestSeq: 9007199254740993, Approved: true, Reason: "transient"}
		after, err := canon.HashRecord("", 1, ev)
		if err != nil || before != after {
			t.Fatalf("transient approval changed receipt/model history hash: %s -> %s, %v", before, after, err)
		}
		event.Result.Approval = nil
	}
}

func TestApprovalDecisionCanonicalRequestAndProvenance(t *testing.T) {
	event := api.Event{ExecutionID: "e1", Kind: api.EventApprovalResult, Actor: api.IdentityRef{Principal: "human"},
		ApprovalResult: &api.ApprovalResult{ToolCallID: "c1", Reason: "no", RequestSeq: 9007199254740993}}
	const want = `{"actor":{"principal":"human"},"approval_result":{"reason":"no","request_seq":"9007199254740993","tool_call_id":"c1"},"execution_id":"e1","kind":"EVENT_APPROVAL_RESULT"}`
	b, err := canon.Event(event)
	if err != nil || string(b) != want {
		t.Fatalf("decision canonical bytes = %s, %v; want %s", b, err, want)
	}
	original, err := canon.HashRecord("", 1, event)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*api.Event){
		"request":    func(e *api.Event) { e.ApprovalResult.RequestSeq-- },
		"decision":   func(e *api.Event) { e.ApprovalResult.Approved = true },
		"reason":     func(e *api.Event) { e.ApprovalResult.Reason = "other" },
		"provenance": func(e *api.Event) { e.Actor.Principal = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := event
			decision := *event.ApprovalResult
			changed.ApprovalResult = &decision
			mutate(&changed)
			h, err := canon.HashRecord("", 1, changed)
			if err != nil || h == original {
				t.Fatalf("decision mutation not bound into hash: %s, %v", h, err)
			}
		})
	}
}

func TestApprovalContractPreservesSessionMetadata(t *testing.T) {
	// pending_approval is additive, output-only state. With it unset, existing resource and
	// caller metadata retain exactly their old canonical mapping, including int64 strings.
	sess := &v1.Session{Metadata: &v1.ResourceMetadata{Project: "tenant", Name: "name", Uid: "s1", Version: 7},
		Harness: "echo", Model: "m", LastSeq: 99,
		Identity: &v1.IdentityRef{Principal: "p", Issuer: "i", Subject: "s"},
		Origin:   &v1.Origin{Source: "source", Subject: "subject", Uri: "uri", Attributes: map[string]string{"context": "value"}},
		Labels:   map[string]string{"label": "value"}, Annotations: map[string]string{"note": "value"}}
	const want = `{"annotations":{"note":"value"},"harness":"echo","identity":{"issuer":"i","principal":"p","subject":"s"},"labels":{"label":"value"},"last_seq":"99","metadata":{"name":"name","project":"tenant","uid":"s1","version":"7"},"model":"m","origin":{"attributes":{"context":"value"},"source":"source","subject":"subject","uri":"uri"}}`
	b, err := canon.Proto(sess)
	if err != nil || string(b) != want {
		t.Fatalf("session metadata canonical bytes = %s, %v; want %s", b, err, want)
	}
}

func TestApprovalContractPreservesRegistrySpecDigest(t *testing.T) {
	// #100 hashes HarnessSpec, not Session/ControllerFrame or descriptive metadata.
	// These literal bytes and independently computed digest must survive common schema additions.
	spec := &v1.HarnessSpec{DescriptorId: "echo", Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY},
		Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "127.0.0.1:9000"}}}
	const want = `{"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"},"descriptor_id":"echo","remote":{"address":"127.0.0.1:9000"}}`
	const wantDigest = "sha256:f0c96c8b67d5dc75be557f64ccd59496eaf9cfb102754ee54e9d4c571eb2eafc"
	b, err := canon.Proto(spec)
	if err != nil || string(b) != want {
		t.Fatalf("registry spec canonical bytes = %s, %v; want %s", b, err, want)
	}
	sum := sha256.Sum256(b)
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != wantDigest {
		t.Fatalf("registry spec digest = %s, want %s", got, wantDigest)
	}
}
