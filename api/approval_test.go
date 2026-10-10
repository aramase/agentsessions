package api_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aramase/agentsessions/api"
)

func TestApprovalParkedErrorCarriesCorrelation(t *testing.T) {
	ref := api.ApprovalRef{ExecutionID: "e1", ToolCallID: "c1", RequestSeq: 9007199254740993}
	err := fmt.Errorf("run: %w", &api.ApprovalParkedError{Ref: ref})
	if !errors.Is(err, api.ErrApprovalParked) {
		t.Fatalf("error = %v, want ErrApprovalParked", err)
	}
	var parked *api.ApprovalParkedError
	if !errors.As(err, &parked) || parked.Ref != ref {
		t.Fatalf("parked correlation = %#v, want %#v", parked, ref)
	}
	if errors.Is(errors.New("approval parked"), api.ErrApprovalParked) {
		t.Fatal("matching error text must not stand in for the typed sentinel")
	}
}

func TestModelInputReceiptCompatibility(t *testing.T) {
	// Literal JSON and SHA-256 from the pre-approval-contract shape at 8d97860.
	// Do not derive the expectation via a converter or from the new receipt type.
	const want = `{"Model":"m","Messages":[{"Role":"tool","Parts":[{"Text":null,"File":null,"Data":null,"Reasoning":null,"ToolResult":{"ID":"c1","Output":null,"OutputURI":"","OutputDigest":"","IsError":false,"Error":""}}]}],"Params":null}`
	const wantHash = "e519b8f957d02755a004b638c9d819f925e8bb9b65fa06a71bfd4f121b95472a"
	for _, approval := range []*api.ApprovalResult{
		nil,
		{ToolCallID: "c1", Approved: true, Reason: "yes", RequestSeq: 42},
		{ToolCallID: "other", Approved: false, Reason: "no", RequestSeq: 99},
	} {
		req := api.ModelRequest{Model: "m", Messages: []api.Message{{Role: "tool", Parts: []api.Part{
			{ToolResult: &api.ToolResult{ID: "c1", Approval: approval}},
		}}}}
		b, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if string(b) != want || hex.EncodeToString(sum[:]) != wantHash {
			t.Fatalf("legacy model request changed: %s, hash %x; want %s, hash %s", b, sum, want, wantHash)
		}
	}
}

func TestApprovalResultJSONCompatibility(t *testing.T) {
	for _, tc := range []struct {
		result api.ApprovalResult
		want   string
	}{
		{api.ApprovalResult{ToolCallID: "c1", Reason: "no"}, `{"ToolCallID":"c1","Approved":false,"Reason":"no"}`},
		{api.ApprovalResult{ToolCallID: "c1", Reason: "no", RequestSeq: 42}, `{"ToolCallID":"c1","Approved":false,"Reason":"no","RequestSeq":42}`},
	} {
		b, err := json.Marshal(tc.result)
		if err != nil || string(b) != tc.want {
			t.Fatalf("approval JSON = %s, %v; want %s", b, err, tc.want)
		}
	}
}

func TestModelInputBindsDurableApprovalReceipt(t *testing.T) {
	base := api.ToolResult{ID: "c1", IsError: true, Error: "denied", Code: api.ToolResultCodeApprovalDenied,
		ApprovalRequestSeq: 9007199254740993, ApprovalDecisionSeq: 9007199254740994}
	encode := func(result api.ToolResult) string {
		t.Helper()
		b, err := json.Marshal(api.ModelRequest{Model: "m", Messages: []api.Message{{Role: "tool", Parts: []api.Part{{ToolResult: &result}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	original := encode(base)
	for name, mutate := range map[string]func(*api.ToolResult){
		"code":     func(r *api.ToolResult) { r.Code = api.ToolResultCodeExecutorError },
		"request":  func(r *api.ToolResult) { r.ApprovalRequestSeq-- },
		"decision": func(r *api.ToolResult) { r.ApprovalDecisionSeq-- },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if encode(changed) == original {
				t.Fatal("durable receipt change did not change model input")
			}
		})
	}
}
