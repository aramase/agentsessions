// Package wire converts between the hand-written api domain types (the SPI programmed
// against internally) and the generated protobuf wire types in api/genpb. Keeping the two
// representations pinned by an explicit, compile-checked conversion layer is what keeps
// them from drifting: if a field is added to one side and not the other, the conversion
// (and its round-trip test) fails to compile or fails the test. The api.Event is the
// load-bearing type — it crosses both the Harness.Connect stream and Sessions.Exec/Replay,
// and it is what the event log hash-chains — so its round-trip must be lossless.
//
// Content constraint: structured content maps (ToolCall.Args, ToolResult.Output, DataPart)
// MUST hold JSON-shaped values. They cross the wire as a protobuf Struct, whose numbers are
// float64 — so a Go int round-trips hash-equal but not reflect-equal, and an int64 above 2^53
// is silently lossy. Producers must normalize to JSON-native types at ingest.
package wire

import (
	"encoding/json"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
)

// EventToProto converts a domain Event to its wire form. Ordering/integrity fields are not
// on Event (they live on LogRecord), so this is a pure body+envelope conversion.
func EventToProto(e api.Event) *v1.Event {
	out := &v1.Event{
		ExecutionId:   e.ExecutionID,
		SchemaVersion: e.SchemaVersion,
		Kind:          kindToProto(e.Kind),
		Actor:         identityToProto(e.Actor),
	}
	if !e.Timestamp.IsZero() {
		out.Ts = timestamppb.New(e.Timestamp)
	}
	switch {
	case e.Message != nil:
		out.Body = &v1.Event_Message{Message: msgToProto(e.Message)}
	case e.ModelCall != nil:
		out.Body = &v1.Event_Model{Model: modelCallToProto(e.ModelCall)}
	case e.ToolCall != nil:
		out.Body = &v1.Event_Tool{Tool: toolCallToProto(e.ToolCall)}
	case e.Result != nil:
		out.Body = &v1.Event_Result{Result: toolResultToProto(e.Result)}
	case e.Approval != nil:
		out.Body = &v1.Event_Approval{Approval: approvalReqToProto(e.Approval)}
	case e.ApprovalResult != nil:
		out.Body = &v1.Event_ApprovalResult{ApprovalResult: approvalResToProto(e.ApprovalResult)}
	case e.Usage != nil:
		out.Body = &v1.Event_Usage{Usage: usageToProto(e.Usage)}
	case e.Lifecycle != nil:
		out.Body = &v1.Event_Lifecycle{Lifecycle: lifecycleToProto(e.Lifecycle)}
	case e.End != nil:
		out.Body = &v1.Event_End{End: endToProto(e.End)}
	case e.Err != nil:
		out.Body = &v1.Event_Error{Error: errorToProto(e.Err)}
	}
	return out
}

// EventFromProto converts a wire Event back to the domain type.
func EventFromProto(p *v1.Event) api.Event {
	if p == nil {
		return api.Event{}
	}
	out := api.Event{
		ExecutionID:   p.GetExecutionId(),
		SchemaVersion: p.GetSchemaVersion(),
		Kind:          kindFromProto(p.GetKind()),
		Actor:         identityFromProto(p.GetActor()),
	}
	if p.GetTs() != nil {
		out.Timestamp = p.GetTs().AsTime()
	}
	switch b := p.GetBody().(type) {
	case *v1.Event_Message:
		out.Message = msgFromProto(b.Message)
	case *v1.Event_Model:
		out.ModelCall = modelCallFromProto(b.Model)
	case *v1.Event_Tool:
		out.ToolCall = toolCallFromProto(b.Tool)
	case *v1.Event_Result:
		out.Result = toolResultFromProto(b.Result)
	case *v1.Event_Approval:
		out.Approval = approvalReqFromProto(b.Approval)
	case *v1.Event_ApprovalResult:
		out.ApprovalResult = approvalResFromProto(b.ApprovalResult)
	case *v1.Event_Usage:
		out.Usage = usageFromProto(b.Usage)
	case *v1.Event_Lifecycle:
		out.Lifecycle = lifecycleFromProto(b.Lifecycle)
	case *v1.Event_End:
		out.End = endFromProto(b.End)
	case *v1.Event_Error:
		out.Err = errorFromProto(b.Error)
	}
	return out
}

// ---- content sub-messages ----

// MessageToProto converts a domain Message to its wire form (exported for the Sessions service).
func MessageToProto(m *api.Message) *v1.Message { return msgToProto(m) }

// MessageFromProto converts a wire Message to the domain type (exported for the Sessions service).
func MessageFromProto(p *v1.Message) *api.Message { return msgFromProto(p) }

// ToolCallToProto / ToolCallFromProto / ToolResultToProto / ToolResultFromProto are exported for the
// harness wire bridge, which mediates a CONTROLLER_MEDIATED tool call out of process exactly as it
// mediates a model call.
func ToolCallToProto(t *api.ToolCall) *v1.ToolCall         { return toolCallToProto(t) }
func ToolCallFromProto(t *v1.ToolCall) *api.ToolCall       { return toolCallFromProto(t) }
func ToolResultToProto(t *api.ToolResult) *v1.ToolResult   { return toolResultToProto(t) }
func ToolResultFromProto(t *v1.ToolResult) *api.ToolResult { return toolResultFromProto(t) }

func msgToProto(m *api.Message) *v1.Message {
	if m == nil {
		return nil
	}
	parts := make([]*v1.Part, len(m.Parts))
	for i := range m.Parts {
		parts[i] = partToProto(m.Parts[i])
	}
	return &v1.Message{Role: m.Role, Parts: parts}
}

func msgFromProto(p *v1.Message) *api.Message {
	if p == nil {
		return nil
	}
	parts := make([]api.Part, len(p.GetParts()))
	for i, pp := range p.GetParts() {
		parts[i] = partFromProto(pp)
	}
	return &api.Message{Role: p.GetRole(), Parts: parts}
}

func partToProto(p api.Part) *v1.Part {
	out := &v1.Part{}
	switch {
	case p.Text != nil:
		out.Part = &v1.Part_Text{Text: &v1.TextPart{Text: p.Text.Text}}
	case p.File != nil:
		out.Part = &v1.Part_File{File: fileToProto(p.File)}
	case p.Data != nil:
		out.Part = &v1.Part_Data{Data: &v1.DataPart{Data: toStruct(p.Data)}}
	case p.Reasoning != nil:
		out.Part = &v1.Part_Reasoning{Reasoning: reasoningToProto(p.Reasoning)}
	}
	return out
}

func partFromProto(p *v1.Part) api.Part {
	if p == nil {
		return api.Part{}
	}
	var out api.Part
	switch b := p.GetPart().(type) {
	case *v1.Part_Text:
		out.Text = &api.TextPart{Text: b.Text.GetText()}
	case *v1.Part_File:
		out.File = fileFromProto(b.File)
	case *v1.Part_Data:
		out.Data = fromStruct(b.Data.GetData())
	case *v1.Part_Reasoning:
		out.Reasoning = reasoningFromProto(b.Reasoning)
	}
	return out
}

func fileToProto(f *api.FilePart) *v1.FilePart {
	if f == nil {
		return nil
	}
	out := &v1.FilePart{Mime: f.MIME, Name: f.Name, Digest: f.Digest}
	switch {
	case len(f.Bytes) > 0:
		out.Source = &v1.FilePart_Bytes{Bytes: f.Bytes}
	case f.URI != "":
		out.Source = &v1.FilePart_Uri{Uri: f.URI}
	}
	return out
}

func fileFromProto(f *v1.FilePart) *api.FilePart {
	if f == nil {
		return nil
	}
	out := &api.FilePart{MIME: f.GetMime(), Name: f.GetName(), Digest: f.GetDigest()}
	switch s := f.GetSource().(type) {
	case *v1.FilePart_Bytes:
		out.Bytes = s.Bytes
	case *v1.FilePart_Uri:
		out.URI = s.Uri
	}
	return out
}

func reasoningToProto(r *api.ReasoningPart) *v1.ReasoningPart {
	if r == nil {
		return nil
	}
	out := &v1.ReasoningPart{
		Provider:      r.Provider,
		ModelId:       r.ModelID,
		ItemId:        r.ItemID,
		Ordinal:       r.Ordinal,
		ValidityScope: r.ValidityScope,
		OpaqueDigest:  r.OpaqueDigest,
	}
	switch {
	case len(r.Opaque) > 0:
		out.Opaque = &v1.ReasoningPart_OpaqueBytes{OpaqueBytes: r.Opaque}
	case r.OpaqueURI != "":
		out.Opaque = &v1.ReasoningPart_OpaqueUri{OpaqueUri: r.OpaqueURI}
	}
	if len(r.Summary) > 0 {
		out.Summary = make([]*v1.Part, len(r.Summary))
		for i := range r.Summary {
			out.Summary[i] = partToProto(r.Summary[i])
		}
	}
	return out
}

func reasoningFromProto(r *v1.ReasoningPart) *api.ReasoningPart {
	if r == nil {
		return nil
	}
	out := &api.ReasoningPart{
		Provider:      r.GetProvider(),
		ModelID:       r.GetModelId(),
		ItemID:        r.GetItemId(),
		Ordinal:       r.GetOrdinal(),
		ValidityScope: r.GetValidityScope(),
		OpaqueDigest:  r.GetOpaqueDigest(),
	}
	switch o := r.GetOpaque().(type) {
	case *v1.ReasoningPart_OpaqueBytes:
		out.Opaque = o.OpaqueBytes
	case *v1.ReasoningPart_OpaqueUri:
		out.OpaqueURI = o.OpaqueUri
	}
	if len(r.GetSummary()) > 0 {
		out.Summary = make([]api.Part, len(r.GetSummary()))
		for i, sp := range r.GetSummary() {
			out.Summary[i] = partFromProto(sp)
		}
	}
	return out
}

func modelCallToProto(m *api.ModelCall) *v1.ModelCall {
	if m == nil {
		return nil
	}
	return &v1.ModelCall{Model: m.Model, Params: m.Params, InputHash: m.InputHash, Id: m.ID}
}

func modelCallFromProto(m *v1.ModelCall) *api.ModelCall {
	if m == nil {
		return nil
	}
	return &api.ModelCall{Model: m.GetModel(), Params: m.GetParams(), InputHash: m.GetInputHash(), ID: m.GetId()}
}

func toolCallToProto(t *api.ToolCall) *v1.ToolCall {
	if t == nil {
		return nil
	}
	return &v1.ToolCall{
		Id:             t.ID,
		Tool:           t.Tool,
		Args:           toStruct(t.Args),
		Mediation:      mediationToProto(t.Mediation),
		IdempotencyKey: t.IdempotencyKey,
	}
}

func toolCallFromProto(t *v1.ToolCall) *api.ToolCall {
	if t == nil {
		return nil
	}
	return &api.ToolCall{
		ID:             t.GetId(),
		Tool:           t.GetTool(),
		Args:           fromStruct(t.GetArgs()),
		Mediation:      mediationFromProto(t.GetMediation()),
		IdempotencyKey: t.GetIdempotencyKey(),
	}
}

func toolResultToProto(t *api.ToolResult) *v1.ToolResult {
	if t == nil {
		return nil
	}
	return &v1.ToolResult{
		Id:           t.ID,
		Output:       toStruct(t.Output),
		OutputUri:    t.OutputURI,
		OutputDigest: t.OutputDigest,
		IsError:      t.IsError,
		Error:        t.Error,
	}
}

func toolResultFromProto(t *v1.ToolResult) *api.ToolResult {
	if t == nil {
		return nil
	}
	return &api.ToolResult{
		ID:           t.GetId(),
		Output:       fromStruct(t.GetOutput()),
		OutputURI:    t.GetOutputUri(),
		OutputDigest: t.GetOutputDigest(),
		IsError:      t.GetIsError(),
		Error:        t.GetError(),
	}
}

func approvalReqToProto(a *api.ApprovalRequest) *v1.ApprovalRequest {
	if a == nil {
		return nil
	}
	return &v1.ApprovalRequest{ToolCallId: a.ToolCallID, Reason: a.Reason}
}

func approvalReqFromProto(a *v1.ApprovalRequest) *api.ApprovalRequest {
	if a == nil {
		return nil
	}
	return &api.ApprovalRequest{ToolCallID: a.GetToolCallId(), Reason: a.GetReason()}
}

func approvalResToProto(a *api.ApprovalResult) *v1.ApprovalResult {
	if a == nil {
		return nil
	}
	return &v1.ApprovalResult{ToolCallId: a.ToolCallID, Approved: a.Approved, Reason: a.Reason}
}

func approvalResFromProto(a *v1.ApprovalResult) *api.ApprovalResult {
	if a == nil {
		return nil
	}
	return &api.ApprovalResult{ToolCallID: a.GetToolCallId(), Approved: a.GetApproved(), Reason: a.GetReason()}
}

func usageToProto(u *api.Usage) *v1.Usage {
	if u == nil {
		return nil
	}
	return &v1.Usage{
		Model:           u.Model,
		InputTokens:     u.InputTokens,
		OutputTokens:    u.OutputTokens,
		ReasoningTokens: u.ReasoningTokens,
	}
}

func usageFromProto(u *v1.Usage) *api.Usage {
	if u == nil {
		return nil
	}
	return &api.Usage{
		Model:           u.GetModel(),
		InputTokens:     u.GetInputTokens(),
		OutputTokens:    u.GetOutputTokens(),
		ReasoningTokens: u.GetReasoningTokens(),
	}
}

func lifecycleToProto(l *api.Lifecycle) *v1.Lifecycle {
	if l == nil {
		return nil
	}
	return &v1.Lifecycle{Kind: lifecycleKindToProto(l.Kind), Detail: l.Detail}
}

func lifecycleFromProto(l *v1.Lifecycle) *api.Lifecycle {
	if l == nil {
		return nil
	}
	return &api.Lifecycle{Kind: lifecycleKindFromProto(l.GetKind()), Detail: l.GetDetail()}
}

func endToProto(h *api.HarnessEnd) *v1.HarnessEnd {
	if h == nil {
		return nil
	}
	return &v1.HarnessEnd{State: h.State, Error: errorToProto(h.Error)}
}

func endFromProto(h *v1.HarnessEnd) *api.HarnessEnd {
	if h == nil {
		return nil
	}
	return &api.HarnessEnd{State: h.GetState(), Error: errorFromProto(h.GetError())}
}

func errorToProto(e *api.Error) *v1.Error {
	if e == nil {
		return nil
	}
	return &v1.Error{Code: e.Code, Description: e.Description}
}

func errorFromProto(e *v1.Error) *api.Error {
	if e == nil {
		return nil
	}
	return &api.Error{Code: e.GetCode(), Description: e.GetDescription()}
}

func identityToProto(id api.IdentityRef) *v1.IdentityRef {
	if id == (api.IdentityRef{}) {
		return nil
	}
	return &v1.IdentityRef{Principal: id.Principal, Issuer: id.Issuer, Subject: id.Subject}
}

func identityFromProto(p *v1.IdentityRef) api.IdentityRef {
	if p == nil {
		return api.IdentityRef{}
	}
	return api.IdentityRef{Principal: p.GetPrincipal(), Issuer: p.GetIssuer(), Subject: p.GetSubject()}
}

// ---- enums ----

func kindToProto(k api.EventKind) v1.EventKind {
	switch k {
	case api.EventInput:
		return v1.EventKind_EVENT_INPUT
	case api.EventModelCall:
		return v1.EventKind_EVENT_MODEL_CALL
	case api.EventOutput:
		return v1.EventKind_EVENT_OUTPUT
	case api.EventToolCall:
		return v1.EventKind_EVENT_TOOL_CALL
	case api.EventToolResult:
		return v1.EventKind_EVENT_TOOL_RESULT
	case api.EventApprovalRequest:
		return v1.EventKind_EVENT_APPROVAL_REQUEST
	case api.EventApprovalResult:
		return v1.EventKind_EVENT_APPROVAL_RESULT
	case api.EventUsage:
		return v1.EventKind_EVENT_USAGE
	case api.EventLifecycle:
		return v1.EventKind_EVENT_LIFECYCLE
	case api.EventEnd:
		return v1.EventKind_EVENT_END
	case api.EventError:
		return v1.EventKind_EVENT_ERROR
	default:
		return v1.EventKind_EVENT_KIND_UNSPECIFIED
	}
}

func kindFromProto(k v1.EventKind) api.EventKind {
	switch k {
	case v1.EventKind_EVENT_INPUT:
		return api.EventInput
	case v1.EventKind_EVENT_MODEL_CALL:
		return api.EventModelCall
	case v1.EventKind_EVENT_OUTPUT:
		return api.EventOutput
	case v1.EventKind_EVENT_TOOL_CALL:
		return api.EventToolCall
	case v1.EventKind_EVENT_TOOL_RESULT:
		return api.EventToolResult
	case v1.EventKind_EVENT_APPROVAL_REQUEST:
		return api.EventApprovalRequest
	case v1.EventKind_EVENT_APPROVAL_RESULT:
		return api.EventApprovalResult
	case v1.EventKind_EVENT_USAGE:
		return api.EventUsage
	case v1.EventKind_EVENT_LIFECYCLE:
		return api.EventLifecycle
	case v1.EventKind_EVENT_END:
		return api.EventEnd
	case v1.EventKind_EVENT_ERROR:
		return api.EventError
	default:
		return ""
	}
}

func mediationToProto(m api.Mediation) v1.Mediation {
	switch m {
	case api.MediationInHarnessReported:
		return v1.Mediation_MEDIATION_IN_HARNESS_REPORTED
	case api.MediationControllerMediated:
		return v1.Mediation_MEDIATION_CONTROLLER_MEDIATED
	case api.MediationRequiresApproval:
		return v1.Mediation_MEDIATION_REQUIRES_APPROVAL
	default:
		return v1.Mediation_MEDIATION_UNSPECIFIED
	}
}

func mediationFromProto(m v1.Mediation) api.Mediation {
	switch m {
	case v1.Mediation_MEDIATION_IN_HARNESS_REPORTED:
		return api.MediationInHarnessReported
	case v1.Mediation_MEDIATION_CONTROLLER_MEDIATED:
		return api.MediationControllerMediated
	case v1.Mediation_MEDIATION_REQUIRES_APPROVAL:
		return api.MediationRequiresApproval
	default:
		return ""
	}
}

func lifecycleKindToProto(k api.LifecycleKind) v1.Lifecycle_Kind {
	switch k {
	case api.LifecycleSuspend:
		return v1.Lifecycle_LIFECYCLE_SUSPEND
	case api.LifecycleResume:
		return v1.Lifecycle_LIFECYCLE_RESUME
	case api.LifecycleFork:
		return v1.Lifecycle_LIFECYCLE_FORK
	case api.LifecycleBaseline:
		return v1.Lifecycle_LIFECYCLE_BASELINE
	case api.LifecycleCancel:
		return v1.Lifecycle_LIFECYCLE_CANCEL
	default:
		return v1.Lifecycle_LIFECYCLE_UNSPECIFIED
	}
}

func lifecycleKindFromProto(k v1.Lifecycle_Kind) api.LifecycleKind {
	switch k {
	case v1.Lifecycle_LIFECYCLE_SUSPEND:
		return api.LifecycleSuspend
	case v1.Lifecycle_LIFECYCLE_RESUME:
		return api.LifecycleResume
	case v1.Lifecycle_LIFECYCLE_FORK:
		return api.LifecycleFork
	case v1.Lifecycle_LIFECYCLE_BASELINE:
		return api.LifecycleBaseline
	case v1.Lifecycle_LIFECYCLE_CANCEL:
		return api.LifecycleCancel
	default:
		return ""
	}
}

// ---- structpb helpers ----

// toStruct converts a JSON-shaped map to a protobuf Struct. structpb.NewStruct accepts
// structpb-native values directly; other JSON-shaped values (int, int64, json.Number) are
// normalized through a JSON round-trip so numbers become float64. Content maps MUST be
// JSON-shaped (see package doc) — a Marshal failure here means that contract was violated
// upstream, in which case the value is dropped rather than propagated as corrupt.
func toStruct(m map[string]any) *structpb.Struct {
	if m == nil {
		return nil
	}
	if s, err := structpb.NewStruct(m); err == nil {
		return s
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil // not JSON-serializable: violates the JSON-shaped content constraint
	}
	var nm map[string]any
	if err := json.Unmarshal(b, &nm); err != nil {
		return nil
	}
	s, _ := structpb.NewStruct(nm) // cannot fail: nm is the product of a JSON round-trip
	return s
}

// fromStruct converts a protobuf Struct back to a map. Numbers come back as float64 (the
// JSON number type), which is why the round-trip is stable only for JSON-shaped values.
func fromStruct(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}
