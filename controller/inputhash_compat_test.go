package controller

import (
	"testing"

	"github.com/aramase/agentsessions/api"
)

// legacyInputHash is hashModelInput(legacyRequest()) computed at origin/main 4c1cead, before
// ModelRequest and Part gained tool fields. Logs recorded by that host carry hashes computed the same
// way on EVENT_MODEL_CALL, so a request that uses no tool field must keep hashing to this value or
// every earlier log fails the I0 check on replay.
const legacyInputHash = "210636379d42235ed6d7b2b1e9c7d7e59c8f887a851ec502ac50f82e36867256"

// legacyRequest uses every Part kind that existed before tool parts, including a reasoning summary,
// which nests Parts inside a Part.
func legacyRequest() api.ModelRequest {
	return api.ModelRequest{
		Model:  "m",
		Params: map[string]string{"temperature": "0"},
		Messages: []api.Message{
			{Role: "system", Parts: []api.Part{{Text: &api.TextPart{Text: "be brief"}}}},
			{Role: "user", Parts: []api.Part{
				{Text: &api.TextPart{Text: "hi"}},
				{File: &api.FilePart{MIME: "image/png", Bytes: []byte{1, 2}, Name: "img", Digest: "sha256:aa"}},
				{Data: map[string]any{"b": 1.5, "a": "x"}},
			}},
			{Role: "assistant", Parts: []api.Part{
				{Reasoning: &api.ReasoningPart{Provider: "p", ModelID: "m", Opaque: []byte("r"), ItemID: "rs_1", Ordinal: 1,
					Summary: []api.Part{{Text: &api.TextPart{Text: "brief"}}}}},
				{Text: &api.TextPart{Text: "yo"}},
			}},
		},
	}
}

func TestInputHashStableForRequestsWithoutTools(t *testing.T) {
	if got := hashModelInput(legacyRequest()); got != legacyInputHash {
		t.Fatalf("hashModelInput changed for a request that uses no tool field: got %s, want %s", got, legacyInputHash)
	}
}

// TestInputHashCoversTools pins that I0 covers what the model is offered and what it is fed back: a
// harness that changes a tool definition, the tool choice, or a tool part on replay fails the check,
// while JSON-equal schemas built in a different key order or with an int instead of a float64 of the
// same value (what a Struct round-trip produces) do not.
func TestInputHashCoversTools(t *testing.T) {
	base := func() api.ModelRequest {
		r := legacyRequest()
		r.Tools = []api.ToolDefinition{{Name: "get_weather", Description: "d", InputSchema: map[string]any{"type": "object", "maxProperties": 2.0}}}
		r.ToolChoice = &api.ToolChoice{Mode: api.ToolChoiceAuto}
		r.Messages = append(r.Messages,
			api.Message{Role: "assistant", Parts: []api.Part{{ToolCall: &api.ToolCall{ID: "c1", Tool: "get_weather", Args: map[string]any{"city": "Paris", "days": 2.0}}}}},
			api.Message{Role: "tool", Parts: []api.Part{{ToolResult: &api.ToolResult{ID: "c1", Content: []api.Part{{Text: &api.TextPart{Text: "sunny"}}}}}}},
		)
		return r
	}
	h := hashModelInput(base())
	if h == legacyInputHash {
		t.Fatal("tool fields did not change the input hash")
	}

	same := base()
	same.Tools[0].InputSchema = map[string]any{"maxProperties": 2, "type": "object"}
	same.Messages[3].Parts[0].ToolCall.Args = map[string]any{"days": 2, "city": "Paris"}
	if got := hashModelInput(same); got != h {
		t.Fatalf("JSON-equal tool fields hashed differently: %s != %s", got, h)
	}

	for name, mutate := range map[string]func(*api.ModelRequest){
		"tool name":        func(r *api.ModelRequest) { r.Tools[0].Name = "other" },
		"tool description": func(r *api.ModelRequest) { r.Tools[0].Description = "other" },
		"tool schema":      func(r *api.ModelRequest) { r.Tools[0].InputSchema["type"] = "array" },
		"tool order":       func(r *api.ModelRequest) { r.Tools = append([]api.ToolDefinition{{Name: "a"}}, r.Tools...) },
		"no tools":         func(r *api.ModelRequest) { r.Tools = nil },
		"choice mode":      func(r *api.ModelRequest) { r.ToolChoice.Mode = api.ToolChoiceRequired },
		"choice name":      func(r *api.ModelRequest) { r.ToolChoice.Name = "get_weather" },
		"no choice":        func(r *api.ModelRequest) { r.ToolChoice = nil },
		"call id":          func(r *api.ModelRequest) { r.Messages[3].Parts[0].ToolCall.ID = "c2" },
		"call tool":        func(r *api.ModelRequest) { r.Messages[3].Parts[0].ToolCall.Tool = "other" },
		"call args":        func(r *api.ModelRequest) { r.Messages[3].Parts[0].ToolCall.Args["city"] = "Rome" },
		"result id":        func(r *api.ModelRequest) { r.Messages[4].Parts[0].ToolResult.ID = "c2" },
		"result content": func(r *api.ModelRequest) {
			r.Messages[4].Parts[0].ToolResult.Content = []api.Part{{Text: &api.TextPart{Text: "rain"}}}
		},
		"result error": func(r *api.ModelRequest) { r.Messages[4].Parts[0].ToolResult.IsError = true },
	} {
		r := base()
		mutate(&r)
		if hashModelInput(r) == h {
			t.Errorf("%s: change not covered by the input hash", name)
		}
	}
}
