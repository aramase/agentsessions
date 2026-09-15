package openai_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/model/openai"
)

// newClient points a client at a stub endpoint. Every test here runs against httptest, so the
// suite never needs network access or a key.
func newClient(t *testing.T, h http.HandlerFunc, opts ...openai.Option) *openai.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	base := []openai.Option{openai.WithBaseURL(srv.URL), openai.WithModel("test-model")}
	c, err := openai.New(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func completion(text string) string {
	return `{"choices":[{"message":{"content":"` + text + `"}}],
	         "usage":{"prompt_tokens":7,"completion_tokens":11}}`
}

// The request must carry the model, the messages, and the bearer token, and must land on the
// /chat/completions path relative to the configured base URL.
func TestModelSendsChatCompletionRequest(t *testing.T) {
	var gotPath, gotAuth, gotBody string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = io.WriteString(w, completion("hi there"))
	}, openai.WithAPIKey("sk-test"))

	resp, err := c.Model(t.Context(), api.ModelRequest{
		Model:    "gpt-test",
		Messages: []api.Message{*api.TextMessage("user", "hello")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if gotPath != "/chat/completions" {
		t.Fatalf("path = %q, want /chat/completions", gotPath)
	}
	if gotAuth != "Bearer sk-test" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	for _, want := range []string{`"model":"gpt-test"`, `"role":"user"`, `"content":"hello"`} {
		if !strings.Contains(gotBody, want) {
			t.Fatalf("request body %s missing %s", gotBody, want)
		}
	}
	if got := resp.Message.Text(); got != "hi there" {
		t.Fatalf("text = %q, want %q", got, "hi there")
	}
	if resp.Message.Role != "assistant" {
		t.Fatalf("role = %q, want assistant", resp.Message.Role)
	}
}

// Usage is what makes a session accountable, so it must come from the response rather than be
// inferred.
func TestModelRecordsUsage(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"x"}}],
		  "usage":{"prompt_tokens":3,"completion_tokens":5,"completion_tokens_details":{"reasoning_tokens":2}}}`)
	})
	resp, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err != nil {
		t.Fatal(err)
	}
	want := api.Usage{Model: "test-model", InputTokens: 3, OutputTokens: 5, ReasoningTokens: 2}
	if resp.Usage != want {
		t.Fatalf("usage = %+v, want %+v", resp.Usage, want)
	}
}

// An empty model on the request falls back to the client's default, and so does the reference
// harness's "echo" placeholder, so the sample harnesses work against a real endpoint unchanged.
func TestModelFallsBackToDefaultModel(t *testing.T) {
	for _, requested := range []string{"", "echo"} {
		var body string
		c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			body = string(b)
			_, _ = io.WriteString(w, completion("ok"))
		})
		if _, err := c.Model(t.Context(), api.ModelRequest{
			Model:    requested,
			Messages: []api.Message{*api.TextMessage("user", "q")},
		}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(body, `"model":"test-model"`) {
			t.Fatalf("model %q did not fall back to the default: %s", requested, body)
		}
	}
}

// Reasoning is recorded verbatim for replay continuity (I2), so it must survive as an opaque part
// when the endpoint returns it.
func TestModelPreservesReasoning(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"answer","reasoning_content":"because"}}],"usage":{}}`)
	})
	resp, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err != nil {
		t.Fatal(err)
	}
	var reasoning *api.ReasoningPart
	for _, p := range resp.Message.Parts {
		if p.Reasoning != nil {
			reasoning = p.Reasoning
		}
	}
	if reasoning == nil {
		t.Fatal("no reasoning part produced")
	}
	if string(reasoning.Opaque) != "because" {
		t.Fatalf("opaque = %q, want %q", reasoning.Opaque, "because")
	}
	if resp.Message.Text() != "answer" {
		t.Fatalf("text = %q", resp.Message.Text())
	}
}

// A response with no reasoning must not gain one. A synthesized part would be recorded and
// replayed as though the provider had produced it.
func TestModelDoesNotInventReasoning(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, completion("plain"))
	})
	resp, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range resp.Message.Parts {
		if p.Reasoning != nil {
			t.Fatal("synthesized a reasoning part the endpoint never returned")
		}
	}
}

// Content this provider cannot send must fail loudly. Dropping it would send less than the harness
// asked for while the journal records the full request, so replay would reproduce the wrong call.
func TestModelRejectsUnsupportedParts(t *testing.T) {
	for name, part := range map[string]api.Part{
		"file": {File: &api.FilePart{MIME: "image/png", Bytes: []byte{1}}},
		"data": {Data: map[string]any{"k": "v"}},
	} {
		t.Run(name, func(t *testing.T) {
			var called atomic.Bool
			c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				called.Store(true)
				_, _ = io.WriteString(w, completion("x"))
			})
			_, err := c.Model(t.Context(), api.ModelRequest{
				Messages: []api.Message{{Role: "user", Parts: []api.Part{part}}},
			})
			if !errors.Is(err, openai.ErrUnsupportedPart) {
				t.Fatalf("err = %v, want ErrUnsupportedPart", err)
			}
			if called.Load() {
				t.Fatal("sent a request despite unsupported content")
			}
		})
	}
}

// A provider error message is far more actionable than its status code, since a bad key and a bad
// model id are both 4xx.
func TestModelSurfacesProviderError(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error"}}`)
	})
	_, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "Incorrect API key provided") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("error %q should carry the provider message and status", err)
	}
}

// A non-JSON error body still has to produce something diagnosable: gateways and proxies return
// HTML or plain text on failure.
func TestModelSurfacesNonJSONError(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "upstream unavailable")
	})
	_, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("err = %v, want one naming http 502", err)
	}
}

// A 200 with no choices is not a completion; treating it as an empty one would journal a turn that
// never produced output.
func TestModelRejectsEmptyChoices(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[],"usage":{}}`)
	})
	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err == nil {
		t.Fatal("want an error for a response with no choices")
	}
}

// Cancelling the turn must abandon the in-flight completion rather than let it run unobserved.
// This is the ctx threading from the harness SPI reaching all the way to the provider.
func TestModelHonoursContextCancellation(t *testing.T) {
	release := make(chan struct{})
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
		_, _ = io.WriteString(w, completion("late"))
	})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Model(ctx, api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// The model id lands on every recorded MODEL_CALL, so a client that cannot name one would write a
// journal that does not say what produced the completion.
func TestNewRequiresModel(t *testing.T) {
	if _, err := openai.New(openai.WithBaseURL("http://example.invalid")); err == nil {
		t.Fatal("New accepted a client with no default model")
	}
}

// A local or gateway-fronted endpoint usually wants no auth, so an empty key must send no header
// rather than an empty bearer.
func TestModelOmitsAuthorizationWithoutKey(t *testing.T) {
	var hadAuth bool
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		_, _ = io.WriteString(w, completion("x"))
	})
	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	if hadAuth {
		t.Fatal("sent an Authorization header with no API key configured")
	}
}

// Multi-part text is concatenated rather than truncated to the first part.
func TestModelConcatenatesTextParts(t *testing.T) {
	var body string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		_, _ = io.WriteString(w, completion("x"))
	})
	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{{
		Role:  "user",
		Parts: []api.Part{{Text: &api.TextPart{Text: "one "}}, {Text: &api.TextPart{Text: "two"}}},
	}}}); err != nil {
		t.Fatal(err)
	}
	var sent struct {
		Messages []struct{ Content string } `json:"messages"`
	}
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatal(err)
	}
	if got := sent.Messages[0].Content; got != "one two" {
		t.Fatalf("content = %q, want %q", got, "one two")
	}
}

// realOpenAIResponse is the documented Chat Completions response body, with every field this
// client does not read left in place: id, object, created, system_fingerprint, index, logprobs,
// finish_reason, refusal, annotations, total_tokens, and the prediction-token details.
//
// The hand-written stubs above send only the fields the parser wants, which cannot catch a parser
// that breaks on the surrounding envelope. This one is shaped like what a provider actually
// returns.
const realOpenAIResponse = `{
  "id": "chatcmpl-B9MHDbslfkBeAs8l4bebGdFOJ6PeG",
  "object": "chat.completion",
  "created": 1741570283,
  "model": "gpt-4o-mini-2024-07-18",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "2 + 2 equals 4.",
        "refusal": null,
        "annotations": []
      },
      "logprobs": null,
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 14,
    "completion_tokens": 8,
    "total_tokens": 22,
    "prompt_tokens_details": {"cached_tokens": 0, "audio_tokens": 0},
    "completion_tokens_details": {
      "reasoning_tokens": 0,
      "audio_tokens": 0,
      "accepted_prediction_tokens": 0,
      "rejected_prediction_tokens": 0
    }
  },
  "service_tier": "default",
  "system_fingerprint": "fp_fc9f1d7035"
}`

// Parsing must survive the full provider envelope, not just the trimmed stubs the other tests use.
func TestModelParsesRealProviderResponseShape(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, realOpenAIResponse)
	})
	resp, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "what is 2+2?")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Message.Text(); got != "2 + 2 equals 4." {
		t.Fatalf("text = %q", got)
	}
	if resp.Usage.InputTokens != 14 || resp.Usage.OutputTokens != 8 {
		t.Fatalf("usage = %+v, want 14 in / 8 out", resp.Usage)
	}
	for _, p := range resp.Message.Parts {
		if p.Reasoning != nil {
			t.Fatal("a non-reasoning response produced a reasoning part")
		}
	}
}

// A reasoning-model response carries reasoning_tokens in the usage details even when the provider
// does not return the reasoning text itself, so accounting must still pick it up.
func TestModelParsesReasoningTokensWithoutReasoningText(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{
		  "id":"chatcmpl-x","object":"chat.completion","model":"o4-mini",
		  "choices":[{"index":0,"message":{"role":"assistant","content":"4"},"finish_reason":"stop"}],
		  "usage":{"prompt_tokens":12,"completion_tokens":410,"total_tokens":422,
		    "completion_tokens_details":{"reasoning_tokens":384}}}`)
	})
	resp, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.ReasoningTokens != 384 {
		t.Fatalf("reasoning tokens = %d, want 384", resp.Usage.ReasoningTokens)
	}
	if resp.Message.Text() != "4" {
		t.Fatalf("text = %q", resp.Message.Text())
	}
}

// An endpoint that scopes the model into its path and pins an API version with a query string must
// be reachable by configuration. This is the shape several hosted deployments use, and it differs
// from the default only in the envelope: the request body is identical.
func TestModelHonoursCustomPath(t *testing.T) {
	var gotPath, gotQuery string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		_, _ = io.WriteString(w, completion("ok"))
	}, openai.WithPath("/openai/deployments/gpt-4o/chat/completions?api-version=2024-10-21"))

	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/openai/deployments/gpt-4o/chat/completions" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotQuery != "api-version=2024-10-21" {
		t.Fatalf("query = %q, want the pinned api-version", gotQuery)
	}
}

// A path given without a leading slash still has to produce a valid URL rather than silently
// concatenating into the host segment.
func TestWithPathNormalizesLeadingSlash(t *testing.T) {
	var gotPath string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = io.WriteString(w, completion("ok"))
	}, openai.WithPath("v1/chat/completions"))

	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("path = %q, want a normalized leading slash", gotPath)
	}
}

// An endpoint that authenticates outside the bearer convention must be reachable without a
// separate provider implementation, since it differs only in the header name.
func TestModelHonoursCustomAuthHeader(t *testing.T) {
	var gotAPIKey string
	var hadAuthorization bool
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("api-key")
		_, hadAuthorization = r.Header["Authorization"]
		_, _ = io.WriteString(w, completion("ok"))
	}, openai.WithHeader("api-key", "secret-value"))

	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	if gotAPIKey != "secret-value" {
		t.Fatalf("api-key = %q", gotAPIKey)
	}
	if hadAuthorization {
		t.Fatal("sent an Authorization header when the endpoint authenticates with api-key")
	}
}

// Gateways commonly need routing or tenant headers alongside the credential, so headers must
// accumulate rather than replace one another.
func TestWithHeaderAccumulates(t *testing.T) {
	got := map[string]string{}
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"Authorization", "X-Team-Id", "X-Trace"} {
			got[name] = r.Header.Get(name)
		}
		_, _ = io.WriteString(w, completion("ok"))
	},
		openai.WithAPIKey("sk-test"),
		openai.WithHeader("X-Team-Id", "platform"),
		openai.WithHeader("X-Trace", "on"),
	)

	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"Authorization": "Bearer sk-test", "X-Team-Id": "platform", "X-Trace": "on"}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("header %s = %q, want %q", name, got[name], value)
		}
	}
}

// Later options win, so a caller can override a credential set by an earlier layer of config
// rather than sending two conflicting values.
func TestWithHeaderLastWins(t *testing.T) {
	var got string
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, completion("ok"))
	}, openai.WithAPIKey("first"), openai.WithHeader("Authorization", "Bearer second"))

	if _, err := c.Model(t.Context(), api.ModelRequest{Messages: []api.Message{*api.TextMessage("user", "q")}}); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer second" {
		t.Fatalf("authorization = %q, want the later value", got)
	}
}

// sseCompletion renders a streaming response: content chunks, then a usage-only frame, then the
// terminator. That is the shape the real endpoint sends when stream_options.include_usage is set.
func sseCompletion(chunks ...string) string {
	var b strings.Builder
	for _, c := range chunks {
		b.WriteString(`data: {"choices":[{"delta":{"content":"` + c + `"}}]}` + "\n\n")
	}
	b.WriteString(`data: {"choices":[],"usage":{"prompt_tokens":6,"completion_tokens":4}}` + "\n\n")
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

// Streaming must report each chunk as it arrives AND return the complete message, because the host
// records the latter. If they disagreed, a watched turn and a replayed one would differ.
func TestStreamModelReportsChunksAndFullMessage(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"stream":true`) {
			t.Errorf("streaming request did not set stream: %s", body)
		}
		if !strings.Contains(string(body), `"include_usage":true`) {
			t.Errorf("streaming request did not ask for usage: %s", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseCompletion("Hello", ", ", "world"))
	})

	var chunks []string
	resp, err := c.StreamModel(t.Context(), api.ModelRequest{
		Messages: []api.Message{*api.TextMessage("user", "hi")},
	}, func(s string) { chunks = append(chunks, s) })
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks = %v, want 3", chunks)
	}
	if got := resp.Message.Text(); got != "Hello, world" {
		t.Fatalf("assembled text = %q, want %q", got, "Hello, world")
	}
	if strings.Join(chunks, "") != resp.Message.Text() {
		t.Fatalf("chunks %q do not assemble to the returned message %q", strings.Join(chunks, ""), resp.Message.Text())
	}
}

// Usage arrives on a trailing frame with no choices. Without stream_options a streamed turn would
// record zero tokens, so this is what keeps accounting honest.
func TestStreamModelCapturesTrailingUsage(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, sseCompletion("x"))
	})
	resp, err := c.StreamModel(t.Context(), api.ModelRequest{
		Messages: []api.Message{*api.TextMessage("user", "hi")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.InputTokens != 6 || resp.Usage.OutputTokens != 4 {
		t.Fatalf("usage = %+v, want 6 in / 4 out", resp.Usage)
	}
}

// A streaming error must surface as the provider's message rather than as an empty completion.
func TestStreamModelSurfacesProviderError(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"Rate limit reached"}}`)
	})
	_, err := c.StreamModel(t.Context(), api.ModelRequest{
		Messages: []api.Message{*api.TextMessage("user", "hi")},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "Rate limit reached") {
		t.Fatalf("err = %v, want the provider message", err)
	}
}

// Reasoning chunks accumulate into an opaque part rather than being emitted as visible output.
func TestStreamModelAccumulatesReasoningSeparately(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w,
			`data: {"choices":[{"delta":{"reasoning_content":"think"}}]}`+"\n\n"+
				`data: {"choices":[{"delta":{"content":"answer"}}]}`+"\n\n"+
				"data: [DONE]\n\n")
	})
	var chunks []string
	resp, err := c.StreamModel(t.Context(), api.ModelRequest{
		Messages: []api.Message{*api.TextMessage("user", "hi")},
	}, func(s string) { chunks = append(chunks, s) })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(chunks, "") != "answer" {
		t.Fatalf("streamed %q, want only the visible output", strings.Join(chunks, ""))
	}
	var reasoning *api.ReasoningPart
	for _, p := range resp.Message.Parts {
		if p.Reasoning != nil {
			reasoning = p.Reasoning
		}
	}
	if reasoning == nil || string(reasoning.Opaque) != "think" {
		t.Fatalf("reasoning part = %v, want the accumulated reasoning", reasoning)
	}
}
