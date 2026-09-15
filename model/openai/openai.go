// Package openai is a model provider for OpenAI-compatible Chat Completions endpoints.
//
// It speaks the HTTP wire protocol directly and depends on no vendor SDK, which is what keeps it
// in the core module: the protocol is an interoperable format that many endpoints implement, not a
// vendor. Point it at a different base URL and it drives a different service, including a
// self-hosted one or a gateway that fronts a provider with a non-compatible native API.
//
// Endpoints vary in two ways that are not about the protocol at all: where the completions path
// sits, and how the request authenticates. WithPath and WithHeader cover both, so an endpoint that
// scopes the model into its path, pins an API version with a query string, or authenticates with
// something other than a bearer token is reached by configuration rather than by code.
//
// The limit is the request body. An endpoint that accepts the chat-completions body works here; one
// with its own body or signing scheme (a native messages API, or a cloud that signs requests) is a
// different protocol, and the seam for those is controller.ModelFunc, which this type satisfies.
//
// Client.Model satisfies controller.ModelFunc, so it plugs straight into placement.New.
//
// It is used only on the LIVE path. Replay serves recorded completions from the journal and never
// reaches a provider, which is what makes a replayed session free and byte-identical; see
// controller.Controller.Replay.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aramase/agentsessions/api"
)

// DefaultBaseURL is the public OpenAI endpoint. Any compatible service works by overriding it.
const DefaultBaseURL = "https://api.openai.com/v1"

// DefaultPath is where the completions endpoint sits under the base URL. Endpoints that scope the
// model into the path, or that pin an API version with a query string, override it.
const DefaultPath = "/chat/completions"

// defaultTimeout bounds a request that the caller did not bound itself. A model call that hangs
// forever holds the turn's single writer slot open, so an unbounded default would turn one wedged
// request into a session that can never be advanced.
const defaultTimeout = 2 * time.Minute

// ErrUnsupportedPart reports content this provider cannot represent. It is returned rather than
// dropped: the log records the request the harness built, so silently sending less than that would
// make the journal describe a call that never happened, and replay would reproduce the wrong thing.
var ErrUnsupportedPart = errors.New("openai: unsupported content part")

// Client calls an OpenAI-compatible Chat Completions endpoint.
type Client struct {
	baseURL string
	path    string
	headers map[string]string
	model   string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL points the client at a compatible endpoint other than the default.
func WithBaseURL(u string) Option {
	return func(c *Client) {
		if u != "" {
			c.baseURL = strings.TrimSuffix(u, "/")
		}
	}
}

// WithAPIKey sets a bearer token. It is sugar for the Authorization header most compatible
// endpoints expect; an endpoint that authenticates differently uses WithHeader instead. Empty
// sends no header at all, which is what a local endpoint or a gateway that injects its own
// credential wants.
func WithAPIKey(k string) Option {
	return func(c *Client) {
		if k != "" {
			c.headers["Authorization"] = "Bearer " + k
		}
	}
}

// WithHeader sets a request header, replacing any previous value for the same name. It is how an
// endpoint that authenticates outside the bearer convention is reached (an api-key or x-api-key
// header), and how a gateway that needs routing or tenant headers is satisfied.
//
// This exists because those endpoints differ from OpenAI only in the envelope: the request body
// they accept is identical, so requiring a separate provider implementation for them would be a
// recompile in exchange for nothing.
func WithHeader(name, value string) Option {
	return func(c *Client) {
		if name != "" {
			c.headers[name] = value
		}
	}
}

// WithPath overrides the path appended to the base URL. It may carry a query string, which is what
// an endpoint that pins an API version needs.
func WithPath(p string) Option {
	return func(c *Client) {
		if p != "" {
			if !strings.HasPrefix(p, "/") {
				p = "/" + p
			}
			c.path = p
		}
	}
}

// WithModel sets the model used when a request does not name one.
func WithModel(m string) Option {
	return func(c *Client) {
		if m != "" {
			c.model = m
		}
	}
}

// WithHTTPClient supplies the HTTP client, for custom transports, proxies, or tests.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.http = h
		}
	}
}

// New builds a Client. A default model is required: the model id ends up on every recorded
// MODEL_CALL event, so a client that cannot name one would write a journal that does not say what
// produced the completion.
func New(opts ...Option) (*Client, error) {
	c := &Client{
		baseURL: DefaultBaseURL,
		path:    DefaultPath,
		headers: map[string]string{},
		http:    &http.Client{Timeout: defaultTimeout},
	}
	for _, o := range opts {
		o(c)
	}
	if c.model == "" {
		return nil, errors.New("openai: a default model is required (WithModel)")
	}
	return c, nil
}

// Model performs one live completion. It satisfies controller.ModelFunc.
//
// ctx carries the turn's cancellation and deadline through to the HTTP request, so a cancelled
// execution abandons an in-flight completion rather than leaving it to finish unobserved.
func (c *Client) Model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	model := req.Model
	if model == "" || model == "echo" {
		// "echo" is the reference harness's placeholder id. Treating it as "unset" lets the sample
		// harnesses run against a real endpoint without every one of them hard-coding a model.
		model = c.model
	}
	msgs, err := toChatMessages(req.Messages)
	if err != nil {
		return api.ModelResponse{}, err
	}
	body, err := json.Marshal(chatRequest{Model: model, Messages: msgs})
	if err != nil {
		return api.ModelResponse{}, fmt.Errorf("openai: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+c.path, bytes.NewReader(body))
	if err != nil {
		return api.ModelResponse{}, fmt.Errorf("openai: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for name, value := range c.headers {
		httpReq.Header.Set(name, value)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return api.ModelResponse{}, fmt.Errorf("openai: %s: %w", model, err)
	}
	defer resp.Body.Close()

	// Bound the read: a compatible-looking endpoint that streams unbounded bytes would otherwise
	// grow the process until it dies, and a completion is small.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return api.ModelResponse{}, fmt.Errorf("openai: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return api.ModelResponse{}, statusError(model, resp.StatusCode, raw)
	}

	var out chatResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return api.ModelResponse{}, fmt.Errorf("openai: decode response: %w", err)
	}
	if len(out.Choices) == 0 {
		return api.ModelResponse{}, fmt.Errorf("openai: %s returned no choices", model)
	}
	return toModelResponse(model, out), nil
}

// statusError turns a non-200 into an error carrying the provider's own message, which is usually
// far more actionable than the status code alone (a wrong model id and a missing key are both 4xx).
func statusError(model string, code int, raw []byte) error {
	var e errorResponse
	if err := json.Unmarshal(raw, &e); err == nil && e.Error.Message != "" {
		return fmt.Errorf("openai: %s: %s (http %d)", model, e.Error.Message, code)
	}
	detail := strings.TrimSpace(string(raw))
	if len(detail) > 200 {
		detail = detail[:200] + "..."
	}
	if detail == "" {
		return fmt.Errorf("openai: %s: http %d", model, code)
	}
	return fmt.Errorf("openai: %s: http %d: %s", model, code, detail)
}

// toChatMessages converts the neutral content model to chat messages.
//
// Reasoning parts are dropped on the way OUT by design: they are opaque provider state recorded for
// replay continuity (I2), not input this endpoint accepts. File and data parts are refused instead,
// because dropping them would change what the model saw while the journal still claims otherwise.
func toChatMessages(msgs []api.Message) ([]chatMessage, error) {
	out := make([]chatMessage, 0, len(msgs))
	for i := range msgs {
		m := &msgs[i]
		var text strings.Builder
		for _, p := range m.Parts {
			switch {
			case p.Text != nil:
				text.WriteString(p.Text.Text)
			case p.Reasoning != nil:
				// Carried in the journal, not resent.
			case p.File != nil:
				return nil, fmt.Errorf("%w: file parts are not supported yet", ErrUnsupportedPart)
			case p.Data != nil:
				return nil, fmt.Errorf("%w: data parts are not supported yet", ErrUnsupportedPart)
			}
		}
		role := m.Role
		if role == "" {
			role = "user"
		}
		out = append(out, chatMessage{Role: role, Content: text.String()})
	}
	return out, nil
}

// toModelResponse builds the neutral completion.
//
// A reasoning part is produced only when the endpoint actually returned reasoning content. The
// project never synthesizes one: a fabricated part would be recorded verbatim and replayed as if
// the provider had produced it.
func toModelResponse(model string, out chatResponse) api.ModelResponse {
	choice := out.Choices[0]
	parts := make([]api.Part, 0, 2)
	if r := choice.Message.ReasoningContent; r != "" {
		parts = append(parts, api.Part{Reasoning: &api.ReasoningPart{
			Provider: "openai",
			ModelID:  model,
			Opaque:   []byte(r),
		}})
	}
	if t := choice.Message.Content; t != "" {
		parts = append(parts, api.Part{Text: &api.TextPart{Text: t}})
	}
	return api.ModelResponse{
		Message: api.Message{Role: "assistant", Parts: parts},
		Usage: api.Usage{
			Model:           model,
			InputTokens:     out.Usage.PromptTokens,
			OutputTokens:    out.Usage.CompletionTokens,
			ReasoningTokens: out.Usage.CompletionTokensDetails.ReasoningTokens,
		},
	}
}

// ---- wire types ----

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
			// Not an OpenAI field. OpenAI keeps reasoning hidden and reports only its token
			// count. Several compatible endpoints (DeepSeek and gateways that pass it through)
			// do return the text here, so it is read when present and absent everywhere else,
			// which simply yields no reasoning part. Untested against a live reasoning endpoint.
			ReasoningContent string `json:"reasoning_content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens            int64 `json:"prompt_tokens"`
		CompletionTokens        int64 `json:"completion_tokens"`
		CompletionTokensDetails struct {
			ReasoningTokens int64 `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
}

type errorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}
