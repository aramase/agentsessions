package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChatReportsConversationSize(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "stream"}[stream], func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"model":  "test-model",
				"stream": stream,
				"messages": []map[string]string{
					{"role": "user", "content": "first"},
					{"role": "assistant", "content": "reply"},
					{"role": "user", "content": "second"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
			resp := httptest.NewRecorder()
			chat(resp, req)
			if resp.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
			}
			if !strings.Contains(resp.Body.String(), "context=3 last=second") {
				t.Fatalf("response = %s", resp.Body.String())
			}
			if stream && !strings.Contains(resp.Body.String(), "data: [DONE]") {
				t.Fatalf("stream did not terminate: %s", resp.Body.String())
			}
		})
	}
}
