package modelconfig_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/internal/modelconfig"
)

func TestNewAppliesExplicitConfig(t *testing.T) {
	// Environment policy belongs to the host, not this constructor.
	t.Setenv("MODEL_API_KEY", "ignored-model-key")
	t.Setenv("OPENAI_API_KEY", "ignored-openai-key")
	const key = "test-key"
	for _, tt := range []struct {
		name       string
		authHeader string
		key        string
		wantBearer string
		wantAPIKey string
	}{
		{"bearer", "Authorization", key, "Bearer " + key, ""},
		{"case insensitive bearer", "authorization", key, "Bearer " + key, ""},
		{"custom header", "api-key", key, "", key},
		{"no key", "Authorization", "", "", ""},
		{"no header", "", key, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			type request struct {
				uri    string
				header http.Header
				model  string
				err    error
			}
			requests := make(chan request, 1)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Model string `json:"model"`
				}
				err := json.NewDecoder(r.Body).Decode(&body)
				requests <- request{uri: r.RequestURI, header: r.Header.Clone(), model: body.Model, err: err}
				_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
			}))
			t.Cleanup(endpoint.Close)

			c, err := modelconfig.New(modelconfig.Config{
				Model:      "configured-model",
				BaseURL:    endpoint.URL + "/v1",
				Path:       "/deployment/completions?api-version=test",
				AuthHeader: tt.authHeader,
				APIKey:     tt.key,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Model(t.Context(), api.ModelRequest{
				Messages: []api.Message{*api.TextMessage("user", "hello")},
			})
			if err != nil {
				t.Fatal(err)
			}
			got := <-requests
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.uri != "/v1/deployment/completions?api-version=test" || got.model != "configured-model" {
				t.Fatalf("request URI = %q, model = %q", got.uri, got.model)
			}
			if got.header.Get("Authorization") != tt.wantBearer || got.header.Get("api-key") != tt.wantAPIKey {
				t.Fatal("credential header does not match explicit configuration")
			}
		})
	}
}

func TestNewRequiresModel(t *testing.T) {
	c, err := modelconfig.New(modelconfig.Config{})
	if err == nil || c != nil {
		t.Fatalf("client = %v, error = %v, want missing-model failure", c, err)
	}
}
