// Package modelconfig constructs the OpenAI-compatible client shared by repository-owned hosts.
// Callers own flag and environment handling, logging, and any built-in model fallback.
package modelconfig

import (
	"strings"

	"github.com/aramase/agentsessions/model/openai"
)

// Config supplies explicit host-side model settings. Empty BaseURL and Path use openai's defaults.
type Config struct {
	Model   string
	BaseURL string
	Path    string
	// AuthHeader selects bearer authentication for Authorization (case-insensitive), or a raw
	// credential for any other header name. Empty AuthHeader or APIKey sends no credential.
	AuthHeader string
	APIKey     string
}

// New builds a client whose Model and StreamModel methods satisfy the controller's model seams.
// A model is required; this package neither reads the environment nor selects a fallback model.
func New(cfg Config) (*openai.Client, error) {
	opts := []openai.Option{
		openai.WithModel(cfg.Model),
		openai.WithBaseURL(cfg.BaseURL),
		openai.WithPath(cfg.Path),
	}
	if cfg.APIKey != "" {
		if strings.EqualFold(cfg.AuthHeader, "Authorization") {
			opts = append(opts, openai.WithAPIKey(cfg.APIKey))
		} else {
			opts = append(opts, openai.WithHeader(cfg.AuthHeader, cfg.APIKey))
		}
	}
	return openai.New(opts...)
}
