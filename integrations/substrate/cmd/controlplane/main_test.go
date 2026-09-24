package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseConfigRequiresDeploymentInputs(t *testing.T) {
	if _, err := parseConfig(nil); err == nil || !strings.Contains(err.Error(), "-model is required") {
		t.Fatalf("error = %v, want missing model", err)
	}
	cfg, err := parseConfig([]string{
		"-model", "test-model",
		"-ateapi-insecure-skip-verify",
		"-atespace", "test-space",
		"-actor-template", "test-chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.model != "test-model" || cfg.atespace != "test-space" || cfg.templateName != "test-chat" {
		t.Fatalf("config = %+v", cfg)
	}
	if !cfg.ateapiInsecureSkipVerify {
		t.Fatal("kind TLS override was not parsed")
	}
	if _, err := parseConfig([]string{"-model", "test", "-log-level", "verbose"}); err == nil {
		t.Fatal("invalid log level was accepted")
	}
}

func TestFileTokenCredentialsRereadsToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	creds := &fileTokenCredentials{path: path}
	for _, token := range []string{"first", "second"} {
		if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := creds.GetRequestMetadata(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got["authorization"] != "Bearer "+token {
			t.Fatalf("authorization = %q", got["authorization"])
		}
	}
	if !creds.RequireTransportSecurity() {
		t.Fatal("token credentials must require TLS")
	}
}

func TestFileTokenCredentialsRejectsEmptyToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (&fileTokenCredentials{path: path}).GetRequestMetadata(t.Context()); err == nil {
		t.Fatal("empty token was accepted")
	}
}

func TestEnsureAtespaceIsIdempotent(t *testing.T) {
	for _, code := range []codes.Code{codes.OK, codes.AlreadyExists} {
		t.Run(code.String(), func(t *testing.T) {
			client := &controlClient{code: code}
			if err := ensureAtespace(t.Context(), client, "agentsessions"); err != nil {
				t.Fatal(err)
			}
			if client.name != "agentsessions" {
				t.Fatalf("atespace = %q", client.name)
			}
		})
	}
	client := &controlClient{code: codes.PermissionDenied}
	if err := ensureAtespace(t.Context(), client, "agentsessions"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("error = %v, want PermissionDenied", err)
	}
}

type controlClient struct {
	atepb.ControlClient
	code codes.Code
	name string
}

func (c *controlClient) CreateAtespace(_ context.Context, req *atepb.CreateAtespaceRequest, _ ...grpc.CallOption) (*atepb.Atespace, error) {
	c.name = req.GetAtespace().GetMetadata().GetName()
	if c.code != codes.OK {
		return nil, status.Error(c.code, c.code.String())
	}
	return req.GetAtespace(), nil
}
