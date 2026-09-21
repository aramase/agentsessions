// Command controlplane composes the Sessions server with the real agent-substrate runtime.
//
// The generic server lifecycle lives in the root module's internal/sessionserver package. This
// command exists in the integration module because it is the dependency boundary that may import
// agent-substrate's generated ate-api client.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/chatagent"
	ateadapter "github.com/aramase/agentsessions/integrations/substrate"
	"github.com/aramase/agentsessions/internal/modelconfig"
	"github.com/aramase/agentsessions/internal/sessionserver"
	"github.com/aramase/agentsessions/internal/version"
	"github.com/aramase/agentsessions/model/openai"
	"github.com/aramase/agentsessions/placement"
	runtimesubstrate "github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

type config struct {
	addr                     string
	journal                  string
	project                  string
	model                    string
	modelBaseURL             string
	modelPath                string
	modelAuthHeader          string
	logLevel                 string
	ateapiAddr               string
	ateapiServerName         string
	ateapiTokenFile          string
	ateapiCAFile             string
	ateapiInsecureSkipVerify bool
	atespace                 string
	templateNamespace        string
	templateName             string
	showVersion              bool
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "controlplane:", err)
		os.Exit(2)
	}
	if cfg.showVersion {
		fmt.Println("controlplane", version.Get())
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	level, err := parseLogLevel(cfg.logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "controlplane:", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := run(ctx, cfg, logger); err != nil {
		fmt.Fprintln(os.Stderr, "controlplane:", err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("controlplane", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.addr, "addr", ":8080", "address to serve the Sessions API on")
	fs.StringVar(&cfg.journal, "journal", "/data/agentsessions.db", "sqlite journal path")
	fs.StringVar(&cfg.project, "project", sqlitelog.DefaultProject, "default project (tenant)")
	fs.StringVar(&cfg.model, "model", "", "model id for the chat harness (required)")
	fs.StringVar(&cfg.modelBaseURL, "model-base-url", openai.DefaultBaseURL, "base URL of the OpenAI-compatible endpoint")
	fs.StringVar(&cfg.modelPath, "model-path", openai.DefaultPath, "completions path under the base URL; may carry a query string")
	fs.StringVar(&cfg.modelAuthHeader, "model-auth-header", "Authorization", "header carrying the credential from MODEL_API_KEY")
	fs.StringVar(&cfg.logLevel, "log-level", "info", "log level: debug, info, warn, or error")
	fs.StringVar(&cfg.ateapiAddr, "ateapi-addr", "api.ate-system.svc:443", "agent-substrate ate-api gRPC address")
	fs.StringVar(&cfg.ateapiServerName, "ateapi-server-name", "api.ate-system.svc", "TLS server name for ate-api")
	fs.StringVar(&cfg.ateapiTokenFile, "ateapi-token-file", "/var/run/secrets/tokens/ateapi-token", "projected service-account token for ate-api")
	fs.StringVar(&cfg.ateapiCAFile, "ateapi-ca-file", "", "PEM CA bundle for ate-api; empty uses system roots")
	fs.BoolVar(&cfg.ateapiInsecureSkipVerify, "ateapi-insecure-skip-verify", false, "skip ate-api certificate verification (kind demo only)")
	fs.StringVar(&cfg.atespace, "atespace", "agentsessions", "atespace that owns session actors")
	fs.StringVar(&cfg.templateNamespace, "actor-template-namespace", "ate-agentsessions", "namespace containing the chat ActorTemplate")
	fs.StringVar(&cfg.templateName, "actor-template", "chat-harness", "chat ActorTemplate name")
	fs.BoolVar(&cfg.showVersion, "version", false, "print the build version and exit")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.showVersion {
		return cfg, nil
	}
	if _, err := parseLogLevel(cfg.logLevel); err != nil {
		return config{}, err
	}
	required := []struct {
		name  string
		value string
	}{
		{"model", cfg.model},
		{"ateapi-addr", cfg.ateapiAddr},
		{"ateapi-server-name", cfg.ateapiServerName},
		{"ateapi-token-file", cfg.ateapiTokenFile},
		{"atespace", cfg.atespace},
		{"actor-template-namespace", cfg.templateNamespace},
		{"actor-template", cfg.templateName},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return config{}, fmt.Errorf("-%s is required", field.name)
		}
	}
	return cfg, nil
}

func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return 0, fmt.Errorf("invalid -log-level %q: %w", value, err)
	}
	return level, nil
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	modelClient, err := modelconfig.New(modelconfig.Config{
		Model:      cfg.model,
		BaseURL:    cfg.modelBaseURL,
		Path:       cfg.modelPath,
		AuthHeader: cfg.modelAuthHeader,
		APIKey:     modelAPIKey(),
	})
	if err != nil {
		return fmt.Errorf("configure model: %w", err)
	}

	conn, err := dialATEAPI(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := ensureAtespace(initCtx, atepb.NewControlClient(conn), cfg.atespace); err != nil {
		return err
	}

	descriptor, err := (chatagent.Harness{Model: cfg.model}).Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe chat harness: %w", err)
	}
	backend := runtimesubstrate.New(
		ateadapter.New(conn, ""),
		cfg.atespace,
		runtimesubstrate.ObjectRef{Namespace: cfg.templateNamespace, Name: cfg.templateName},
		descriptor,
		runtimesubstrate.WithLogger(logger),
	)
	registry, err := chatRegistry(backend, modelClient.Model, modelClient.StreamModel, logger)
	if err != nil {
		return err
	}

	return sessionserver.ListenAndServe(ctx, cfg.addr, sessionserver.Config{
		Journal:          cfg.journal,
		Project:          cfg.project,
		ModelDescription: cfg.model + " @ " + cfg.modelBaseURL + cfg.modelPath,
	}, registry, logger)
}

func chatRegistry(runtime *runtimesubstrate.Backend, modelFn controller.ModelFunc, streamFn controller.StreamFunc, logger *slog.Logger) (*placement.Registry, error) {
	registry, err := placement.NewRegistry("chat", map[string]*placement.Placer{
		"chat": placement.New(runtime, modelFn,
			placement.WithLogger(logger),
			placement.WithStreamingModel(streamFn),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("build harness registry: %w", err)
	}
	return registry, nil
}

func dialATEAPI(cfg config) (*grpc.ClientConn, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.ateapiServerName,
		InsecureSkipVerify: cfg.ateapiInsecureSkipVerify, //nolint:gosec // explicit kind-only escape hatch
	}
	if cfg.ateapiCAFile != "" {
		pem, err := os.ReadFile(cfg.ateapiCAFile)
		if err != nil {
			return nil, fmt.Errorf("read ate-api CA file %q: %w", cfg.ateapiCAFile, err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ate-api CA file %q contains no certificates", cfg.ateapiCAFile)
		}
		tlsConfig.RootCAs = roots
	}
	conn, err := grpc.NewClient(cfg.ateapiAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithPerRPCCredentials(&fileTokenCredentials{path: cfg.ateapiTokenFile}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial ate-api %s: %w", cfg.ateapiAddr, err)
	}
	return conn, nil
}

func ensureAtespace(ctx context.Context, ctl atepb.ControlClient, name string) error {
	_, err := ctl.CreateAtespace(ctx, &atepb.CreateAtespaceRequest{
		Atespace: &atepb.Atespace{Metadata: &atepb.ResourceMetadata{Name: name}},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("ensure atespace %q: %w", name, err)
	}
	return nil
}

type fileTokenCredentials struct{ path string }

func (c *fileTokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("read ate-api token file %q: %w", c.path, err)
	}
	value := strings.TrimSpace(string(token))
	if value == "" {
		return nil, fmt.Errorf("ate-api token file %q is empty", c.path)
	}
	return map[string]string{"authorization": "Bearer " + value}, nil
}

func (*fileTokenCredentials) RequireTransportSecurity() bool { return true }

func modelAPIKey() string {
	if key := os.Getenv("MODEL_API_KEY"); key != "" {
		return key
	}
	return os.Getenv("OPENAI_API_KEY")
}
