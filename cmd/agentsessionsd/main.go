// Command agentsessionsd serves the Sessions API over TCP.
//
// Until now the Sessions service was only ever registered inside agentctl, on a per-process unix
// socket torn down when the command exits, so agentctl's --server flag had nothing to dial and a
// non-Go client had no way to reach a session at all. This is that missing entry point.
//
// Harnesses are registered at build time. This binary serves the reference echo harness by default
// and adds the conversational chat harness when -model is set, each on its own filesystem-only local
// backend. A deployment that needs others builds a server with a larger registry, or with backends
// whose capabilities can satisfy them. Unknown harnesses are refused rather than substituted.
//
// With -model set it drives a real OpenAI-compatible endpoint; without one it uses the built-in
// echo model, so the quickstart runs with no key. The API key comes from the environment rather
// than a flag, because a flag would put the credential in the process list and shell history.
//
// It is plaintext and unauthenticated: there is no authn, no authz, and no TLS anywhere in the
// reference implementation, and project is a filter rather than a tenancy boundary. Do not expose
// it to an untrusted network. See SECURITY.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/chatagent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/internal/modelconfig"
	"github.com/aramase/agentsessions/internal/version"
	"github.com/aramase/agentsessions/model/openai"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agentsessionsd:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "address to serve the Sessions API on")
	journal := flag.String("journal", "agentsessions.db", "sqlite journal path")
	project := flag.String("project", sqlitelog.DefaultProject, "default project (tenant)")
	model := flag.String("model", "", "model id for an OpenAI-compatible endpoint; empty uses the built-in echo model")
	modelBaseURL := flag.String("model-base-url", openai.DefaultBaseURL, "base URL of the OpenAI-compatible endpoint")
	modelPath := flag.String("model-path", openai.DefaultPath, "completions path under the base URL; may carry a query string")
	modelAuthHeader := flag.String("model-auth-header", "Authorization", "header carrying the credential from MODEL_API_KEY")
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("agentsessionsd", version.Get())
		return nil
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store, err := sqlitelog.Open(*journal, sqlitelog.WithDefaultProject(*project))
	if err != nil {
		return fmt.Errorf("open journal %s: %w", *journal, err)
	}
	defer func() { _ = store.Close() }()

	modelFn, streamFn, modelDesc, err := modelFunc(*model, *modelBaseURL, *modelPath, *modelAuthHeader)
	if err != nil {
		return err
	}

	registry, closeBackends, err := harnessRegistry(*model, modelFn, streamFn, logger)
	if err != nil {
		return err
	}
	defer closeBackends()

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(logger)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(logger)),
	)
	v1.RegisterSessionsServer(srv, session.NewService(store, registry,
		session.WithLogger(logger), session.WithDefaultProject(*project)))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	logger.Info("agentsessionsd listening",
		"version", version.Get().Version,
		"addr", lis.Addr().String(),
		"journal", *journal,
		"project", *project,
		"harnesses", registry.Names(),
		"default_harness", registry.Default(),
		"model", modelDesc,
	)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		srv.GracefulStop()
		return nil
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	}
}

// harnessRegistry owns the local backends as a group so every startup error and shutdown closes
// all of them. Echo remains the default; chat requires an explicitly configured model.
func harnessRegistry(model string, modelFn controller.ModelFunc, streamFn controller.StreamFunc, logger *slog.Logger) (*placement.Registry, func(), error) {
	echo := local.New(echoagent.Harness{}, local.WithLogger(logger))
	backends := []*local.Backend{echo}
	closeBackends := func() {
		for _, backend := range backends {
			_ = backend.Close()
		}
	}
	opts := []placement.Option{
		placement.WithLogger(logger),
		placement.WithStreamingModel(streamFn),
	}
	placers := map[string]*placement.Placer{
		"echo": placement.New(echo, modelFn, opts...),
	}
	if model != "" {
		chat := local.New(chatagent.Harness{Model: model}, local.WithLogger(logger))
		backends = append(backends, chat)
		placers["chat"] = placement.New(chat, modelFn, opts...)
	}
	registry, err := placement.NewRegistry("echo", placers)
	if err != nil {
		closeBackends()
		return nil, nil, fmt.Errorf("build harness registry: %w", err)
	}
	return registry, closeBackends, nil
}

// modelFunc selects the model the host mediates. Empty -model keeps the built-in echo model so the
// quickstart runs with no key; otherwise it builds an OpenAI-compatible client.
//
// The credential is read from MODEL_API_KEY (or OPENAI_API_KEY) rather than taken as a flag,
// because a flag would put it in the process list and in shell history. An unset key sends no
// credential header at all, which is what a local endpoint or a gateway that injects its own
// expects.
//
// -model-path and -model-auth-header exist because endpoints differ in envelope while accepting
// the same request body: some scope the model into the path or pin an API version, and some
// authenticate with a header other than Authorization. Without these, reaching one of those would
// mean recompiling the server for a difference that is pure configuration.
func modelFunc(model, baseURL, path, authHeader string) (controller.ModelFunc, controller.StreamFunc, string, error) {
	if model == "" {
		// The built-in model answers instantly, so there is nothing to stream.
		return echoagent.Model, nil, "echo (built-in)", nil
	}
	client, err := modelconfig.New(modelconfig.Config{
		Model:      model,
		BaseURL:    baseURL,
		Path:       path,
		AuthHeader: authHeader,
		APIKey:     modelAPIKey(),
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("configure model: %w", err)
	}
	return client.Model, client.StreamModel, model + " @ " + baseURL + path, nil
}

// modelAPIKey reads the credential, preferring the endpoint-neutral name. OPENAI_API_KEY is
// accepted too, since that is the variable most tooling already sets.
func modelAPIKey() string {
	if k := os.Getenv("MODEL_API_KEY"); k != "" {
		return k
	}
	return os.Getenv("OPENAI_API_KEY")
}
