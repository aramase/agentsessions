// Command agentsessionsd serves the Sessions API over TCP.
//
// Until now the Sessions service was only ever registered inside agentctl, on a per-process unix
// socket torn down when the command exits, so agentctl's --server flag had nothing to dial and a
// non-Go client had no way to reach a session at all. This is that missing entry point.
//
// Harnesses are registered at build time. This binary serves the reference echo harness on the
// filesystem-only local backend; a deployment that needs others builds a server with a larger
// registry, or with backends whose capabilities can satisfy them. A harness the registry does not
// know is refused rather than silently substituted.
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
	"github.com/aramase/agentsessions/harness/echoagent"
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
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store, err := sqlitelog.Open(*journal, sqlitelog.WithDefaultProject(*project))
	if err != nil {
		return fmt.Errorf("open journal %s: %w", *journal, err)
	}
	defer store.Close()

	backend := local.New(echoagent.Harness{}, local.WithLogger(logger))
	defer backend.Close()

	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(backend, echoagent.Model, placement.WithLogger(logger)),
	})
	if err != nil {
		return fmt.Errorf("build harness registry: %w", err)
	}

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
		"addr", lis.Addr().String(),
		"journal", *journal,
		"project", *project,
		"harnesses", registry.Names(),
		"default_harness", registry.Default(),
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
