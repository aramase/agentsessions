// Command harnessnode is the harness packaged to run INSIDE a compute sandbox (a substrate actor, or
// any runtime). It serves a selected harness over the harnesswire gRPC Harness.Connect stream on :80 —
// the port the atenet mesh routes to — so the external agentsessions controller drives it remotely,
// exactly as it drives the in-process runtime/local harness over a unix socket. A separate HTTP
// /readyz on :8081 lets ResumeActor block until the harness is live (ActorTemplate readyz probe).
//
// This is the substrate realization of the harness half of the step-6 dial path: the transport moves
// from a unix socket to the actor's mesh DNS; the harnesswire server is unchanged.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/chatagent"
	"github.com/aramase/agentsessions/harness/counteragent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/runtime/substrate"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	harness, err := selectHarness()
	if err != nil {
		log.Fatalf("harnessnode: configure harness: %v", err)
	}
	grpcAddr := env("HARNESS_ADDR", ":"+substrate.HarnessPort) // harnesswire gRPC; the driver dials PodIP here
	readyzAddr := env("HARNESS_READYZ", ":8081")               // HTTP readyz for the ActorTemplate probe

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// harnesswire gRPC server (h2c: plain TCP, no TLS — the mesh terminates/originates HTTP/2).
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("harnessnode: listen %s: %v", grpcAddr, err)
	}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(slog.Default())),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(slog.Default())),
	)
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(harness))

	// readyz on a side port so the actor is reported live once the gRPC server is accepting.
	readyz := &http.Server{
		Addr:    readyzAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }),
	}
	go func() {
		if err := readyz.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("harnessnode: readyz: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
		_ = readyz.Close()
	}()

	log.Printf("harnessnode: %q harness serving harnesswire on %s, readyz on %s", env("HARNESS_KIND", "echo"), grpcAddr, readyzAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("harnessnode: serve: %v", err)
	}
}

// selectHarness picks the harness this node serves, by HARNESS_KIND (default "echo"). One image
// serves both conformance tiers: "echo" and "chat" are STATELESS_REPLAY; "counter" requires a memory
// snapshot. Chat's model ID is supplied by HARNESS_MODEL; provider configuration stays on the host.
func selectHarness() (api.Harness, error) {
	switch env("HARNESS_KIND", "echo") {
	case "chat":
		model := os.Getenv("HARNESS_MODEL")
		if model == "" {
			return nil, errors.New("HARNESS_MODEL is required when HARNESS_KIND=chat")
		}
		return chatagent.Harness{Model: model}, nil
	case "counter":
		return &counteragent.Harness{}, nil
	default:
		return echoagent.Harness{}, nil
	}
}
