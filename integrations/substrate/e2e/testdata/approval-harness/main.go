// Command approval-harness is a test-only actor image, not a production harnessnode mode.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/integrations/substrate/e2e/internal/approvalfixture"
	"github.com/aramase/agentsessions/runtime/substrate"
)

func env(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func main() {
	mode := env("HARNESS_MODE", "stateless")
	if mode != "stateless" && mode != "memory" {
		log.Fatalf("approval-harness: unknown HARNESS_MODE %q", mode)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", env("HARNESS_ADDR", ":"+substrate.HarnessPort))
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterHarnessServer(server, harnesswire.NewServer(approvalfixture.Harness{Memory: mode == "memory"}))
	readyz := &http.Server{Addr: env("HARNESS_READYZ", ":8081"), ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })}
	go func() {
		if err := readyz.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("approval-harness: readyz: %v", err)
			stop()
		}
	}()
	go func() { <-ctx.Done(); server.Stop(); _ = readyz.Close() }()
	log.Printf("approval-harness: mode=%s listening=%s", mode, listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
