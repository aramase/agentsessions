// Command modelstub is a deterministic OpenAI-compatible endpoint for the kind smoke test.
// It is test infrastructure, not the model used by the documented production-shaped deployment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Stream bool `json:"stream"`
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/", chat)
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	log.Printf("modelstub listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Model == "" || len(req.Messages) == 0 {
		http.Error(w, "model and messages are required", http.StatusBadRequest)
		return
	}
	last := req.Messages[len(req.Messages)-1].Content
	text := fmt.Sprintf("context=%d last=%s", len(req.Messages), last)
	log.Printf("completion model=%q messages=%d", req.Model, len(req.Messages))

	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, map[string]any{
			"choices": []any{map[string]any{"delta": map[string]string{"content": text}}},
		})
		writeSSE(w, map[string]any{
			"choices": []any{},
			"usage": map[string]int{
				"prompt_tokens":     len(req.Messages),
				"completion_tokens": 1,
			},
		})
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]string{"role": "assistant", "content": text},
		}},
		"usage": map[string]int{
			"prompt_tokens":     len(req.Messages),
			"completion_tokens": 1,
		},
	})
}

func writeSSE(w io.Writer, value any) {
	payload, _ := json.Marshal(value)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
}
