package main

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/model/openai"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func TestModelAPIKey(t *testing.T) {
	for _, tt := range []struct {
		name   string
		model  string
		openai string
		want   string
	}{
		{"no key", "", "", ""},
		{"endpoint-neutral key", "model-key", "", "model-key"},
		{"legacy fallback", "", "openai-key", "openai-key"},
		{"endpoint-neutral takes precedence", "model-key", "openai-key", "model-key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MODEL_API_KEY", tt.model)
			t.Setenv("OPENAI_API_KEY", tt.openai)
			if modelAPIKey() != tt.want {
				t.Fatal("model credential precedence changed")
			}
		})
	}
}

func TestHarnessRegistry(t *testing.T) {
	for _, tt := range []struct {
		model string
		names []string
	}{
		{"", []string{"echo"}},
		{"test-model", []string{"chat", "echo"}},
	} {
		t.Run("model="+tt.model, func(t *testing.T) {
			modelFn, streamFn, _, err := modelFunc(tt.model, openai.DefaultBaseURL, openai.DefaultPath, "Authorization")
			if err != nil {
				t.Fatal(err)
			}
			registry, closeBackends, err := harnessRegistry(tt.model, modelFn, streamFn, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(closeBackends)
			if got := registry.Names(); !reflect.DeepEqual(got, tt.names) {
				t.Fatalf("harnesses = %v, want %v", got, tt.names)
			}
			if registry.Default() != "echo" {
				t.Fatalf("default = %q, want echo", registry.Default())
			}
			defaultPlacer, err := registry.For("")
			if err != nil {
				t.Fatal(err)
			}
			echo, err := registry.For("echo")
			if err != nil || echo != defaultPlacer {
				t.Fatalf("empty harness did not select echo: %v", err)
			}
			if tt.model != "" {
				chat, err := registry.For("chat")
				if err != nil || chat == echo {
					t.Fatalf("chat did not get its own placer: %v", err)
				}
			}

			c := newTestClient(t, registry)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if tt.model == "" {
				turn, err := c.Exec(ctx, client.ExecOptions{Inputs: []string{"hello"}})
				if err != nil {
					t.Fatal(err)
				}
				if turn.Session.GetHarness() != "echo" || turn.Output != "echo:hello" {
					t.Fatalf("default turn = %+v, want built-in echo", turn)
				}
				if _, err := registry.For("chat"); !errors.Is(err, placement.ErrUnknownHarness) {
					t.Fatalf("unconfigured chat error = %v", err)
				}
			}
			unknown := []string{"unknown"}
			if tt.model == "" {
				unknown = append(unknown, "chat")
			}
			for _, harness := range unknown {
				_, err := registry.For(harness)
				if !errors.Is(err, placement.ErrUnknownHarness) {
					t.Fatalf("unknown harness error = %v", err)
				}
				_, err = c.CreateSession(ctx, &v1.Session{Harness: harness})
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("create unknown harness: %v", err)
				}
				_, err = c.Exec(ctx, client.ExecOptions{Harness: harness, Inputs: []string{"hello"}})
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("exec unknown harness: %v", err)
				}
			}
		})
	}
}

func TestHarnessRegistryClosesBackends(t *testing.T) {
	registry, closeBackends, err := harnessRegistry("test-model", echoagent.Model, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeBackends)
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var sockets []string
	for _, name := range registry.Names() {
		placer, err := registry.For(name)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := placer.Exec(ctx, store.Session(name), name, []api.Message{*api.TextMessage("user", "hello")}, 0)
		if err != nil {
			t.Fatal(err)
		}
		sock, ok := strings.CutPrefix(inc.Address, "unix://")
		if !ok {
			t.Fatalf("unexpected local address %q", inc.Address)
		}
		if _, err := os.Stat(sock); err != nil {
			t.Fatalf("backend socket %q: %v", sock, err)
		}
		sockets = append(sockets, sock)
	}
	if len(sockets) != 2 || sockets[0] == sockets[1] {
		t.Fatalf("echo and chat must have independent backends, got sockets %v", sockets)
	}
	closeBackends()
	for _, sock := range sockets {
		if _, err := os.Stat(sock); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("backend socket %q remains after cleanup: %v", sock, err)
		}
	}
}

func TestChatSessionConversation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		create    bool
		streaming bool
	}{
		{name: "create on exec"},
		{name: "explicit create", create: true},
		{name: "host streaming", streaming: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan api.ModelRequest, 4)
			modelFn := func(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
				requests <- req
				reply := "first reply"
				if req.Messages[len(req.Messages)-1].Text() == "follow-up" {
					reply = "second reply"
				}
				return api.ModelResponse{Message: *api.TextMessage("assistant", reply)}, nil
			}
			var streamFn controller.StreamFunc
			if tt.streaming {
				streamFn = func(ctx context.Context, req api.ModelRequest, onChunk func(string)) (api.ModelResponse, error) {
					resp, err := modelFn(ctx, req)
					onChunk(resp.Message.Text())
					return resp, err
				}
			}
			registry, closeBackends, err := harnessRegistry("test-model", modelFn, streamFn, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(closeBackends)
			c := newTestClient(t, registry)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			firstOpts := client.ExecOptions{Harness: "chat", Inputs: []string{"first question"}}
			if tt.create {
				sess, err := c.CreateSession(ctx, &v1.Session{Harness: "chat", Model: "metadata-only"})
				if err != nil {
					t.Fatal(err)
				}
				firstOpts.Session = sess.GetMetadata().GetUid()
				firstOpts.Harness = ""
			}
			first, err := c.Exec(ctx, firstOpts)
			if err != nil {
				t.Fatal(err)
			}
			if first.Session.GetHarness() != "chat" || first.Output != "first reply" {
				t.Fatalf("first turn = %+v", first)
			}
			var deltas string
			second, err := c.Exec(ctx, client.ExecOptions{
				Session: first.Session.GetMetadata().GetUid(),
				Inputs:  []string{"follow-up"},
				OnDelta: func(d *v1.Delta) { deltas += d.GetChunk() },
			})
			if err != nil {
				t.Fatal(err)
			}
			if second.Output != "second reply" {
				t.Fatalf("second output = %q", second.Output)
			}
			if tt.streaming && deltas != "second reply" {
				t.Fatalf("host deltas = %q", deltas)
			}
			if len(requests) != 2 {
				t.Fatalf("model requests = %d, want 2", len(requests))
			}
			for i, want := range [][]api.Message{
				{*api.TextMessage("user", "first question")},
				{
					*api.TextMessage("user", "first question"),
					*api.TextMessage("assistant", "first reply"),
					*api.TextMessage("user", "follow-up"),
				},
			} {
				req := <-requests
				if req.Model != "test-model" || !reflect.DeepEqual(req.Messages, want) {
					t.Fatalf("request %d = %+v, want configured model and %+v", i+1, req, want)
				}
			}
			for _, turn := range []*client.TurnResult{first, second} {
				var kinds []v1.EventKind
				for _, rec := range turn.Records {
					kinds = append(kinds, rec.GetEvent().GetKind())
				}
				want := []v1.EventKind{
					v1.EventKind_EVENT_INPUT, v1.EventKind_EVENT_MODEL_CALL,
					v1.EventKind_EVENT_OUTPUT, v1.EventKind_EVENT_END,
				}
				if !reflect.DeepEqual(kinds, want) {
					t.Fatalf("turn event kinds = %v, want %v", kinds, want)
				}
			}
		})
	}
}

func TestChatSessionModelFailure(t *testing.T) {
	modelErr := errors.New("model unavailable")
	modelFn := func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		return api.ModelResponse{}, modelErr
	}
	registry, closeBackends, err := harnessRegistry("test-model", modelFn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeBackends)
	c := newTestClient(t, registry)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	turn, err := c.Exec(ctx, client.ExecOptions{Harness: "chat", Inputs: []string{"hello"}})
	if err == nil || !strings.Contains(err.Error(), modelErr.Error()) {
		t.Fatalf("exec error = %v, want model failure", err)
	}
	if turn == nil || turn.Session == nil {
		t.Fatal("failed turn did not report its session")
	}
	// Sessions.Replay reads the journal; it does not re-execute the harness via Controller.Replay.
	records, err := c.Replay(ctx, turn.Session.GetMetadata().GetUid(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []v1.EventKind
	for _, rec := range records {
		kinds = append(kinds, rec.GetEvent().GetKind())
	}
	want := []v1.EventKind{v1.EventKind_EVENT_INPUT, v1.EventKind_EVENT_MODEL_CALL, v1.EventKind_EVENT_ERROR}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("failed turn events = %v, want %v", kinds, want)
	}
	if !strings.Contains(records[2].GetEvent().GetError().GetDescription(), modelErr.Error()) {
		t.Fatalf("error record = %v", records[2])
	}
}

func newTestClient(t *testing.T, registry *placement.Registry) *client.Client {
	t.Helper()
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, session.NewService(store, registry))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := client.Dial("passthrough:///bufnet", client.WithDialOptions(
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
