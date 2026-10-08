package main

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/counteragent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
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
			registry, closeBackends, err := harnessRegistry(tt.model, nil, modelFn, streamFn, nil)
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
	registry, closeBackends, err := harnessRegistry("test-model", nil, echoagent.Model, nil, nil)
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
			registry, closeBackends, err := harnessRegistry("test-model", nil, modelFn, streamFn, nil)
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
					v1.EventKind_EVENT_EXECUTION_START,
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
	registry, closeBackends, err := harnessRegistry("test-model", nil, modelFn, nil, nil)
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
	want := []v1.EventKind{
		v1.EventKind_EVENT_EXECUTION_START, v1.EventKind_EVENT_INPUT,
		v1.EventKind_EVENT_MODEL_CALL, v1.EventKind_EVENT_ERROR,
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("failed turn events = %v, want %v", kinds, want)
	}
	if !strings.Contains(records[3].GetEvent().GetError().GetDescription(), modelErr.Error()) {
		t.Fatalf("error record = %v", records[3])
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

func TestRemoteHarnessFlagParsing(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		want    map[string]string
		wantErr bool
	}{
		{name: "name and address", values: []string{"shouty=127.0.0.1:9000"}, want: map[string]string{"shouty": "127.0.0.1:9000"}},
		{name: "repeatable", values: []string{"a=host:1", "b=host:2"}, want: map[string]string{"a": "host:1", "b": "host:2"}},
		{name: "surrounding space is trimmed", values: []string{" a = host:1 "}, want: map[string]string{"a": "host:1"}},
		{name: "address may carry a scheme-less host name", values: []string{"a=harness.svc.cluster.local:80"}, want: map[string]string{"a": "harness.svc.cluster.local:80"}},
		{name: "IPv6 host", values: []string{"a=[::1]:9000"}, want: map[string]string{"a": "[::1]:9000"}},
		{name: "dns resolver", values: []string{"a=dns:///harness.example:9000"}, want: map[string]string{"a": "dns:///harness.example:9000"}},
		{name: "dns resolver with an authority", values: []string{"a=dns://8.8.8.8/harness.example:9000"}, want: map[string]string{"a": "dns://8.8.8.8/harness.example:9000"}},
		{name: "absolute unix socket", values: []string{"a=unix:///run/h.sock"}, want: map[string]string{"a": "unix:///run/h.sock"}},
		{name: "relative unix socket", values: []string{"a=unix:h.sock"}, want: map[string]string{"a": "unix:h.sock"}},
		{name: "relative unix socket after unix://", values: []string{"a=unix://run/h.sock"}, want: map[string]string{"a": "unix://run/h.sock"}},
		{name: "no separator", values: []string{"shouty"}, wantErr: true},
		{name: "empty name", values: []string{"=host:1"}, wantErr: true},
		{name: "empty address", values: []string{"a="}, wantErr: true},
		{name: "duplicate name", values: []string{"a=host:1", "a=host:2"}, wantErr: true},
		{name: "http URL", values: []string{"a=http://127.0.0.1:9000"}, wantErr: true},
		{name: "https URL", values: []string{"a=https://harness.example"}, wantErr: true},
		{name: "other gRPC scheme", values: []string{"a=passthrough:///127.0.0.1:9000"}, wantErr: true},
		{name: "unix with no path", values: []string{"a=unix://"}, wantErr: true},
		{name: "unix with only a root", values: []string{"a=unix:///"}, wantErr: true},
		{name: "unix with only a doubled root", values: []string{"a=unix:////"}, wantErr: true},
		{name: "unix with only a dot", values: []string{"a=unix:///."}, wantErr: true},
		{name: "unix current directory", values: []string{"a=unix:."}, wantErr: true},
		{name: "bare unix", values: []string{"a=unix:"}, wantErr: true},
		{name: "missing port", values: []string{"a=127.0.0.1"}, wantErr: true},
		{name: "missing host", values: []string{"a=:9000"}, wantErr: true},
		{name: "port not a number", values: []string{"a=host:http"}, wantErr: true},
		{name: "port out of range", values: []string{"a=host:70000"}, wantErr: true},
		{name: "port zero", values: []string{"a=host:0"}, wantErr: true},
		{name: "not an address", values: []string{"a=not an address"}, wantErr: true},
		{name: "space in host", values: []string{"a=not an address:80"}, wantErr: true},
		{name: "dns with no endpoint", values: []string{"a=dns:///"}, wantErr: true},
		{name: "dns with no port", values: []string{"a=dns:///harness.example"}, wantErr: true},
		{name: "dns with an authority and no endpoint", values: []string{"a=dns://8.8.8.8"}, wantErr: true},
		{name: "dns resolver with a port", values: []string{"a=dns://8.8.8.8:5353/harness.example:9000"}, want: map[string]string{"a": "dns://8.8.8.8:5353/harness.example:9000"}},
		{name: "dns resolver as IPv6", values: []string{"a=dns://[::1]:53/harness.example:9000"}, want: map[string]string{"a": "dns://[::1]:53/harness.example:9000"}},
		{name: "dns resolver as a host name", values: []string{"a=dns://ns.example/harness.example:9000"}, want: map[string]string{"a": "dns://ns.example/harness.example:9000"}},
		{name: "dns opaque form", values: []string{"a=dns:harness.example:9000"}, want: map[string]string{"a": "dns:harness.example:9000"}},
		{name: "dns resolver with an invalid escape", values: []string{"a=dns://%/127.0.0.1:9000"}, wantErr: true},
		{name: "dns endpoint with an invalid escape", values: []string{"a=dns:///127.0.0.1%zz:9000"}, wantErr: true},
		{name: "dns resolver with an unclosed IPv6 bracket", values: []string{"a=dns://[::1/127.0.0.1:9000"}, wantErr: true},
		{name: "dns resolver with an empty port", values: []string{"a=dns://8.8.8.8:/harness.example:9000"}, wantErr: true},
		{name: "dns resolver with a port out of range", values: []string{"a=dns://8.8.8.8:70000/harness.example:9000"}, wantErr: true},
		{name: "dns resolver with only a port", values: []string{"a=dns://:53/harness.example:9000"}, wantErr: true},
		{name: "dns resolver that is not a host", values: []string{"a=dns://n$s/harness.example:9000"}, wantErr: true},
		{name: "dns with userinfo", values: []string{"a=dns://user@8.8.8.8/harness.example:9000"}, wantErr: true},
		{name: "dns with a query", values: []string{"a=dns:///harness.example:9000?x=1"}, wantErr: true},
		{name: "dns with a fragment", values: []string{"a=dns:///harness.example:9000#x"}, wantErr: true},
		{name: "host named like a gRPC scheme", values: []string{"a=passthrough:8080"}, wantErr: true},
		{name: "host named like another gRPC scheme", values: []string{"a=unix-abstract:80"}, wantErr: true},
		{name: "host named like a gRPC scheme in upper case", values: []string{"a=PASSTHROUGH:8080"}, wantErr: true},
		{name: "host named like a gRPC scheme behind dns", values: []string{"a=dns:///passthrough:8080"}, want: map[string]string{"a": "dns:///passthrough:8080"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remotes := remoteHarnesses{}
			var err error
			for _, v := range tt.values {
				if err = remotes.Set(v); err != nil {
					break
				}
			}
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Set(%q) accepted an invalid value", tt.values)
				}
				t.Logf("Set(%q): %v", tt.values, err)
				return
			}
			if err != nil {
				t.Fatalf("Set(%q): %v", tt.values, err)
			}
			if len(remotes) != len(tt.want) {
				t.Fatalf("got %d harnesses, want %d", len(remotes), len(tt.want))
			}
			for name, addr := range tt.want {
				if remotes[name] != addr {
					t.Fatalf("harness %q = %q, want %q", name, remotes[name], addr)
				}
			}
		})
	}
}

// A remote harness must not silently displace one this binary serves itself, and when two names
// collide the error must not depend on map iteration order.
func TestRemoteHarnessCannotShadowABuiltInName(t *testing.T) {
	for i := 0; i < 20; i++ {
		remotes := remoteHarnesses{"echo": "127.0.0.1:9000", "chat": "127.0.0.1:9001"}
		_, closeBackends, err := harnessRegistry("test-model", remotes, echoagent.Model, nil, nil)
		if err == nil {
			closeBackends()
			t.Fatal("registry accepted remote harnesses named echo and chat, shadowing the built-ins")
		}
		if want := `harness "chat" is already served`; !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want the first name in sorted order (%s)", err, want)
		}
	}
}

// A malformed address names the harness and the accepted forms, so the operator can fix the flag.
func TestRemoteHarnessAddressErrorNamesTheForms(t *testing.T) {
	err := remoteHarnesses{}.Set("mine=http://127.0.0.1:9000")
	if err == nil {
		t.Fatal("Set accepted an http:// address")
	}
	for _, want := range []string{`"mine"`, `"http"`, "host:port", "unix:///absolute/path"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %s", err, want)
		}
	}
}

// The dns: addresses the flag accepts are ones gRPC can resolve, and the ones gRPC refuses are refused
// at startup too. gRPC's DNS resolver reads the resolver authority only for a host-name endpoint, so
// these resolve localhost; the authorities are loopback, so the background lookup that Build starts
// never leaves the machine before Close stops it. The flag is stricter than gRPC in places (it
// requires a port and refuses a query), which TestRemoteHarnessFlagParsing covers.
func TestDNSAddressesAgreeWithGRPC(t *testing.T) {
	for _, tt := range []struct {
		addr  string
		valid bool
	}{
		{addr: "dns:///localhost:9000", valid: true},
		{addr: "dns:localhost:9000", valid: true},
		{addr: "dns:///127.0.0.1:9000", valid: true},
		{addr: "dns:///[::1]:9000", valid: true},
		{addr: "dns://127.0.0.1/localhost:9000", valid: true},
		{addr: "dns://127.0.0.1:5353/localhost:9000", valid: true},
		{addr: "dns://[::1]/localhost:9000", valid: true},
		{addr: "dns://[::1]:5353/localhost:9000", valid: true},
		{addr: "dns://localhost/localhost:9000", valid: true},
		{addr: "dns://%/127.0.0.1:9000"},
		{addr: "dns://%/localhost:9000"},
		{addr: "dns:///127.0.0.1%zz:9000"},
		{addr: "dns://[::1/localhost:9000"},
		{addr: "dns://127.0.0.1:/localhost:9000"},
		{addr: "dns:///localhost:"},
		{addr: "dns:///"},
		{addr: "dns://127.0.0.1"},
		{addr: "dns:"},
	} {
		t.Run(tt.addr, func(t *testing.T) {
			grpcErr, flagErr := grpcResolves(tt.addr), checkHarnessAddress(tt.addr)
			t.Logf("gRPC: %v; flag: %v", grpcErr, flagErr)
			if tt.valid != (grpcErr == nil) {
				t.Fatalf("gRPC resolves %q: %v, want %v", tt.addr, grpcErr == nil, tt.valid)
			}
			if tt.valid != (flagErr == nil) {
				t.Fatalf("the flag accepts %q: %v, want %v", tt.addr, flagErr == nil, tt.valid)
			}
		})
	}
}

// grpcResolves parses addr the way grpc.NewClient does and builds gRPC's DNS resolver for it, which
// is where an invalid endpoint or resolver authority is reported.
func grpcResolves(addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	_ = conn.Close()
	u, err := url.Parse(addr)
	if err != nil {
		return err
	}
	r, err := resolver.Get("dns").Build(resolver.Target{URL: *u}, nopResolverClientConn{}, resolver.BuildOptions{})
	if err != nil {
		return err
	}
	r.Close()
	return nil
}

type nopResolverClientConn struct{}

func (nopResolverClientConn) UpdateState(resolver.State) error { return nil }
func (nopResolverClientConn) ReportError(error)                {}
func (nopResolverClientConn) NewAddress([]resolver.Address)    {}
func (nopResolverClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

// The startup checks that need no side effect refuse a name collision before main opens the journal,
// with the same deterministic error harnessRegistry gives as a backstop.
func TestCheckRemotes(t *testing.T) {
	tests := []struct {
		name    string
		model   string
		remotes remoteHarnesses
		wantErr string
	}{
		{name: "no remotes", model: ""},
		{name: "free name", model: "m", remotes: remoteHarnesses{"mine": "127.0.0.1:1"}},
		{name: "needs a model", model: "", remotes: remoteHarnesses{"mine": "127.0.0.1:1"}, wantErr: "requires -model"},
		{name: "shadows echo", model: "m", remotes: remoteHarnesses{"echo": "127.0.0.1:1"}, wantErr: `harness "echo" is already served`},
		{name: "shadows chat", model: "m", remotes: remoteHarnesses{"chat": "127.0.0.1:1"}, wantErr: `harness "chat" is already served`},
		{name: "first collision in sorted order", model: "m", remotes: remoteHarnesses{"echo": "127.0.0.1:1", "chat": "127.0.0.1:2"}, wantErr: `harness "chat" is already served`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkRemotes(tt.model, tt.remotes)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkRemotes: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("checkRemotes = %v, want an error containing %s", err, tt.wantErr)
			}
		})
	}
}

// Without -model a remote harness's model calls would be answered by the built-in echo stub and
// journaled as if a model had produced them. Refuse at startup instead.
func TestRemoteHarnessRequiresAModel(t *testing.T) {
	_, closeBackends, err := harnessRegistry("", remoteHarnesses{"mine": "127.0.0.1:9000"}, echoagent.Model, nil, nil)
	if err == nil {
		closeBackends()
		t.Fatal("registry accepted a remote harness with no -model")
	}
	if !strings.Contains(err.Error(), "-model") || !strings.Contains(err.Error(), "mine") {
		t.Fatalf("error = %q, want it to name the harness and the missing -model", err)
	}
}

// A harness registered by address is routed by name through the registry and the Sessions API,
// a REQUIRES_MEMORY_SNAPSHOT one is refused as FailedPrecondition, and closeBackends releases them.
func TestRemoteHarnessRoutesThroughTheRegistry(t *testing.T) {
	goneAddr, stopGone := serveStoppableHarness(t, echoagent.Harness{})
	remotes := remoteHarnesses{
		"mine": serveHarness(t, echoagent.Harness{}),
		"ctr":  serveHarness(t, &counteragent.Harness{}),
		"gone": goneAddr,
	}
	registry, closeBackends, err := harnessRegistry("test-model", remotes, echoagent.Model, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeBackends()
		}
	})
	if got, want := registry.Names(), []string{"chat", "ctr", "echo", "gone", "mine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("harnesses = %v, want %v", got, want)
	}

	c := newTestClient(t, registry)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	turn, err := c.Exec(ctx, client.ExecOptions{Harness: "mine", Inputs: []string{"hello"}})
	if err != nil {
		t.Fatalf("exec on remote harness: %v", err)
	}
	if turn.Session.GetHarness() != "mine" || turn.Output != "echo:hello" {
		t.Fatalf("turn = harness %q output %q, want mine / echo:hello", turn.Session.GetHarness(), turn.Output)
	}

	refused, err := c.Exec(ctx, client.ExecOptions{Harness: "ctr", Inputs: []string{"hello"}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec on a REQUIRES_MEMORY_SNAPSHOT remote harness: got %v, want FailedPrecondition", err)
	}
	if refused != nil && refused.LastSeq != 0 {
		t.Fatalf("a refused exec journaled records: last_seq=%d", refused.LastSeq)
	}

	// A harness that has gone away is UNAVAILABLE on every admitted call, and nothing is journaled,
	// so the caller can retry once it is back.
	goneTurn, err := c.Exec(ctx, client.ExecOptions{Harness: "gone", Inputs: []string{"hello"}})
	if err != nil {
		t.Fatalf("exec on remote harness: %v", err)
	}
	goneUID := goneTurn.Session.GetMetadata().GetUid()
	stopGone()
	if _, err := c.Exec(ctx, client.ExecOptions{Session: goneUID, ExpectedLastSeq: &goneTurn.LastSeq, Inputs: []string{"again"}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("exec on an unreachable remote harness: got %v, want Unavailable", err)
	}
	if _, err := c.Fork(ctx, goneUID, client.ForkOptions{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("fork of a session on an unreachable remote harness: got %v, want Unavailable", err)
	}
	if s, err := c.GetSession(ctx, goneUID); err != nil || s.GetLastSeq() != goneTurn.LastSeq {
		t.Fatalf("session after refused calls: last_seq=%d err=%v, want %d", s.GetLastSeq(), err, goneTurn.LastSeq)
	}

	closeBackends()
	closed = true
	if _, err := c.Exec(ctx, client.ExecOptions{Session: turn.Session.GetMetadata().GetUid(), ExpectedLastSeq: &turn.LastSeq, Inputs: []string{"again"}}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("exec after closeBackends = %v, want the remote backend to report it is closed", err)
	}
}

// serveStoppableHarness is serveHarness with a stop function, for a harness that goes away.
func serveStoppableHarness(t *testing.T, h api.Harness) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(h))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), srv.Stop
}

// serveHarness runs a harness on its own loopback listener, the way an operator runs one out of band.
func serveHarness(t *testing.T, h api.Harness) string {
	t.Helper()
	addr, _ := serveStoppableHarness(t, h)
	return addr
}
