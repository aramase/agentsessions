package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func TestHarnessSpecParsingRejectsUnknownAndMalformedJSON(t *testing.T) {
	for _, body := range []string{`{`, `{"remote":{"address":"127.0.0.1:1"},"unknown":"data"}`} {
		path := filepath.Join(t.TempDir(), "spec.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		err := cmdHarness([]string{"register", "--server", "127.0.0.1:1", "--name", "mine", "--spec", path})
		if err == nil || !strings.Contains(err.Error(), "parse HarnessSpec") {
			t.Fatalf("body %s: got %v, want strict JSON parse error before dialing", body, err)
		}
	}
}

func TestHarnessCommandRequiresOperatorServer(t *testing.T) {
	for _, args := range [][]string{
		{"register", "--name", "mine", "--spec", "spec.json"},
		{"get", "--name", "mine"}, {"list"}, {"retire", "--name", "mine"},
	} {
		if err := cmdHarness(args); err == nil || !strings.Contains(err.Error(), "--server") {
			t.Errorf("harness %v: got %v, want --server usage error", args, err)
		}
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "usage: agentctl harness"},
		{[]string{"bad", "--server", "127.0.0.1:1"}, `unknown harness command "bad"`},
		{[]string{"get", "--server", "127.0.0.1:1"}, "--name is required"},
		{[]string{"register", "--server", "127.0.0.1:1", "--name", "mine"}, "--spec is required"},
	} {
		if err := cmdHarness(tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("harness %v: got %v, want %q", tc.args, err, tc.want)
		}
	}
}

// The request commits even if stdout becomes unwritable: do not tell a caller it succeeded
// when the JSON registration receipt never reached them. Exercise the real registry over TCP.
func TestHarnessRegisterReportsStdoutFailureAfterCommit(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	backend := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = backend.Close() })
	placers, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(backend, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := session.NewHarnessRegistry(store, placers, func(_ string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
		remoteBackend := remote.New(spec.GetRemote().GetAddress())
		return placement.New(remoteBackend, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId())), func() { _ = remoteBackend.Close() }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterHarnessRegistryServer(server, registry)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	spec := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(spec, []byte(`{"remote":{"address":"127.0.0.1:1"},"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	read, closed, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close() })
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = closed
	defer func() { os.Stdout = stdout }()
	callErr := cmdHarness([]string{"register", "--server", listener.Addr().String(), "--name", "mine", "--spec", spec})
	os.Stdout = stdout
	if got, err := store.Harness("mine"); err != nil || got.Name != "mine" || got.SpecDigest == "" {
		t.Fatalf("registration did not commit despite stdout failure: %+v, %v", got, err)
	}
	if !errors.Is(callErr, os.ErrClosed) || !strings.Contains(callErr.Error(), "RPC succeeded") {
		t.Fatalf("lost registration receipt: got %v, want wrapped closed stdout and explicit RPC success", callErr)
	}
}
