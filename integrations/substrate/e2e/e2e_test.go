// Package e2e is the real-substrate conformance suite. Every test here runs against a LIVE
// ate-system: no fakes, no doubles, no canned control-plane replies.
//
// It is an ordinary Go test package, compiled into a test binary and executed as a Kubernetes Job
// inside the cluster. In-cluster is not a preference: the atenet mesh is HTTP/1.1-only to actors, so
// a harness is reached by a direct gRPC dial to the actor's PodIP:HarnessPort, which only routes from
// inside. Control is reached over the ate-api-server ClusterIP with a mounted ServiceAccount token.
//
// Tests SKIP unless AGENTSESSIONS_E2E=1, so `go test ./...` on a laptop or in the fast PR job still
// compiles every line here — a driver that no longer builds fails the cheap gate, not the 17-minute
// one.
package e2e

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harnesswire"
	ateadapter "github.com/aramase/agentsessions/integrations/substrate"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Template names installed by the workflow before the suite runs.
const (
	echoTemplate    = "echo-harness"    // STATELESS_REPLAY, gVisor
	counterTemplate = "counter-microvm" // REQUIRES_MEMORY_SNAPSHOT, micro-VM
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// requireCluster skips unless this binary was pointed at a live ate-system.
func requireCluster(t *testing.T) {
	t.Helper()
	if os.Getenv("AGENTSESSIONS_E2E") != "1" {
		t.Skip("needs a live ate-system; set AGENTSESSIONS_E2E=1 (the substrate-e2e workflow does)")
	}
}

// fixture is one test's connection to the cluster: a Control channel and a private atespace.
type fixture struct {
	conn     *grpc.ClientConn
	atespace string
	tmplNS   string
}

// newFixture dials Control and ensures the test's atespace exists. Each test gets its own atespace
// so a failure cannot leave actors that collide with the next run.
func newFixture(t *testing.T, atespace string) *fixture {
	t.Helper()
	requireCluster(t)

	addr := env("ATEAPI_ADDR", "api.ate-system.svc:443")
	serverName := env("ATEAPI_SERVER_NAME", "api.ate-system.svc")
	tokenFile := env("ATEAPI_TOKEN_FILE", "/var/run/secrets/tokens/ateapi-token")

	// TLS on :443 plus a per-RPC bearer read from the mounted projected SA token
	// (--ateapi-client-auth=token). Server verification is skipped: the SA JWT, not the channel, is
	// the credential, and the dial is in-cluster. ServerName is set for SNI.
	creds := credentials.NewTLS(&tls.Config{ServerName: serverName, InsecureSkipVerify: true}) //nolint:gosec // in-cluster dial; the mounted SA JWT is the credential
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(&fileTokenCreds{path: tokenFile}))
	if err != nil {
		t.Fatalf("dial Control %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	f := &fixture{conn: conn, atespace: atespace, tmplNS: env("ACTORTEMPLATE_NS", "ate-agentsessions")}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = atepb.NewControlClient(conn).CreateAtespace(ctx, &atepb.CreateAtespaceRequest{
		Atespace: &atepb.Atespace{Metadata: &atepb.ResourceMetadata{Name: atespace}},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ensure atespace %q: %v", atespace, err)
	}
	t.Logf("Control reachable at %s; atespace %q ready", addr, atespace)
	return f
}

// backend builds the substrate Runtime for a harness template in this fixture's atespace.
func (f *fixture) backend(template string, desc api.Descriptor) *substrate.Backend {
	return substrate.New(ateadapter.New(f.conn, ""), f.atespace,
		substrate.ObjectRef{Namespace: f.tmplNS, Name: template}, desc)
}

// journal opens an in-memory hash-chained log store for the test. The log is the session's source of
// truth; keeping it in-process (not in the cluster) is what lets the test assert on it directly.
func journal(t *testing.T) *sqlitelog.Store {
	t.Helper()
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// uniqueUID keeps runs independent on a persistent cluster: every placement is a new actor, so a
// re-run never collides with the actors a previous one left behind.
func uniqueUID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// stopQuietly tears an actor down on a fresh context, so cleanup still runs when the test's own
// context is spent. Failures are logged, not fatal: the assertions have already been made.
func stopQuietly(t *testing.T, b *substrate.Backend, uid string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := b.Stop(ctx, api.Incarnation{ID: uid}); err != nil {
		t.Logf("cleanup: stop actor %q: %v", uid, err)
	}
}

// dialActor opens a harnesswire client straight to the actor's harness at PodIP:HarnessPort — the
// same direct dial the Placer performs, reused where a test needs a second connection of its own.
func dialActor(address string) (api.Harness, func() error, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close, nil
}

func outputsOf(recs []eventlog.Record) []string {
	var out []string
	for _, r := range recs {
		if r.Event.Kind == api.EventOutput && r.Event.Message != nil {
			out = append(out, r.Event.Message.Text())
		}
	}
	return out
}

// lastOutput is the most recent OUTPUT text in a session's log — for the counter harness, its
// current count.
func lastOutput(t *testing.T, log *sqlitelog.Log) string {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	outs := outputsOf(recs)
	if len(outs) == 0 {
		t.Fatal("session produced no output")
	}
	return outs[len(outs)-1]
}

// fileTokenCreds presents the mounted projected SA token as a per-RPC bearer, re-reading the file
// each call so Kubernetes token rotation is picked up (mirrors substrate's own in-cluster client).
type fileTokenCreds struct{ path string }

func (c *fileTokenCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("read token file %q: %w", c.path, err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return nil, fmt.Errorf("token file %q is empty", c.path)
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (c *fileTokenCreds) RequireTransportSecurity() bool { return true }
