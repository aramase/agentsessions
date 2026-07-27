// Command axis1 is the in-cluster axis-1 conformance driver for real agent-substrate. It runs as a
// Kubernetes Job inside the ate-system cluster (where pod IPs route), reaches ate-api-server Control
// over its ClusterIP DNS with a mounted ServiceAccount token, places the echo harness on substrate
// (ResumeActor{boot:true}), drives one turn over a DIRECT dial to the actor's PodIP:HarnessPort — the
// atenet mesh is HTTP/1.1-only to actors, so gRPC bypasses the router — then replays the journal
// byte-identically with zero model invocations and verifies the tamper-evident chain.
//
// It needs no Kubernetes API access: the actor's pod IP comes from ResumeActor, not the k8s API. Exit
// 0 means axis-1 conformance holds on real substrate; any non-zero exit fails the Job.
//
// This lives in the substrate integration module (its own go.mod), so the neutral core still imports
// zero substrate. It is the ko-built, in-cluster realization of the former external e2e driver.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	ateadapter "github.com/aramase/agentsessions/integrations/substrate"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatalf("axis-1 conformance FAILED: %v", err)
	}
	log.Printf("axis-1 conformance PASSED")
}

func run(ctx context.Context) error {
	controlAddr := env("ATEAPI_ADDR", "api.ate-system.svc:443")
	tokenFile := env("ATEAPI_TOKEN_FILE", "/var/run/secrets/tokens/ateapi-token")
	serverName := env("ATEAPI_SERVER_NAME", "api.ate-system.svc")
	atespace := env("SUBSTRATE_ATESPACE", "axis1")
	tmplNS := env("ACTORTEMPLATE_NS", "ate-agentsessions")
	tmplName := env("ACTORTEMPLATE_NAME", "echo-harness")
	session := env("SESSION_UID", "")
	if session == "" {
		// A conformance run is a fresh session; a unique UID keeps runs independent and re-runnable on a
		// persistent cluster (each placement is a new actor, no CreateActor AlreadyExists collision).
		session = fmt.Sprintf("axis1-%d", time.Now().UnixNano())
	}

	conn, err := controlConn(controlAddr, serverName, tokenFile)
	if err != nil {
		return fmt.Errorf("dial Control %s: %w", controlAddr, err)
	}
	defer conn.Close()

	if err := ensureAtespace(ctx, conn, atespace); err != nil {
		return fmt.Errorf("ensure atespace %q: %w", atespace, err)
	}
	log.Printf("Control reachable at %s; atespace %q ready", controlAddr, atespace)

	echoDesc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := substrate.New(ateadapter.New(conn, ""), atespace, substrate.ObjectRef{Namespace: tmplNS, Name: tmplName}, echoDesc)

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		return err
	}
	defer store.Close()
	journal := store.Session(session)
	// No injected dialer: the Placer's default dialer dials the actor's PodIP:HarnessPort directly.
	p := placement.New(backend, echoagent.Model)

	// Place + drive one turn: Create the actor (boot:true), dial the harness at PodIP:HarnessPort, run.
	inc, err := p.Exec(ctx, journal, session, []api.Message{*api.TextMessage("user", "hi")}, 0)
	if inc.ID != "" {
		// Best-effort cleanup so repeated runs don't leak actors onto workers. Runs at return, after the
		// replay below, on a fresh context (the main one may be near its deadline).
		defer func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer ccancel()
			if serr := backend.Stop(cctx, inc); serr != nil {
				log.Printf("cleanup: stop actor %q: %v", inc.ID, serr)
			}
		}()
	}
	if err != nil {
		return fmt.Errorf("placed exec: %w", err)
	}
	log.Printf("placed actor: address=%s worker=%s", inc.Address, inc.Worker)
	recs, err := journal.Read(1)
	if err != nil {
		return err
	}
	live := outputsOf(recs)
	if len(live) == 0 {
		return fmt.Errorf("placed turn produced no output")
	}
	log.Printf("live turn output: %v", live)

	// Replay through the same placed harness (same PodIP:HarnessPort): the recorded answer is served,
	// the model is not invoked.
	har, closeHar, err := dialActor(inc.Address)
	if err != nil {
		return fmt.Errorf("replay dial %s: %w", inc.Address, err)
	}
	defer closeHar()
	c2, err := controller.New(journal, echoagent.Model)
	if err != nil {
		return err
	}
	replay, err := c2.Replay(ctx, har)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	if !reflect.DeepEqual(live, replay) {
		return fmt.Errorf("replay diverged: live=%v replay=%v", live, replay)
	}
	if c2.ModelInvocations() != 0 {
		return fmt.Errorf("replay invoked the model %d times (want 0, I1)", c2.ModelInvocations())
	}
	if err := journal.Verify(); err != nil {
		return fmt.Errorf("chain verify: %w", err)
	}
	log.Printf("replay byte-identical, model invocations=0, chain verified")
	return nil
}

// controlConn dials ate-api Control in-cluster: TLS on :443 plus a per-RPC bearer read from the
// mounted projected SA token (--ateapi-client-auth=token). Server verification is skipped — the SA
// JWT, not the channel, is the credential and the dial is in-cluster; ServerName is set for SNI.
func controlConn(addr, serverName, tokenFile string) (*grpc.ClientConn, error) {
	tlsCreds := credentials.NewTLS(&tls.Config{ServerName: serverName, InsecureSkipVerify: true}) //nolint:gosec // in-cluster dial; the mounted SA JWT is the credential
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(&fileTokenCreds{path: tokenFile}),
	)
}

// fileTokenCreds presents the mounted projected SA token as a per-RPC bearer, re-reading the file each
// call so Kubernetes token rotation is picked up (mirrors substrate's own in-cluster client).
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

// ensureAtespace idempotently creates the atespace actors are placed in (substrate requires it before
// CreateActor, like a namespace).
func ensureAtespace(ctx context.Context, conn *grpc.ClientConn, name string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := atepb.NewControlClient(conn).CreateAtespace(ctx, &atepb.CreateAtespaceRequest{
		Atespace: &atepb.Atespace{Metadata: &atepb.ResourceMetadata{Name: name}},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}
	return nil
}

// dialActor opens a harnesswire client to the actor's harness over h2c at PodIP:HarnessPort — the same
// direct dial the Placer's default dialer performs, reused here for the replay re-dial.
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
