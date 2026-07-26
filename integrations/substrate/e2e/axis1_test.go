// Package e2e drives the agentsessions conformance path against a REAL agent-substrate ate-system in
// kind. It lives in the substrate integration module (not core), so the neutral core still imports no
// substrate. It is gated on SUBSTRATE_E2E=1 and a live cluster, so a normal `go test ./...` skips it.
//
// Axis 1 (this file): the STATELESS_REPLAY path. The Placer places the echo harness on substrate,
// cold-boots the actor (ResumeActor{boot:true}), drives one turn over the atenet mesh, then the
// controller replays the durable journal byte-identically with zero model invocations — proving the
// SPI mapping + our determinism model on real substrate, no memory dependency. Axis 2
// (REQUIRES_MEMORY_SNAPSHOT on a micro-VM) is a separate driver.
//
// The ate-api Control client authenticates with substrate's token scheme (TLS on :443 plus a bearer
// SA JWT minted for the api-server's audience); see controlConn. Everything else — the Placer wiring,
// the mesh dial, the conformance assertion — is the same code paths exercised by the unit and
// runtime/local tests.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"regexp"
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

// kubectlPortForward forwards a Service port to a local ephemeral port via a `kubectl port-forward`
// subprocess (no client-go dependency), returning the local port and a stop func. It parses the
// "Forwarding from 127.0.0.1:<port>" line kubectl prints when ready.
func kubectlPortForward(t *testing.T, namespace, svc string, remotePort int) (int, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward",
		"-n", namespace, "svc/"+svc, fmt.Sprintf(":%d", remotePort))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatalf("port-forward %s/%s: %v", namespace, svc, err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("port-forward %s/%s start: %v", namespace, svc, err)
	}
	re := regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+)`)
	local := make(chan int, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if m := re.FindStringSubmatch(sc.Text()); m != nil {
				var p int
				fmt.Sscanf(m[1], "%d", &p)
				local <- p
				return
			}
		}
		close(local)
	}()
	select {
	case p, ok := <-local:
		if !ok {
			cancel()
			t.Fatalf("port-forward %s/%s: never became ready", namespace, svc)
		}
		return p, cancel
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatalf("port-forward %s/%s: timeout waiting for ready", namespace, svc)
		return 0, cancel
	}
}

// controlConn dials the port-forwarded ate-api Control with the token-auth scheme substrate's kind
// install configures (--ateapi-client-auth=token): TLS on :443 plus a per-RPC bearer SA JWT. Over a
// 127.0.0.1 port-forward the server cert's SAN (api.ate-system.svc) cannot match the loopback dial
// target, so server verification is skipped — the transport is a local kubectl tunnel, and the token,
// not the channel, is the credential. The token is minted by mintControlToken for the audience the
// api-server binds; the server authenticates any valid SA JWT (no per-identity authz). Env-overridable.
func controlConn(t *testing.T, localPort int) *grpc.ClientConn {
	t.Helper()
	target := env("ATEAPI_TARGET", fmt.Sprintf("127.0.0.1:%d", localPort))
	tlsCreds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // loopback port-forward tunnel; the SA JWT is the credential
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(bearerToken(mintControlToken(t))),
	)
	if err != nil {
		t.Fatalf("ate-api dial: %v", err)
	}
	return conn
}

// mintControlToken issues a short-lived SA JWT for the ate-api audience via `kubectl create token` —
// the external-client analog of the projected token substrate's in-cluster callers mount. The
// audience must equal the api-server's --client-jwt-audience; the SA identity is unconstrained.
func mintControlToken(t *testing.T) string {
	t.Helper()
	sa := env("ATEAPI_TOKEN_SA", "default")
	saNS := env("ATEAPI_TOKEN_SA_NS", env("ATE_SYSTEM_NS", "ate-system"))
	aud := env("ATEAPI_AUDIENCE", "api.ate-system.svc")
	cmd := exec.Command("kubectl", "create", "token", sa, "-n", saNS, "--audience", aud)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl create token %s/%s (aud=%s): %v: %s", saNS, sa, aud, err, stderr.String())
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		t.Fatal("kubectl create token returned an empty token")
	}
	return tok
}

// bearerToken presents a static SA JWT as gRPC per-RPC credentials. RequireTransportSecurity is true
// so the token only ever rides the TLS channel, never cleartext.
type bearerToken string

func (b bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

func (bearerToken) RequireTransportSecurity() bool { return true }

// ensureAtespace idempotently creates the atespace actors are placed in. Substrate requires it to
// exist before CreateActor (like a namespace); it is out-of-band setup, not part of our runtime SPI,
// so the e2e provisions it directly over the Control API rather than through the Placer.
func ensureAtespace(t *testing.T, conn *grpc.ClientConn, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := atepb.NewControlClient(conn).CreateAtespace(ctx, &atepb.CreateAtespaceRequest{
		Atespace: &atepb.Atespace{Metadata: &atepb.ResourceMetadata{Name: name}},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("create atespace %q: %v", name, err)
	}
}

// dialActor opens a harnesswire client to the actor's harness over h2c at a host:port — the actor's
// PodIP:HarnessPort (Incarnation.Address). This is the same direct dial the Placer's default dialer
// performs; the e2e reuses it for the replay re-dial. The atenet mesh is HTTP/1.1-only to actors, so
// gRPC bypasses the router and reaches the pod directly — which requires in-cluster pod-network
// reachability (see the in-cluster runner, slice 2).
func dialActor(t *testing.T, address string) (api.Harness, func() error) {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial harness %s: %v", address, err)
	}
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), conn.Close
}

// TestAxis1PlacedExecReplayOnSubstrate is the axis-1 acceptance: one placed turn on real substrate
// (ResumeActor{boot:true} + direct PodIP:HarnessPort dial) replays byte-identically with zero model
// invocations, and the tamper-evident chain still verifies.
func TestAxis1PlacedExecReplayOnSubstrate(t *testing.T) {
	if os.Getenv("SUBSTRATE_E2E") == "" {
		t.Skip("set SUBSTRATE_E2E=1 (and a live ate-system + KUBECONFIG) to run the substrate axis-1 e2e")
	}
	ns := env("ATE_SYSTEM_NS", "ate-system")
	atespace := env("SUBSTRATE_ATESPACE", "e2e")
	tmplNS := env("ACTORTEMPLATE_NS", "ate-agentsessions")
	tmplName := env("ACTORTEMPLATE_NAME", "echo-harness")

	// Port-forward the ate-api Control (lifecycle). The harness is reached directly at the actor's
	// PodIP:HarnessPort (Path A), not the router — so once this driver runs in-cluster (slice 2) it
	// needs no router port-forward at all. Externally the Control port-forward still works.
	ctlPort, stopCtl := kubectlPortForward(t, ns, env("ATEAPI_SVC", "api"), 443)
	defer stopCtl()

	ctlConn := controlConn(t, ctlPort)
	ensureAtespace(t, ctlConn, atespace)
	adapter := ateadapter.New(ctlConn, "")
	// The echo harness declares STATELESS_REPLAY (axis 1); the descriptor is what the placement gate
	// reads before Create (substrate's harness is remote, so it is configured, not introspected).
	echoDesc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := substrate.New(adapter, atespace, substrate.ObjectRef{Namespace: tmplNS, Name: tmplName}, echoDesc)

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("e2e")
	// No injected dialer: the Placer's default dialer dials the actor's PodIP:HarnessPort directly.
	p := placement.New(backend, echoagent.Model)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Place + drive one turn: Create the actor (boot:true), dial the harness at PodIP:HarnessPort, run.
	inc, err := p.Exec(ctx, log, "e2e", []api.Message{*api.TextMessage("user", "hi")}, 0)
	if err != nil {
		t.Fatalf("placed exec on substrate: %v", err)
	}
	recs, _ := log.Read(1)
	live := outputsOf(recs)
	if len(live) == 0 {
		t.Fatal("placed turn produced no output")
	}

	// Replay through the same placed harness (same PodIP:HarnessPort): the recorded answer is served,
	// the model is not invoked.
	har, closeHar := dialActor(t, inc.Address)
	defer closeHar()
	c2, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := c2.Replay(ctx, har)
	if err != nil {
		t.Fatalf("replay on substrate: %v", err)
	}
	if !reflect.DeepEqual(live, replay) {
		t.Fatalf("substrate replay diverged: live=%v replay=%v", live, replay)
	}
	if c2.ModelInvocations() != 0 {
		t.Fatalf("substrate replay invoked the model %d times (I1)", c2.ModelInvocations())
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("chain verify after placed exec+replay: %v", err)
	}
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
