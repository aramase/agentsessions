// Command axis2 is the in-cluster axis-2 conformance driver for real agent-substrate: it proves
// memory continuity across suspend/restore — the differentiator a plain pod (a filesystem-only
// runtime) cannot do. Like cmd/axis1 it runs as a Kubernetes Job (Control over the ate-api-server
// ClusterIP + a mounted SA token; the harness over PodIP:HarnessPort), but the harness is the in-RAM
// counter (REQUIRES_MEMORY_SNAPSHOT) on the micro-VM sandbox class.
//
// Flow (spec: conformance-on-substrate §10): place the counter -> drive it to N (count=N in guest RAM,
// NOT in the journal) -> SuspendActor (memory snapshot to storage, worker freed) -> ResumeActor{boot:
// false} (restore RAM on a fresh worker) -> drive one more turn, and assert four things:
//  1. Continuity:        the counter returns N+1 — the in-RAM state survived the snapshot round-trip.
//  2. No double-apply:   the value is N+1, not 2N+1 — the controller did not replay the journal into
//     the restored RAM (the counter never reconstructs state from Start.History).
//  3. Provenance:        the hash chain still verifies ACROSS the SUSPEND boundary.
//  4. Fresh fence:       the post-restore turn extends the chain under a new fence (superseding the
//     suspended incarnation).
//
// It drives the Runtime SPI directly (Create/Snapshot/Restore) rather than the Placer: a memory suspend
// must leave the actor SUSPENDED-and-resumable (SuspendActor), not Stop it (which DeleteActors and
// would break ResumeActor{boot:false}). Exit 0 means axis-2 conformance holds on real substrate.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strconv"
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
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatalf("axis-2 conformance FAILED: %v", err)
	}
	log.Printf("axis-2 conformance PASSED")
}

func run(ctx context.Context) error {
	controlAddr := env("ATEAPI_ADDR", "api.ate-system.svc:443")
	tokenFile := env("ATEAPI_TOKEN_FILE", "/var/run/secrets/tokens/ateapi-token")
	serverName := env("ATEAPI_SERVER_NAME", "api.ate-system.svc")
	atespace := env("SUBSTRATE_ATESPACE", "axis2")
	tmplNS := env("ACTORTEMPLATE_NS", "ate-agentsessions")
	tmplName := env("ACTORTEMPLATE_NAME", "counter-microvm")
	session := env("SESSION_UID", fmt.Sprintf("axis2-%d", time.Now().UnixNano()))
	turns, _ := strconv.Atoi(env("AXIS2_TURNS", "3"))
	if turns < 1 {
		turns = 1
	}

	conn, err := controlConn(controlAddr, serverName, tokenFile)
	if err != nil {
		return fmt.Errorf("dial Control %s: %w", controlAddr, err)
	}
	defer conn.Close()
	if err := ensureAtespace(ctx, conn, atespace); err != nil {
		return fmt.Errorf("ensure atespace %q: %w", atespace, err)
	}

	// The counter declares REQUIRES_MEMORY_SNAPSHOT (in-RAM state); the gate must ACCEPT it on
	// substrate (MemorySnapshot=true). The refusal counterpart (runtime/local) is a unit test.
	counter := api.Descriptor{ID: "counter", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}
	backend := substrate.New(ateadapter.New(conn, ""), atespace, substrate.ObjectRef{Namespace: tmplNS, Name: tmplName}, counter)
	if !controller.CanPlace(counter.Capabilities, backend.Capabilities()) {
		return fmt.Errorf("substrate refused a REQUIRES_MEMORY_SNAPSHOT harness (CanPlace=false)")
	}
	log.Printf("Control reachable at %s; atespace %q ready; substrate accepts REQUIRES_MEMORY_SNAPSHOT", controlAddr, atespace)

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		return err
	}
	defer store.Close()
	journal := store.Session(session)

	// Place the counter and drive it to N: count=N lives in guest RAM, not the journal.
	inc1, err := backend.Create(ctx, &api.SessionSpec{SessionUID: session})
	if err != nil {
		return fmt.Errorf("create actor: %w", err)
	}
	fence1, err := journal.NewFence()
	if err != nil {
		return err
	}
	c1, err := controller.New(journal, echoagent.Model, controller.WithFence(fence1))
	if err != nil {
		return err
	}
	har1, close1, err := dialActor(inc1.Address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", inc1.Address, err)
	}
	for i := 0; i < turns; i++ {
		head, err := journal.Head()
		if err != nil {
			return err
		}
		if err := c1.Advance(ctx, har1, []api.Message{*api.TextMessage("user", "inc")}, head); err != nil {
			_ = close1()
			return fmt.Errorf("drive turn %d: %w", i+1, err)
		}
	}
	_ = close1()
	log.Printf("drove %d turns pre-suspend; outputs=%v (count=%d in guest RAM)", turns, outputsOf(mustRead(journal)), turns)

	// SuspendActor: memory snapshot to storage, worker freed, actor SUSPENDED (NOT deleted — we resume
	// it). Record a SUSPEND lifecycle event so the hash chain spans the snapshot boundary.
	ref, err := backend.Snapshot(ctx, inc1, api.SnapshotExternal)
	if err != nil {
		return fmt.Errorf("suspend (snapshot): %w", err)
	}
	if err := recordSuspend(journal, ref); err != nil {
		return fmt.Errorf("record suspend event: %w", err)
	}
	log.Printf("suspended: memory snapshot at %s", ref.ExternalURI)

	// ResumeActor{boot:false}: restore guest RAM on a (possibly different) worker.
	inc2, err := backend.Restore(ctx, ref)
	if err != nil {
		return fmt.Errorf("resume (restore): %w", err)
	}
	log.Printf("restored: address=%s worker=%s", inc2.Address, inc2.Worker)
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if serr := backend.Stop(cctx, inc2); serr != nil {
			log.Printf("cleanup: stop actor %q: %v", session, serr)
		}
	}()

	// Drive one more turn under a FRESH fence: the counter must continue from N (returns N+1).
	fence2, err := journal.NewFence()
	if err != nil {
		return err
	}
	c2, err := controller.New(journal, echoagent.Model, controller.WithFence(fence2))
	if err != nil {
		return err
	}
	har2, close2, err := dialActor(inc2.Address)
	if err != nil {
		return fmt.Errorf("dial restored %s: %w", inc2.Address, err)
	}
	head, err := journal.Head()
	if err != nil {
		return err
	}
	if err := c2.Advance(ctx, har2, []api.Message{*api.TextMessage("user", "inc")}, head); err != nil {
		_ = close2()
		return fmt.Errorf("drive post-restore turn: %w", err)
	}
	_ = close2()

	// Assertions.
	outs := outputsOf(mustRead(journal))
	if len(outs) == 0 {
		return fmt.Errorf("no outputs recorded")
	}
	last := outs[len(outs)-1]
	log.Printf("post-restore output=%q (full=%v)", last, outs)

	wantContinue := strconv.Itoa(turns + 1)
	if last != wantContinue {
		return fmt.Errorf("continuity broken: post-restore count=%q, want %q (the in-RAM count did not survive the snapshot)", last, wantContinue)
	}
	if doubled := strconv.Itoa(2*turns + 1); last == doubled {
		return fmt.Errorf("double-application: count=%q means the journal was replayed into restored RAM", doubled)
	}
	if err := journal.Verify(); err != nil {
		return fmt.Errorf("chain verify across the snapshot boundary: %w", err)
	}
	if fence2 <= fence1 {
		return fmt.Errorf("fresh-fence broken: post-restore fence %d must supersede the suspended fence %d", fence2, fence1)
	}
	log.Printf("continuity=%s (N+1), no double-apply, chain verified across suspend, fresh fence %d>%d", last, fence2, fence1)
	return nil
}

func mustRead(journal *sqlitelog.Log) []eventlog.Record {
	recs, _ := journal.Read(1)
	return recs
}

// recordSuspend appends a SUSPEND lifecycle event (carrying the snapshot ref) under a fresh fence, so
// the hash chain — and thus provenance — spans the suspend/restore boundary.
func recordSuspend(journal *sqlitelog.Log, ref api.SnapshotRef) error {
	fence, err := journal.NewFence()
	if err != nil {
		return err
	}
	head, err := journal.Head()
	if err != nil {
		return err
	}
	_, err = journal.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleSuspend, Snapshot: &ref}})
	return err
}

// controlConn dials ate-api Control in-cluster: TLS on :443 plus a per-RPC bearer read from the
// mounted projected SA token. Server verification is skipped — the SA JWT, not the channel, is the
// credential and the dial is in-cluster; ServerName is set for SNI.
func controlConn(addr, serverName, tokenFile string) (*grpc.ClientConn, error) {
	tlsCreds := credentials.NewTLS(&tls.Config{ServerName: serverName, InsecureSkipVerify: true}) //nolint:gosec // in-cluster dial; the mounted SA JWT is the credential
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(&fileTokenCreds{path: tokenFile}),
	)
}

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

// dialActor opens a harnesswire client to the actor's harness over h2c at PodIP:HarnessPort.
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
