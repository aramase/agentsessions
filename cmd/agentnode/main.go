// Command agentnode is the co-located agentsessions node used for the pod resume demo: one binary
// running the controller + the echo harness + the durable sqlite journal. On a fresh session it
// executes a turn and persists it; on restart (a new pod remounting the same journal on a
// PersistentVolume) it RESUMES by replaying the journal deterministically — no substrate, no memory
// snapshot — and proves the reconstruction is byte-identical, invoked the model zero times, and the
// tamper-evident chain still verifies.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/sqlitelog"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func recordedOutputs(recs []eventlog.Record) []string {
	var out []string
	for _, r := range recs {
		if r.Event.Kind == api.EventOutput && r.Event.Message != nil {
			out = append(out, r.Event.Message.Text())
		}
	}
	return out
}

func fatalf(host, format string, args ...any) {
	fmt.Printf("[agentnode %s] FATAL: "+format+"\n", append([]any{host}, args...)...)
	os.Exit(1)
}

func main() {
	host, _ := os.Hostname()
	journal := env("AGENT_JOURNAL", "/data/journal.db")
	session := env("AGENT_SESSION", "demo")
	input := env("AGENT_INPUT", "hello from agentsessions")
	ctx := observability.EnsureRequestID(context.Background())
	logger := slog.Default()

	fmt.Printf("[agentnode %s] journal=%s session=%s\n", host, journal, session)

	store, err := sqlitelog.Open(journal)
	if err != nil {
		fatalf(host, "open journal: %v", err)
	}
	defer func() { _ = store.Close() }()
	log := store.Session(session)

	head, err := log.Head()
	if err != nil {
		fatalf(host, "read head: %v", err)
	}

	if head == 0 {
		// New session: run one live turn and persist it to the durable journal.
		c, err := controller.New(log, echoagent.Model, controller.WithLogger(logger), controller.WithSessionUID(session))
		if err != nil {
			fatalf(host, "new controller: %v", err)
		}
		if err := c.Exec(ctx, echoagent.Harness{}, []api.Message{*api.TextMessage("user", input)}, 0); err != nil {
			fatalf(host, "exec: %v", err)
		}
		out, _ := c.Outputs()
		h2, _ := log.Head()
		fmt.Printf("[agentnode %s] MODE=exec   input=%q -> outputs=%v (journal head=%d)\n", host, input, out, h2)
	} else {
		// Existing session on a FRESH pod: resume by replaying the durable journal. No substrate and
		// no memory snapshot: deterministic replay is what makes a plain pod enough.
		recs, err := log.Read(1)
		if err != nil {
			fatalf(host, "read journal: %v", err)
		}
		recorded := recordedOutputs(recs)

		// A fresh incarnation advances the fence.
		c, err := controller.New(
			log,
			echoagent.Model,
			controller.WithLogger(logger),
			controller.WithSessionUID(session),
		)
		if err != nil {
			fatalf(host, "new controller: %v", err)
		}
		replayed, replayErr := c.Replay(ctx, echoagent.Harness{})
		verifyErr := log.Verify()
		byteIdentical := replayErr == nil && verifyErr == nil &&
			c.ModelInvocations() == 0 && reflect.DeepEqual(recorded, replayed)

		fmt.Printf("[agentnode %s] MODE=resume head=%d replayed=%v recorded=%v modelCalls=%d verify=%v BYTE_IDENTICAL=%v\n",
			host, head, replayed, recorded, c.ModelInvocations(), verifyErr, byteIdentical)
		if !byteIdentical {
			fatalf(host, "resume not byte-identical (replayErr=%v)", replayErr)
		}
		fmt.Printf("[agentnode %s] RESUME OK: fresh pod reconstructed the session from the journal via deterministic replay (0 model calls, chain verified)\n", host)
	}

	fmt.Printf("[agentnode %s] idle — delete this pod to trigger resume on a fresh pod\n", host)
	// Block until the pod is terminated (kubectl delete sends SIGTERM). This also keeps the Go
	// runtime from flagging the idle main goroutine as a deadlock, which select{} would.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	fmt.Printf("[agentnode %s] received %s — shutting down\n", host, s)
}
