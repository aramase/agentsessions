// Command agentctl is the client-facing CLI for agentsessions — the top seam a user or GitHub
// drives. By default it runs an EMBEDDED Sessions gRPC server over a local unix socket (backed by a
// sqlite journal on disk) and connects to it as a real gRPC client — not an in-process wrapper — so
// the same code path serves remote. Pass --server <addr> to drive a remote controller instead.
//
//	agentctl create  --name "triage" --project acme
//	agentctl list    --project acme           # enumerate sessions, newest first
//	agentctl exec    --input "hi"            # create + run a turn (prints the session UID)
//	agentctl exec    --session <uid> --input "..."
//	agentctl replay  --session <uid>          # re-deliver the committed log
//	agentctl fork    --session <uid> --at N    # branch into a child session
//	agentctl suspend --session <uid>
//	agentctl resume  --session <uid>
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/internal/version"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmds := map[string]func([]string) error{
		"create":  cmdCreate,
		"list":    cmdList,
		"get":     cmdGet,
		"exec":    cmdExec,
		"replay":  cmdReplay,
		"fork":    cmdFork,
		"suspend": cmdSuspend,
		"resume":  cmdResume,
		"version": cmdVersion,
	}
	run, ok := cmds[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}
	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "agentctl:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: agentctl <create|list|get|exec|replay|fork|suspend|resume|version> [flags]")
}

type config struct {
	server  string
	journal string
	project string
}

func commonFlags(fs *flag.FlagSet) *config {
	c := &config{}
	fs.StringVar(&c.server, "server", "", "remote Sessions gRPC address; empty = embedded local server")
	fs.StringVar(&c.journal, "journal", "agentsessions.db", "sqlite journal path (embedded mode)")
	fs.StringVar(&c.project, "project", sqlitelog.DefaultProject, "project (tenant) to create and list sessions in")
	return c
}

// dial returns a Sessions client. With --server it dials the remote; otherwise it starts an
// embedded Sessions server over a unix socket, backed by the local sqlite journal, and dials that.
func dial(cfg *config) (*client.Client, func(), error) {
	if cfg.server != "" {
		c, err := client.Dial(cfg.server, client.WithProject(cfg.project))
		if err != nil {
			return nil, nil, err
		}
		return c, func() { _ = c.Close() }, nil
	}

	store, err := sqlitelog.Open(cfg.journal, sqlitelog.WithDefaultProject(cfg.project))
	if err != nil {
		return nil, nil, err
	}
	logger := slog.Default()
	backend := local.New(echoagent.Harness{}, local.WithLogger(logger))
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(backend, echoagent.Model, placement.WithLogger(logger)),
	})
	if err != nil {
		backend.Close()
		store.Close()
		return nil, nil, err
	}
	svc := session.NewService(store, registry,
		session.WithLogger(logger), session.WithDefaultProject(cfg.project))
	sock := fmt.Sprintf("%s/agentctl-%d.sock", os.TempDir(), os.Getpid())
	_ = os.Remove(sock)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		backend.Close()
		store.Close()
		return nil, nil, err
	}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(logger)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(logger)),
	)
	v1.RegisterSessionsServer(srv, svc)
	go srv.Serve(lis)

	c, err := client.Dial("passthrough:///embedded",
		client.WithProject(cfg.project),
		client.WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		})),
	)
	if err != nil {
		srv.Stop()
		os.Remove(sock)
		backend.Close()
		store.Close()
		return nil, nil, err
	}
	cleanup := func() {
		_ = c.Close()
		srv.Stop()
		os.Remove(sock)
		backend.Close()
		store.Close()
	}
	return c, cleanup, nil
}

func cmdVersion([]string) error {
	fmt.Println("agentctl", version.Get())
	return nil
}

func cmdCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	cfg := commonFlags(fs)
	var name, harness, model string
	fs.StringVar(&name, "name", "", "human-readable session name")
	fs.StringVar(&harness, "harness", "", "harness to run the session on (default: the host's)")
	fs.StringVar(&model, "model", "", "model id")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	sess, err := c.CreateSession(context.Background(), &v1.Session{
		Metadata: &v1.ResourceMetadata{Project: cfg.project, Name: name},
		Harness:  harness,
		Model:    model,
	})
	if err != nil {
		return err
	}
	fmt.Println(sess.GetMetadata().GetUid())
	return nil
}

// cmdList walks every page rather than printing the first one. A CLI that stopped at the default
// page size would quietly under-report, which is worse than being slow.
func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	cfg := commonFlags(fs)
	var pageSize int
	fs.IntVar(&pageSize, "page-size", 0, "sessions per request; 0 uses the server default")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	sessions, err := c.ListSessions(context.Background(), cfg.project)
	if err != nil {
		return err
	}
	for _, s := range sessions {
		fmt.Printf("%s\tlast_seq=%d\tcompute=%s\tharness=%s\tname=%s\n",
			s.GetMetadata().GetUid(),
			s.GetLastSeq(),
			strings.TrimPrefix(s.GetComputeState().String(), "COMPUTE_"),
			s.GetHarness(),
			s.GetMetadata().GetName(),
		)
	}
	return nil
}

func cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	cfg := commonFlags(fs)
	var uid string
	fs.StringVar(&uid, "session", "", "session UID")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	sess, err := c.GetSession(context.Background(), uid)
	if err != nil {
		return err
	}
	fmt.Printf("session %s project=%s name=%s harness=%s last_seq=%d compute=%s\n",
		sess.GetMetadata().GetUid(),
		sess.GetMetadata().GetProject(),
		sess.GetMetadata().GetName(),
		sess.GetHarness(),
		sess.GetLastSeq(),
		strings.TrimPrefix(sess.GetComputeState().String(), "COMPUTE_"),
	)
	return nil
}

func cmdExec(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess, input string
	fs.StringVar(&sess, "session", "", "session UID (created if empty)")
	fs.StringVar(&input, "input", "", "user input for this turn")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	// One call: an empty session is created by the server, and leaving ExpectedLastSeq nil appends
	// at the head. Records print as they commit rather than after the turn.
	_, err = c.Exec(context.Background(), client.ExecOptions{
		Session: sess,
		Inputs:  []string{input},
		OnSession: func(s *v1.Session) {
			if sess == "" {
				fmt.Printf("session %s\n", s.GetMetadata().GetUid())
			}
		},
		OnRecord: printRecord,
		OnDelta: func(d *v1.Delta) {
			// Deltas are transport: print them raw so output appears as the model produces it,
			// then the finalized EVENT_OUTPUT record prints normally when it commits.
			fmt.Print(d.GetChunk())
			if d.GetDone() {
				fmt.Println()
			}
		},
	})
	return err
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess string
	var from int64
	fs.StringVar(&sess, "session", "", "session UID")
	fs.Int64Var(&from, "from", 1, "replay from this seq")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	records, err := c.Replay(context.Background(), sess, from, 0)
	if err != nil {
		return err
	}
	for _, r := range records {
		printRecord(r)
	}
	return nil
}

func cmdFork(args []string) error {
	fs := flag.NewFlagSet("fork", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess string
	var at int64
	var count int
	var names string
	fs.StringVar(&sess, "session", "", "parent session UID")
	fs.Int64Var(&at, "at", 0, "fork at this seq (0 = head)")
	fs.IntVar(&count, "count", 1, "number of children")
	fs.StringVar(&names, "names", "", "comma-separated display names for the children, in order; must match -count")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	var childNames []string
	if names != "" {
		childNames = strings.Split(names, ",")
	}
	children, err := c.Fork(context.Background(), sess, client.ForkOptions{
		AtSeq: at, Count: int32(count), Names: childNames,
	})
	if err != nil {
		return err
	}
	for _, ch := range children {
		fmt.Printf("child %s parent=%s fork_seq=%d name=%s\n", ch.GetMetadata().GetUid(), ch.GetParentUid(), ch.GetForkSeq(), ch.GetMetadata().GetName())
	}
	return nil
}

func cmdSuspend(args []string) error { return lifecycle(args, "suspend") }
func cmdResume(args []string) error  { return lifecycle(args, "resume") }

func lifecycle(args []string, which string) error {
	fs := flag.NewFlagSet(which, flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess string
	fs.StringVar(&sess, "session", "", "session UID")
	_ = fs.Parse(args)
	c, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx := context.Background()
	var out *v1.Session
	if which == "suspend" {
		out, err = c.Suspend(ctx, sess)
	} else {
		out, err = c.Resume(ctx, sess, false)
	}
	if err != nil {
		return err
	}
	fmt.Printf("session %s %s compute=%s last_seq=%d\n", out.GetMetadata().GetUid(), which, out.GetComputeState(), out.GetLastSeq())
	return nil
}

func printRecord(r *v1.LogRecord) {
	ev := r.GetEvent()
	line := fmt.Sprintf("seq=%-3d %s", r.GetSeq(), ev.GetKind())
	if msg := ev.GetMessage(); msg != nil {
		line += "  " + partsText(msg)
	}
	fmt.Println(line)
}

func partsText(m *v1.Message) string {
	s := m.GetRole() + ":"
	for _, p := range m.GetParts() {
		if t := p.GetText(); t != nil {
			s += " " + t.GetText()
		}
	}
	return s
}
