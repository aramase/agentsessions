// Command agentctl is the client-facing CLI for agentsessions — the top seam a user or GitHub
// drives. By default it runs an EMBEDDED Sessions gRPC server over a local unix socket (backed by a
// sqlite journal on disk) and connects to it as a real gRPC client — not an in-process wrapper — so
// the same code path serves remote. Pass --server <addr> to drive a remote controller instead.
//
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
	"io"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmds := map[string]func([]string) error{
		"create":  cmdCreate,
		"get":     cmdGet,
		"exec":    cmdExec,
		"replay":  cmdReplay,
		"fork":    cmdFork,
		"suspend": cmdSuspend,
		"resume":  cmdResume,
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
	fmt.Fprintln(os.Stderr, "usage: agentctl <create|get|exec|replay|fork|suspend|resume> [flags]")
}

type config struct {
	server  string
	journal string
}

func commonFlags(fs *flag.FlagSet) *config {
	c := &config{}
	fs.StringVar(&c.server, "server", "", "remote Sessions gRPC address; empty = embedded local server")
	fs.StringVar(&c.journal, "journal", "agentsessions.db", "sqlite journal path (embedded mode)")
	return c
}

// dial returns a Sessions client. With --server it dials the remote; otherwise it starts an
// embedded Sessions server over a unix socket, backed by the local sqlite journal, and dials that.
func dial(cfg *config) (v1.SessionsClient, func(), error) {
	if cfg.server != "" {
		conn, err := grpc.NewClient(cfg.server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return v1.NewSessionsClient(conn), func() { conn.Close() }, nil
	}

	store, err := sqlitelog.Open(cfg.journal)
	if err != nil {
		return nil, nil, err
	}
	svc := session.NewService(store, placement.New(local.New(echoagent.Harness{}), echoagent.Model))
	sock := fmt.Sprintf("%s/agentctl-%d.sock", os.TempDir(), os.Getpid())
	_ = os.Remove(sock)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, svc)
	go srv.Serve(lis)

	conn, err := grpc.NewClient(
		"passthrough:///embedded",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}),
	)
	if err != nil {
		srv.Stop()
		os.Remove(sock)
		store.Close()
		return nil, nil, err
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
		os.Remove(sock)
		store.Close()
	}
	return v1.NewSessionsClient(conn), cleanup, nil
}

func cmdCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	cfg := commonFlags(fs)
	_ = fs.Parse(args)
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	sess, err := client.CreateSession(context.Background(), &v1.CreateSessionRequest{})
	if err != nil {
		return err
	}
	fmt.Println(sess.GetMetadata().GetUid())
	return nil
}

func cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	cfg := commonFlags(fs)
	var uid string
	fs.StringVar(&uid, "session", "", "session UID")
	_ = fs.Parse(args)
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	sess, err := client.GetSession(context.Background(), &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		return err
	}
	fmt.Printf("session %s last_seq=%d\n", sess.GetMetadata().GetUid(), sess.GetLastSeq())
	return nil
}

func cmdExec(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess, input string
	fs.StringVar(&sess, "session", "", "session UID (created if empty)")
	fs.StringVar(&input, "input", "", "user input for this turn")
	_ = fs.Parse(args)
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx := context.Background()

	if sess == "" {
		s, err := client.CreateSession(ctx, &v1.CreateSessionRequest{})
		if err != nil {
			return err
		}
		sess = s.GetMetadata().GetUid()
		fmt.Printf("session %s\n", sess)
	}
	cur, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: sess})
	if err != nil {
		return err
	}
	stream, err := client.Exec(ctx, &v1.ExecRequest{
		Session:         sess,
		Inputs:          []*v1.Message{wire.MessageToProto(api.TextMessage("user", input))},
		ExpectedLastSeq: cur.GetLastSeq(),
	})
	if err != nil {
		return err
	}
	for {
		up, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if r := up.GetRecord(); r != nil {
			printRecord(r)
		}
	}
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess string
	var from int64
	fs.StringVar(&sess, "session", "", "session UID")
	fs.Int64Var(&from, "from", 1, "replay from this seq")
	_ = fs.Parse(args)
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	stream, err := client.Replay(context.Background(), &v1.ReplayRequest{Session: sess, FromSeq: from})
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		printRecord(r)
	}
}

func cmdFork(args []string) error {
	fs := flag.NewFlagSet("fork", flag.ExitOnError)
	cfg := commonFlags(fs)
	var sess string
	var at int64
	var count int
	fs.StringVar(&sess, "session", "", "parent session UID")
	fs.Int64Var(&at, "at", 0, "fork at this seq (0 = head)")
	fs.IntVar(&count, "count", 1, "number of children")
	_ = fs.Parse(args)
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	resp, err := client.Fork(context.Background(), &v1.ForkRequest{Session: sess, AtSeq: at, Count: int32(count)})
	if err != nil {
		return err
	}
	for _, ch := range resp.GetChildren() {
		fmt.Printf("child %s parent=%s fork_seq=%d\n", ch.GetMetadata().GetUid(), ch.GetParentUid(), ch.GetForkSeq())
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
	client, cleanup, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx := context.Background()
	var out *v1.Session
	if which == "suspend" {
		out, err = client.Suspend(ctx, &v1.SuspendRequest{Session: sess})
	} else {
		out, err = client.Resume(ctx, &v1.ResumeRequest{Session: sess})
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
