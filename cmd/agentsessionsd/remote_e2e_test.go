package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/runtime/remote"
)

// harnessnodeServesUnix lists the platforms where cmd/harnessnode serves a unix socket; it mirrors
// the build constraint on cmd/harnessnode/listen_unix.go. Elsewhere listen_other.go refuses it.
var harnessnodeServesUnix = map[string]bool{
	"linux": true, "darwin": true, "freebsd": true, "netbsd": true, "openbsd": true, "dragonfly": true, "illumos": true,
}

// TestRemoteHarnessEndToEnd runs the real binaries: agentsessionsd with harnesses registered by
// address, each served by a separate cmd/harnessnode process, over TCP loopback and over a unix
// socket. It covers Create, Exec, an interrupted turn plus Resume, the REQUIRES_MEMORY_SNAPSHOT
// refusal on Exec, an outage reported as Unavailable, and the refusal on Resume when a
// REQUIRES_MEMORY_SNAPSHOT harness has taken over the address of an interrupted session.
func TestRemoteHarnessEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs agentsessionsd and harnessnode")
	}
	bin := buildBinaries(t)

	// A misconfigured -harness is refused before the journal is opened, so a refused start leaves
	// nothing on disk.
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "no model", args: []string{"-harness", "mine=127.0.0.1:1"}, want: "requires -model"},
		{name: "built-in name", args: []string{"-model", "m", "-harness", "echo=127.0.0.1:1"}, want: `harness "echo" is already served`},
		{name: "malformed address", args: []string{"-model", "m", "-harness", "mine=http://127.0.0.1:1"}, want: `unsupported scheme "http"`},
		{name: "unix with no path", args: []string{"-model", "m", "-harness", "mine=unix://"}, want: "has no socket path"},
		{name: "missing port", args: []string{"-model", "m", "-harness", "mine=127.0.0.1"}, want: "missing port"},
		{name: "dns resolver gRPC cannot parse", args: []string{"-model", "m", "-harness", "mine=dns://%/127.0.0.1:9000"}, want: "invalid URL escape"},
		{name: "dns resolver with an empty port", args: []string{"-model", "m", "-harness", "mine=dns://127.0.0.1:/localhost:9000"}, want: `resolver "127.0.0.1:"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			journalPath := filepath.Join(t.TempDir(), "journal.db")
			args := append([]string{"-addr", freeLoopbackAddr(t), "-journal", journalPath}, tt.args...)
			out, err := exec.Command(filepath.Join(bin, "agentsessionsd"), args...).CombinedOutput()
			if err == nil {
				t.Fatalf("agentsessionsd started with %v:\n%s", tt.args, out)
			}
			if !strings.Contains(string(out), tt.want) {
				t.Fatalf("output does not say %q:\n%s", tt.want, out)
			}
			if _, err := os.Stat(journalPath); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("a refused start left a journal at %s (stat: %v)", journalPath, err)
			}
		})
	}

	for _, transport := range []string{"tcp", "unix"} {
		t.Run(transport, func(t *testing.T) {
			if transport == "unix" && !harnessnodeServesUnix[runtime.GOOS] {
				t.Skipf("harnessnode does not serve unix sockets on %s", runtime.GOOS)
			}
			addrFor := func(name string) string {
				if transport == "unix" {
					return "unix://" + filepath.Join(shortTempDir(t), name+".sock")
				}
				return freeLoopbackAddr(t)
			}
			mineAddr, ctrAddr := addrFor("mine"), addrFor("ctr")

			model := newFakeModel(t)
			mine := startHarnessNode(t, bin, "echo", mineAddr)
			startHarnessNode(t, bin, "counter", ctrAddr)

			serverAddr := freeLoopbackAddr(t)
			startServer(t, bin, serverAddr,
				"-journal", filepath.Join(t.TempDir(), "journal.db"),
				"-model", "fake-model",
				"-model-base-url", model.URL,
				"-harness", "mine="+mineAddr,
				"-harness", "ctr="+ctrAddr,
			)
			c, err := client.Dial(serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Create and Exec.
			sess, err := c.CreateSession(ctx, &v1.Session{Harness: "mine"})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			uid := sess.GetMetadata().GetUid()
			turn, err := c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{"hello"}})
			if err != nil {
				t.Fatalf("exec: %v", err)
			}
			if turn.Output != "model:hello" {
				t.Fatalf("exec output = %q, want model:hello", turn.Output)
			}

			// The harness process dies mid-turn, after its model call was recorded. Once it is
			// back, Resume re-drives the turn to completion from the journal without calling the
			// model again.
			interrupt(t, ctx, c, model, mine, uid, "crash")
			mine = startHarnessNode(t, bin, "echo", mineAddr)
			calls := model.calls.Load()
			if err := retryWhileUnavailable(ctx, func() error { _, err := c.Resume(ctx, uid, false); return err }); err != nil {
				t.Fatalf("resume: %v", err)
			}
			kinds, outputs := journal(t, ctx, c, uid)
			if got := outputs[len(outputs)-1]; got != "model:crash" {
				t.Fatalf("output after resume = %q, want model:crash (journal %v)", got, kinds)
			}
			if got := kinds[len(kinds)-2:]; got[0] != "END" || got[1] != "LIFECYCLE" {
				t.Fatalf("resume did not complete the turn and record RESUME: journal %v", kinds)
			}
			if n := model.calls.Load(); n != calls {
				t.Fatalf("resume called the model %d more time(s); the recorded completion should be served", n-calls)
			}
			if turn, err := c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{"after"}}); err != nil || turn.Output != "model:after" {
				t.Fatalf("exec after resume = %+v, %v; want model:after", turn, err)
			}

			// A REQUIRES_MEMORY_SNAPSHOT harness is refused before anything is journaled.
			refused, err := c.CreateSession(ctx, &v1.Session{Harness: "ctr"})
			if err != nil {
				t.Fatalf("create on ctr: %v", err)
			}
			_, err = c.Exec(ctx, client.ExecOptions{Session: refused.GetMetadata().GetUid(), Inputs: []string{"hello"}})
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "REQUIRES_MEMORY_SNAPSHOT") {
				t.Fatalf("exec on counter harness = %v, want FailedPrecondition naming REQUIRES_MEMORY_SNAPSHOT", err)
			}
			if kinds, _ := journal(t, ctx, c, refused.GetMetadata().GetUid()); len(kinds) != 0 {
				t.Fatalf("a refused exec journaled %v", kinds)
			}

			// Interrupt a turn, take the harness down, and bring a REQUIRES_MEMORY_SNAPSHOT
			// harness up at the same address. While nothing answers, Resume is Unavailable; once
			// the counter answers, Resume is refused rather than replaying the turn into it.
			interrupt(t, ctx, c, model, mine, uid, "crash-again")
			before, _ := journal(t, ctx, c, uid)
			_, err = c.Resume(ctx, uid, false)
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), strings.TrimPrefix(mineAddr, "unix://")) {
				t.Fatalf("resume with no harness at the address = %v, want Unavailable naming %s", err, mineAddr)
			}
			startHarnessNode(t, bin, "counter", mineAddr)
			err = retryWhileUnavailable(ctx, func() error { _, err := c.Resume(ctx, uid, false); return err })
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "REQUIRES_MEMORY_SNAPSHOT") {
				t.Fatalf("resume into a swapped-in counter harness = %v, want FailedPrecondition", err)
			}
			if after, _ := journal(t, ctx, c, uid); len(after) != len(before) {
				t.Fatalf("a refused resume wrote to the journal: %v -> %v", before, after)
			}
		})
	}
}

// buildBinaries builds agentsessionsd and harnessnode from this module into a temp dir.
func buildBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, pkg := range []string{"../agentsessionsd", "../harnessnode"} {
		cmd := exec.Command("go", "build", "-o", dir, pkg)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", pkg, err, out)
		}
	}
	return dir
}

type process struct {
	cmd    *exec.Cmd
	out    *bytes.Buffer
	exited chan struct{}
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
}

// kill stops the process without letting it clean up, the way a crash would. A unix socket it was
// serving is deliberately left behind: the next harnessnode on the address has to reclaim it, as it
// would after a real crash.
func (p *process) kill(t *testing.T) {
	t.Helper()
	_ = p.cmd.Process.Kill()
	<-p.exited
}

func start(t *testing.T, path string, env []string, args ...string) *process {
	t.Helper()
	p := &process{cmd: exec.Command(path, args...), out: &bytes.Buffer{}, exited: make(chan struct{})}
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", path, err)
	}
	go func() { _ = p.cmd.Wait(); close(p.exited) }()
	t.Cleanup(func() {
		p.stop(t)
		if t.Failed() {
			t.Logf("%s output:\n%s", filepath.Base(path), p.out)
		}
	})
	return p
}

// startHarnessNode runs cmd/harnessnode serving kind at addr and waits until it answers Describe.
func startHarnessNode(t *testing.T, bin, kind, addr string) *process {
	t.Helper()
	p := start(t, filepath.Join(bin, "harnessnode"),
		[]string{"HARNESS_KIND=" + kind, "HARNESS_ADDR=" + addr, "HARNESS_READYZ=127.0.0.1:0"})
	probe := remote.New(addr)
	defer func() { _ = probe.Close() }()
	waitFor(t, p, "harnessnode "+kind+" at "+addr, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := probe.Describe(ctx)
		return err == nil
	})
	return p
}

// startServer runs agentsessionsd on addr and waits until it serves the Sessions API.
func startServer(t *testing.T, bin, addr string, args ...string) *process {
	t.Helper()
	p := start(t, filepath.Join(bin, "agentsessionsd"), nil, append([]string{"-addr", addr}, args...)...)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	waitFor(t, p, "agentsessionsd at "+addr, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.ListSessions(ctx, "")
		return err == nil
	})
	return p
}

func waitFor(t *testing.T, p *process, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !ready() {
		select {
		case <-p.exited:
			t.Fatalf("%s exited before it was ready:\n%s", what, p.out)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never became ready:\n%s", what, p.out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// interrupt kills the harness process while its model call is in flight, then lets the call
// finish. The host records the completion but the harness is gone, so the turn ends in ERROR with
// no END: an interrupted turn that Resume can re-drive from the journal.
func interrupt(t *testing.T, ctx context.Context, c *client.Client, model *fakeModel, harness *process, uid, input string) {
	t.Helper()
	model.gate.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{input}})
		done <- err
	}()
	select {
	case <-model.arrived:
	case <-time.After(30 * time.Second):
		t.Fatal("the turn never reached the model")
	}
	harness.kill(t)
	model.gate.Store(false)
	model.release <- struct{}{}
	// Lost after admission, so not UNAVAILABLE: the turn was journaled and is left for Resume.
	if err := <-done; status.Code(err) != codes.Internal {
		t.Fatalf("exec %q with its harness killed mid-turn = %v, want Internal", input, err)
	}
	kinds, _ := journal(t, ctx, c, uid)
	if len(kinds) == 0 || kinds[len(kinds)-1] != "ERROR" {
		t.Fatalf("interrupted turn did not end in ERROR: journal %v", kinds)
	}
}

func retryWhileUnavailable(ctx context.Context, call func() error) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := call()
		if status.Code(err) != codes.Unavailable || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// journal returns the event kinds and OUTPUT texts committed on a session.
func journal(t *testing.T, ctx context.Context, c *client.Client, uid string) (kinds, outputs []string) {
	t.Helper()
	records, err := c.Replay(ctx, uid, 0, 0)
	if err != nil {
		t.Fatalf("replay %s: %v", uid, err)
	}
	for _, r := range records {
		ev := r.GetEvent()
		kinds = append(kinds, strings.TrimPrefix(ev.GetKind().String(), "EVENT_"))
		if ev.GetKind() == v1.EventKind_EVENT_OUTPUT {
			var text string
			for _, p := range ev.GetMessage().GetParts() {
				text += p.GetText().GetText()
			}
			outputs = append(outputs, text)
		}
	}
	return kinds, outputs
}

// fakeModel is an OpenAI-compatible endpoint that answers "model:<last message>". While gate is
// set, a request signals arrived and waits for release before answering, so a test can act while a
// model call is in flight.
type fakeModel struct {
	*httptest.Server
	gate    atomic.Bool
	calls   atomic.Int32
	arrived chan struct{}
	release chan struct{}
}

func newFakeModel(t *testing.T) *fakeModel {
	t.Helper()
	m := &fakeModel{arrived: make(chan struct{}), release: make(chan struct{})}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.calls.Add(1)
		var req struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if m.gate.Load() {
			m.arrived <- struct{}{}
			<-m.release
		}
		last := ""
		if n := len(req.Messages); n > 0 {
			last = req.Messages[n-1].Content
		}
		text, _ := json.Marshal("model:" + last)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%s}}]}\n\ndata: [DONE]\n\n", text)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"choices\":[{\"message\":{\"content\":%s}}]}", text)
	}))
	t.Cleanup(m.Close)
	return m
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().String()
}

// shortTempDir keeps unix socket paths under the platform limit (104 bytes on macOS), which
// t.TempDir's long per-test path can exceed.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "as")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
