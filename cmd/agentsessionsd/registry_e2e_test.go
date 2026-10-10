package main

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionalpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
)

func registryClient(t *testing.T, addr string) v1.HarnessRegistryClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return v1.NewHarnessRegistryClient(conn)
}

// Shutdown must drain in-flight Sessions calls before releasing the model/backend/store, even
// while the separate operator listener is active. Abrupt Stop loses the in-flight turn.
func TestOperatorShutdownDrainsInflightSession(t *testing.T) {
	if testing.Short() {
		t.Skip("builds agentsessionsd")
	}
	bin := buildBinaries(t)
	model := newFakeModel(t)
	model.gate.Store(true)
	addr, op := freeLoopbackAddr(t), freeLoopbackAddr(t)
	p := startServer(t, bin, addr, "-registry-addr", op, "-journal", filepath.Join(t.TempDir(), "journal.db"), "-model", "fake-model", "-model-base-url", model.URL)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	type result struct {
		output string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		turn, err := c.Exec(ctx, client.ExecOptions{Harness: "chat", Inputs: []string{"pending"}})
		if err != nil {
			done <- result{err: err}
		} else {
			done <- result{output: turn.Output}
		}
	}()
	select {
	case <-model.arrived:
	case <-ctx.Done():
		t.Fatal("turn did not reach model")
	}
	released := false
	defer func() {
		if !released {
			model.gate.Store(false)
			select {
			case model.release <- struct{}{}:
			case <-time.After(time.Second):
			}
		}
	}()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		t.Fatalf("SIGTERM aborted in-flight turn: %+v", r)
	case <-time.After(250 * time.Millisecond):
	}
	model.gate.Store(false)
	model.release <- struct{}{}
	released = true
	select {
	case r := <-done:
		if r.err != nil || r.output != "model:pending" {
			t.Fatalf("drained turn = %+v", r)
		}
	case <-ctx.Done():
		t.Fatal("turn never drained")
	}
	select {
	case <-p.exited:
	case <-ctx.Done():
		t.Fatal("daemon never exited after draining")
	}
}

func TestOperatorListenerIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("builds agentsessionsd")
	}
	bin := buildBinaries(t)
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			addr, op := freeLoopbackAddr(t), freeLoopbackAddr(t)
			args := []string{"-journal", filepath.Join(t.TempDir(), "journal.db")}
			if enabled {
				args = append(args, "-registry-addr", op)
			}
			p := startServer(t, bin, addr, args...)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			reg := registryClient(t, addr)
			if _, err := reg.ListHarnesses(ctx, &v1.ListHarnessesRequest{}); status.Code(err) != codes.Unimplemented {
				t.Fatalf("registry on Sessions port = %v", err)
			}
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for _, req := range []*reflectionv1.ServerReflectionRequest{
				{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{ListServices: ""}},
				{MessageRequest: &reflectionv1.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: "agentsessions.v1.HarnessRegistry"}},
			} {
				stream, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_ = stream.Send(req)
				_, err = stream.Recv()
				if status.Code(err) != codes.Unimplemented {
					t.Fatalf("reflection v1 on Sessions = %v", err)
				}
			}
			for _, req := range []*reflectionalpha.ServerReflectionRequest{ //nolint:staticcheck // Probe deprecated v1alpha reflection to verify Sessions refuses it.
				{MessageRequest: &reflectionalpha.ServerReflectionRequest_ListServices{ListServices: ""}},
				{MessageRequest: &reflectionalpha.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: "agentsessions.v1.HarnessRegistry"}},
			} {
				stream, err := reflectionalpha.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_ = stream.Send(req)
				_, err = stream.Recv()
				if status.Code(err) != codes.Unimplemented {
					t.Fatalf("reflection v1alpha on Sessions = %v", err)
				}
			}
			if enabled {
				r := registryClient(t, op)
				if _, err := r.ListHarnesses(ctx, &v1.ListHarnessesRequest{}); err != nil {
					t.Fatalf("operator registry: %v", err)
				}
				c, err := client.Dial(op)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				if _, err := c.ListSessions(ctx, ""); status.Code(err) != codes.Unimplemented {
					t.Fatalf("Sessions on operator port = %v", err)
				}
			} else {
				// An off-by-default operator port should remain available for another process to bind.
				lis, err := net.Listen("tcp", op)
				if err != nil {
					t.Fatalf("disabled operator bound port: %v", err)
				}
				lis.Close()
			}
			p.stop(t)
			if strings.Contains(p.out.String(), "operator registry listening") != enabled {
				t.Fatalf("operator startup log mismatch: %s", p.out)
			}
			lis, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("Sessions port remained bound after stop: %v", err)
			}
			lis.Close()
			if enabled {
				lis, err = net.Listen("tcp", op)
				if err != nil {
					t.Fatalf("operator port remained bound after stop: %v", err)
				}
				lis.Close()
			}
		})
	}
}

func TestOperatorMisconfigurationBeforeJournal(t *testing.T) {
	if testing.Short() {
		t.Skip("builds agentsessionsd")
	}
	bin := buildBinaries(t)
	for _, tc := range []struct{ name, sessions, operator, want string }{
		{"wildcard", "127.0.0.1:0", "0.0.0.0:8081", "literal loopback"},
		{"hostname", "127.0.0.1:0", "localhost:8081", "literal loopback"},
		{"nonloopback", "127.0.0.1:0", "192.168.0.1:8081", "literal loopback"},
		{"same port", "127.0.0.1:8100", "127.0.0.1:8100", "cannot share TCP port"},
		{"sessions wildcard alias", ":8100", "127.0.0.1:8100", "cannot share TCP port"},
		{"sessions IPv6 wildcard alias", "[::]:8100", "127.0.0.1:8100", "cannot share TCP port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "refused.db")
			out, err := exec.Command(filepath.Join(bin, "agentsessionsd"), "-journal", path, "-addr", tc.sessions, "-registry-addr", tc.operator).CombinedOutput()
			if err == nil || !strings.Contains(string(out), tc.want) {
				t.Fatalf("startup = %v\n%s; want %s", err, out, tc.want)
			}
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("startup wrote journal: %v", err)
			}
		})
	}
	// The second bind fails before the first Serve can begin; neither port remains bound.
	addr := freeLoopbackAddr(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	out, err := exec.Command(filepath.Join(bin, "agentsessionsd"), "-journal", filepath.Join(t.TempDir(), "bind.db"), "-addr", addr, "-registry-addr", occupied.Addr().String()).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "listen on registry") {
		t.Fatalf("occupied operator bind = %v\n%s", err, out)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Sessions listener leaked on operator bind error: %v", err)
	}
	lis.Close()
}

func TestOperatorRegistryWithoutModelCanInspectButNotRegisterRemote(t *testing.T) {
	if testing.Short() {
		t.Skip("builds agentsessionsd")
	}
	bin := buildBinaries(t)
	addr, op := freeLoopbackAddr(t), freeLoopbackAddr(t)
	startServer(t, bin, addr, "-journal", filepath.Join(t.TempDir(), "journal.db"), "-registry-addr", op)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r := registryClient(t, op)
	if _, err := r.ListHarnesses(ctx, &v1.ListHarnessesRequest{}); err != nil {
		t.Fatalf("inspect without model: %v", err)
	}
	spec := &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "127.0.0.1:1"}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}
	if _, err := r.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "no-model", Spec: spec}); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "requires -model") {
		t.Fatalf("remote without model = %v", err)
	}
	if _, err := r.GetHarness(ctx, &v1.GetHarnessRequest{Name: "no-model"}); status.Code(err) != codes.NotFound {
		t.Fatalf("refused row stored: %v", err)
	}
}

func TestOperatorRegistryLifecycleRealBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("builds agentsessionsd, harnessnode and agentctl")
	}
	bin := buildBinaries(t)
	harnessAddr := freeLoopbackAddr(t)
	startHarnessNode(t, bin, "echo", harnessAddr)
	model := newFakeModel(t)
	sessionsAddr, operatorAddr := freeLoopbackAddr(t), freeLoopbackAddr(t)
	journalPath := filepath.Join(t.TempDir(), "journal.db")
	daemonArgs := []string{"-journal", journalPath, "-model", "fake-model", "-model-base-url", model.URL}
	daemon := startServer(t, bin, sessionsAddr, append(daemonArgs, "-registry-addr", operatorAddr)...)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	c, err := client.Dial(sessionsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reg := registryClient(t, operatorAddr)
	for _, tc := range []struct {
		name string
		spec *v1.HarnessSpec
		want string
	}{
		{"unsupported-substrate", &v1.HarnessSpec{Placement: &v1.HarnessSpec_Substrate{Substrate: &v1.SubstratePlacement{Atespace: "space", Template: "template"}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}, "substrate placement"},
		{"malformed-remote", &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "http://host:123"}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}, "unsupported scheme"},
	} {
		_, err := reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: tc.name, Spec: tc.spec})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("register %s = %v, want FailedPrecondition naming %s", tc.name, err, tc.want)
		}
		if _, err := reg.GetHarness(ctx, &v1.GetHarnessRequest{Name: tc.name}); status.Code(err) != codes.NotFound {
			t.Fatalf("refused registration %s stored a row: %v", tc.name, err)
		}
	}
	// A remote address need not answer at registration: the backend dials only on observation
	// or placement. An unavailable harness must not prevent unrelated registrations.
	unavailable := &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "127.0.0.1:1"}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}
	if _, err := reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "down", Spec: unavailable}); err != nil {
		t.Fatalf("offline remote registration dialed during construction: %v", err)
	}
	if observed, err := reg.GetHarness(ctx, &v1.GetHarnessRequest{Name: "down", Observe: true}); err != nil || observed.GetObserveError() == "" {
		t.Fatalf("offline remote observation = %v, %v", observed, err)
	}
	spec := &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: harnessAddr}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}, DescriptorId: "echo"}
	specJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(specPath, specJSON, 0600); err != nil {
		t.Fatal(err)
	}
	cli := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(filepath.Join(bin, "agentctl"), append([]string{"harness"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("agentctl harness %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	register := cli("register", "--server", operatorAddr, "--name", "mine", "--spec", specPath)
	for _, want := range []string{`"outcome":"REGISTER_OUTCOME_CREATED"`, `"spec_digest":"sha256:`, `"state":"HARNESS_STATE_ACTIVE"`, `"source":"HARNESS_SOURCE_REGISTERED"`} {
		if !strings.Contains(register, want) {
			t.Fatalf("register output missing %q: %s", want, register)
		}
	}
	got := cli("get", "--server", operatorAddr, "--name", "mine", "--observe")
	for _, want := range []string{`"spec_digest":"sha256:`, `"observed":`, `"descriptor_id":"echo"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("get output missing %q: %s", want, got)
		}
	}
	listed := cli("list", "--server", operatorAddr, "--page-size", "1")
	for _, want := range []string{`"name":"mine"`, `"name":"echo"`} {
		if !strings.Contains(listed, want) {
			t.Fatalf("paged list missing %s: %s", want, listed)
		}
	}
	if strings.Contains(listed, "next_page_token") {
		t.Fatalf("CLI returned incomplete pages: %s", listed)
	}
	// A positive page size larger than int32 must retain the server's documented >500 clamp,
	// not wrap negative on the CLI's int32 request boundary.
	wide := cli("list", "--server", operatorAddr, "--page-size", "2147483648")
	for _, want := range []string{`"name":"chat"`, `"name":"down"`, `"name":"echo"`, `"name":"mine"`} {
		if !strings.Contains(wide, want) {
			t.Fatalf("large page-size list missing %s: %s", want, wide)
		}
	}
	if strings.Contains(wide, "next_page_token") {
		t.Fatalf("large page-size list incomplete: %s", wide)
	}
	for _, args := range [][]string{
		{"get", "--server", operatorAddr, "--name", "missing"},
		{"retire", "--server", operatorAddr, "--name", "missing"},
		{"register", "--server", operatorAddr, "--name", "echo", "--spec", specPath},
	} {
		out, err := exec.Command(filepath.Join(bin, "agentctl"), append([]string{"harness"}, args...)...).CombinedOutput()
		want := "NotFound"
		if args[0] == "register" {
			want = "AlreadyExists"
		}
		if err == nil || !strings.Contains(string(out), want) {
			t.Fatalf("CLI %v = %v %s; want %s", args, err, out, want)
		}
	}
	sess, err := c.CreateSession(ctx, &v1.Session{Harness: "mine"})
	if err != nil {
		t.Fatal(err)
	}
	uid := sess.GetMetadata().GetUid()
	turn, err := c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{"hello"}})
	if err != nil || turn.Output != "model:hello" {
		t.Fatalf("Exec = %+v, %v", turn, err)
	}
	beforeKinds, beforeOutputs := journal(t, ctx, c, uid)
	calls := model.calls.Load()
	wrongID := &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: harnessAddr}}, Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}, DescriptorId: "not-echo"}
	if _, err := reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "wrong-id", Spec: wrongID}); err != nil {
		t.Fatalf("register descriptor without dialing: %v", err)
	}
	wrongSession, err := c.CreateSession(ctx, &v1.Session{Harness: "wrong-id"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, client.ExecOptions{Session: wrongSession.GetMetadata().GetUid(), Inputs: []string{"fail"}}); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "descriptor") {
		t.Fatalf("mismatched descriptor = %v", err)
	}
	if kinds, _ := journal(t, ctx, c, wrongSession.GetMetadata().GetUid()); len(kinds) != 0 {
		t.Fatalf("mismatched descriptor journaled: %v", kinds)
	}
	retired := cli("retire", "--server", operatorAddr, "--name", "mine", "--reason", "maintenance")
	for _, want := range []string{`"state":"HARNESS_STATE_RETIRED"`, `"retire_reason":"maintenance"`} {
		if !strings.Contains(retired, want) {
			t.Fatalf("retire missing %q: %s", want, retired)
		}
	}
	if s, err := c.CreateSession(ctx, &v1.Session{Harness: "mine"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Create retired = %v %v", s, err)
	}
	if turn, err := c.Exec(ctx, client.ExecOptions{Harness: "mine", Inputs: []string{"new"}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("auto-create Exec retired = %v %v", turn, err)
	}
	afterKinds, afterOutputs := journal(t, ctx, c, uid)
	if !reflect.DeepEqual(beforeKinds, afterKinds) || !reflect.DeepEqual(beforeOutputs, afterOutputs) || model.calls.Load() != calls {
		t.Fatalf("Replay changed old session or called model: before=%v %v after=%v %v calls=%d -> %d", beforeKinds, beforeOutputs, afterKinds, afterOutputs, calls, model.calls.Load())
	}
	listed = cli("list", "--server", operatorAddr, "--include-retired", "--page-size", "1")
	if !strings.Contains(listed, `"state":"HARNESS_STATE_RETIRED"`) {
		t.Fatalf("retired row missing: %s", listed)
	}
	daemon.stop(t)
	daemon = startServer(t, bin, sessionsAddr, daemonArgs...)
	if turn, err := c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{"after-restart"}}); err != nil || turn.Output != "model:after-restart" {
		t.Fatalf("existing retired session after restart = %+v %v", turn, err)
	}
	if _, err := c.CreateSession(ctx, &v1.Session{Harness: "mine"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired new session after restart = %v", err)
	}
	daemon.stop(t)
	if strings.Contains(daemon.out.String(), "operator registry listening") {
		t.Fatalf("off-by-default listener started: %s", daemon.out)
	}
}
