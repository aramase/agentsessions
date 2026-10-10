package session_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

func specExpecting(address, descriptorID string) *v1.HarnessSpec {
	spec := remoteSpec(address)
	spec.DescriptorId = descriptorID
	return spec
}

func headOf(t *testing.T, h *registryHost, uid string) int64 {
	t.Helper()
	head, err := h.store.Session(uid).Head()
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// A registration that expects one harness while another answers at its address runs no turn: Exec
// is FAILED_PRECONDITION and the session's history stays empty, including when Exec creates the
// session. A registration expecting the harness that answers runs normally.
func TestDescriptorIDMismatchRefusesTurn(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	registerHarness(t, h, "expects-other", specExpecting("127.0.0.1:9000", "expected-other-harness"))

	sess, err := createOn(ctx, h.sessions, "expects-other")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	err = execOn(ctx, h.sessions, sess, "")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec on a harness reporting another id: got %v, want FailedPrecondition", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, `"expected-other-harness"`) || !strings.Contains(msg, `"echo"`) {
		t.Fatalf("error %q does not name the expected and the reported id", msg)
	}
	if head := headOf(t, h, sess); head != 0 {
		t.Fatalf("a refused turn wrote %d event(s)", head)
	}
	if err := execOn(ctx, h.sessions, "", "expects-other"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec auto-create on a harness reporting another id: got %v, want FailedPrecondition", err)
	}

	registerHarness(t, h, "expects-echo", specExpecting("127.0.0.1:9000", "echo"))
	ok, err := createOn(ctx, h.sessions, "expects-echo")
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, h.sessions, ok, "one", 0); len(outs) != 1 || outs[0] != "echo:one" {
		t.Fatalf("turn on the expected harness: outputs %v", outs)
	}
}

// serveEcho runs echo over the Harness service on a loopback listener, the way an operator runs a
// harness out of band.
func serveEcho(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1.RegisterHarnessServer(srv, harnesswire.NewServer(echoagent.Harness{}))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// remoteFactory serves a remote placement with runtime/remote, the backend agentsessionsd's -harness
// flag uses, so the registry dials the address it stored.
func remoteFactory(_ string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
	if spec.GetRemote() == nil {
		return nil, nil, errNoSubstrate
	}
	b := remote.New(spec.GetRemote().GetAddress())
	return placement.New(b, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId())), func() { _ = b.Close() }, nil
}

// The same refusal against a harness actually running at the registered address, served through
// runtime/remote: the registration expects expected-other-harness, echo answers, and the turn is
// refused with nothing journaled. Registered with the id echo reports, the turn runs.
func TestDescriptorIDCheckedOnRemoteHarness(t *testing.T) {
	ctx := context.Background()
	addr := serveEcho(t)
	h := startRegistryHostWith(t, filepath.Join(t.TempDir(), "journal.db"), remoteFactory)

	registerHarness(t, h, "expects-other", specExpecting(addr, "expected-other-harness"))
	sess, err := createOn(ctx, h.sessions, "expects-other")
	if err != nil {
		t.Fatal(err)
	}
	if err := execOn(ctx, h.sessions, sess, ""); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec on echo registered as expected-other-harness: got %v, want FailedPrecondition", err)
	}
	if head := headOf(t, h, sess); head != 0 {
		t.Fatalf("a refused turn wrote %d event(s)", head)
	}

	registerHarness(t, h, "remote-echo", specExpecting(addr, "echo"))
	ok, err := createOn(ctx, h.sessions, "remote-echo")
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, h.sessions, ok, "one", 0); len(outs) != 1 || outs[0] != "echo:one" {
		t.Fatalf("turn on the expected remote harness: outputs %v", outs)
	}
}

// shiftingHarness is echo whose reported descriptor id the test can change, standing in for a
// different harness answering at the registered address. Its first Run fails, leaving an
// interrupted turn for Resume.
type shiftingHarness struct {
	echoagent.Harness
	id     *atomic.Value
	failed *atomic.Bool
}

func (h shiftingHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	d, err := h.Harness.Describe(ctx)
	d.ID = h.id.Load().(string)
	return d, err
}

func (h shiftingHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if h.failed.CompareAndSwap(false, true) {
		return errors.New("harness went away mid-turn")
	}
	return h.Harness.Run(ctx, s, sink)
}

// An existing session is held to the registered id on every path that sends it history: once
// another harness answers, Resume of its interrupted turn, a new Exec and Fork are all
// FAILED_PRECONDITION with nothing journaled and no child created. When the expected harness
// answers again, Resume re-drives the turn.
func TestDescriptorIDGatesResumeAndFork(t *testing.T) {
	ctx := context.Background()
	id := &atomic.Value{}
	id.Store("echo")
	harness := shiftingHarness{id: id, failed: &atomic.Bool{}}
	factory := func(_ string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
		b := local.New(harness)
		return placement.New(b, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId())), func() { _ = b.Close() }, nil
	}
	h := startRegistryHostWith(t, filepath.Join(t.TempDir(), "journal.db"), factory)
	const name = "shifting"
	registerHarness(t, h, name, specExpecting("127.0.0.1:9000", "echo"))

	sess, err := createOn(ctx, h.sessions, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := execOn(ctx, h.sessions, sess, ""); err == nil || status.Code(err) == codes.FailedPrecondition {
		t.Fatalf("the first turn should have run on echo and been interrupted, got %v", err)
	}
	head := headOf(t, h, sess)
	if head == 0 {
		t.Fatal("the first turn wrote nothing")
	}

	id.Store("impostor")
	if _, err := h.sessions.Resume(ctx, &v1.ResumeRequest{Session: sess}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resume on a harness reporting another id: got %v, want FailedPrecondition", err)
	}
	if err := execOn(ctx, h.sessions, sess, ""); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec on a harness reporting another id: got %v, want FailedPrecondition", err)
	}
	if _, err := h.sessions.Fork(ctx, &v1.ForkRequest{Session: sess}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("fork on a harness reporting another id: got %v, want FailedPrecondition", err)
	}
	if got := headOf(t, h, sess); got != head {
		t.Fatalf("a refused call wrote to the session: head %d -> %d", head, got)
	}
	if n := countSessionsOn(t, h.svc, name); n != 1 {
		t.Fatalf("%d sessions on %q after a refused fork, want 1", n, name)
	}

	id.Store("echo")
	if _, err := h.sessions.Resume(ctx, &v1.ResumeRequest{Session: sess}); err != nil {
		t.Fatalf("resume once the expected harness answers: %v", err)
	}
	if got := headOf(t, h, sess); got <= head {
		t.Fatal("resume did not re-drive the interrupted turn")
	}
}

// A factory that builds a Placer without the spec's descriptor id would run turns on whatever
// answers. RegisterHarness refuses it with nothing stored and the factory's resources released, and
// startup refuses it for a stored registration. A spec with no descriptor id needs no check.
func TestRegisterHarnessRefusesPlacerWithoutDescriptorCheck(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b := local.New(echoagent.Harness{})
	defer b.Close()
	newRegistry := func() *placement.Registry {
		registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(b, echoagent.Model)})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	var released atomic.Int32
	unchecked := func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		return placement.New(b, echoagent.Model), func() { released.Add(1) }, nil
	}

	reg, err := session.NewHarnessRegistry(store, newRegistry(), unchecked)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "h", Spec: specExpecting("127.0.0.1:9000", "echo")})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "WithDescriptorID") {
		t.Fatalf("register with a placer that skips the descriptor check: got %v, want Internal naming WithDescriptorID", err)
	}
	if got := released.Load(); got != 1 {
		t.Fatalf("release called %d times, want 1", got)
	}
	if _, err := store.Harness("h"); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatalf("a registration whose placer skips the descriptor check was stored: %v", err)
	}
	if _, err := reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "no-id", Spec: remoteSpec("127.0.0.1:9000")}); err != nil {
		t.Fatalf("register with no descriptor id: %v", err)
	}
	reg.Close()

	checked, err := session.NewHarnessRegistry(store, newRegistry(), echoFactory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checked.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "h", Spec: specExpecting("127.0.0.1:9000", "echo")}); err != nil {
		t.Fatal(err)
	}
	checked.Close()
	isolation, err := session.NewHarnessRegistry(store, newRegistry(), unchecked)
	if err != nil {
		t.Fatalf("startup must isolate a placer that skips the stored descriptor id: %v", err)
	}
	got, err := isolation.GetHarness(ctx, &v1.GetHarnessRequest{Name: "h"})
	if err != nil || !strings.Contains(got.GetUnservableReason(), "WithDescriptorID") {
		t.Fatalf("descriptor failure status = %v, %v", got, err)
	}
	if got := released.Load(); got != 3 {
		t.Fatalf("failed load must release immediately: got %d releases, want 3", got)
	}
	isolation.Close()
	if got := released.Load(); got != 4 { // no-id loaded successfully, released only on Close
		t.Fatalf("Close must release healthy Placer once: got %d, want 4", got)
	}
}

// versionedHarness is echo whose advertised Descriptor.Version the test can change, standing in for
// a new build answering at the registered address. Its first Run fails, leaving an interrupted turn.
type versionedHarness struct {
	echoagent.Harness
	version *atomic.Value
	failed  *atomic.Bool
}

func (h versionedHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	d, err := h.Harness.Describe(ctx)
	d.Version = h.version.Load().(string)
	return d, err
}

func (h versionedHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if h.failed.CompareAndSwap(false, true) {
		return errors.New("harness went away mid-turn")
	}
	return h.Harness.Run(ctx, s, sink)
}

// Retiring a registered harness does not touch recovery: an interrupted turn records the
// registration's name and the harness's version, and after retire and a restart Resume routes to
// that name and holds it to that version, refusing a changed version with FAILED_PRECONDITION and
// nothing journaled, and re-driving the turn once the recorded version answers again. The
// registry adds no version check of its own, and new sessions on the retired harness stay refused.
func TestRetiredHarnessResumesRecordedTurnAtRecordedVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	version := &atomic.Value{}
	version.Store("1")
	harness := versionedHarness{version: version, failed: &atomic.Bool{}}
	factory := func(_ string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
		b := local.New(harness)
		return placement.New(b, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId())), func() { _ = b.Close() }, nil
	}
	h := startRegistryHostWith(t, path, factory)
	const name = "versioned"
	registerHarness(t, h, name, specExpecting("127.0.0.1:9000", "echo"))
	sess, err := createOn(ctx, h.sessions, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := execOn(ctx, h.sessions, sess, ""); err == nil || status.Code(err) == codes.FailedPrecondition {
		t.Fatalf("the first turn should have run and been interrupted, got %v", err)
	}
	recs, err := h.store.Session(sess).Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var start *api.ExecutionStart
	for _, r := range recs {
		if r.Event.ExecutionStart != nil {
			start = r.Event.ExecutionStart
		}
	}
	if start == nil || start.Harness != name || start.HarnessVersion != "1" {
		t.Fatalf("EXECUTION_START = %+v, want harness %q at version 1", start, name)
	}

	if _, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
	h.stop()
	h = startRegistryHostWith(t, path, factory)
	head := headOf(t, h, sess)

	version.Store("2")
	_, err = h.sessions.Resume(ctx, &v1.ResumeRequest{Session: sess})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "version") {
		t.Fatalf("resume at another version: got %v, want FailedPrecondition naming the version", err)
	}
	if got := headOf(t, h, sess); got != head {
		t.Fatalf("a refused resume wrote to the session: head %d -> %d", head, got)
	}

	version.Store("1")
	if _, err := h.sessions.Resume(ctx, &v1.ResumeRequest{Session: sess}); err != nil {
		t.Fatalf("resume on the retired harness at the recorded version: %v", err)
	}
	if got := headOf(t, h, sess); got <= head {
		t.Fatal("resume did not re-drive the interrupted turn")
	}
	if _, err := createOn(ctx, h.sessions, name); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("new session on the retired harness: got %v, want FailedPrecondition", err)
	}
}
