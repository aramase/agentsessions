package session_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// registryHost is one host process: a store file, two static echo harnesses ("echo" and "echo2"),
// a reserved built-in name ("chat"), and every registered harness on its own local echo backend.
// Starting a second one on the same path is what a restart looks like.
type registryHost struct {
	store     *sqlitelog.Store
	svc       *session.Service
	reg       *session.HarnessRegistry
	sessions  v1.SessionsClient
	harnesses v1.HarnessRegistryClient
	stop      func()
}

// errNoSubstrate stands in for a host built without a substrate placement backend.
var errNoSubstrate = errors.New("this host serves remote placement only")

func startRegistryHost(t *testing.T, path string) *registryHost {
	t.Helper()
	return startRegistryHostWith(t, path, echoFactory)
}

// echoFactory serves every remote placement on its own local echo backend, expecting the spec's
// descriptor id as a PlacerFactory must, and stands in for a host with no substrate backend.
func echoFactory(_ string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
	if spec.GetSubstrate() != nil {
		return nil, nil, errNoSubstrate
	}
	b := local.New(echoagent.Harness{})
	return placement.New(b, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId())), func() { _ = b.Close() }, nil
}

// startRegistryHostWith is startRegistryHost with the host's own PlacerFactory.
func startRegistryHostWith(t *testing.T, path string, factory session.PlacerFactory) *registryHost {
	t.Helper()
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	echo, echo2 := local.New(echoagent.Harness{}), local.New(echoagent.Harness{})
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo":  placement.New(echo, echoagent.Model),
		"echo2": placement.New(echo2, echoagent.Model),
	}, placement.WithReservedNames("chat"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := session.NewHarnessRegistry(store, registry, factory)
	if err != nil {
		_ = echo.Close()
		_ = echo2.Close()
		store.Close()
		t.Fatal(err)
	}
	svc := session.NewService(store, registry)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, svc)
	v1.RegisterHarnessRegistryServer(srv, reg)
	go srv.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			conn.Close()
			srv.Stop()
			reg.Close()
			_ = echo.Close()
			_ = echo2.Close()
			store.Close()
		})
	}
	t.Cleanup(stop)
	return &registryHost{
		store:     store,
		svc:       svc,
		reg:       reg,
		sessions:  v1.NewSessionsClient(conn),
		harnesses: v1.NewHarnessRegistryClient(conn),
		stop:      stop,
	}
}

// serveSessions serves svc in-process and returns a client for it.
func serveSessions(t *testing.T, svc v1.SessionsServer) v1.SessionsClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	v1.RegisterSessionsServer(srv, svc)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return v1.NewSessionsClient(conn)
}

func remoteSpec(address string) *v1.HarnessSpec {
	return &v1.HarnessSpec{
		Placement:    &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: address}},
		Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY},
	}
}

func registerHarness(t *testing.T, h *registryHost, name string, spec *v1.HarnessSpec) *v1.RegisterHarnessResponse {
	t.Helper()
	resp, err := h.harnesses.RegisterHarness(context.Background(), &v1.RegisterHarnessRequest{Name: name, Spec: spec})
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return resp
}

func createOn(ctx context.Context, c v1.SessionsClient, harness string) (string, error) {
	s, err := c.CreateSession(ctx, &v1.CreateSessionRequest{Session: &v1.Session{Harness: harness}})
	return s.GetMetadata().GetUid(), err
}

func execOn(ctx context.Context, c v1.SessionsClient, sess, harness string) error {
	stream, err := c.Exec(ctx, &v1.ExecRequest{
		Session: sess,
		Harness: harness,
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "x"))},
	})
	if err != nil {
		return err
	}
	return drainExec(stream)
}

// TestRetiredHarnessRefusesNewSessionsOnly is the lifecycle: register a harness and run a session
// on it, retire it, restart the host, and check that a new session is refused while the existing
// one resumes, runs a turn and forks. Registering the same spec again reactivates it.
func TestRetiredHarnessRefusesNewSessionsOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	const name = "suite-agent-0a1b2c3d"
	h := startRegistryHost(t, path)

	reg := registerHarness(t, h, name, remoteSpec("127.0.0.1:9000"))
	if reg.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_CREATED || reg.GetHarness().GetSource() != v1.HarnessSource_HARNESS_SOURCE_REGISTERED {
		t.Fatalf("first register = %v", reg)
	}
	uid := reg.GetHarness().GetMetadata().GetUid()
	again := registerHarness(t, h, name, remoteSpec("127.0.0.1:9000"))
	if again.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_UNCHANGED || again.GetHarness().GetMetadata().GetUid() != uid {
		t.Fatalf("repeat register = %v; want UNCHANGED with uid %s", again, uid)
	}
	if _, err := h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: name, Spec: remoteSpec("127.0.0.1:9001")}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("different spec under the same name: want AlreadyExists, got %v", err)
	}

	sess, err := createOn(ctx, h.sessions, name)
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, h.sessions, sess, "one", 0); len(outs) != 1 || outs[0] != "echo:one" {
		t.Fatalf("turn 1 outputs = %v", outs)
	}
	if _, err := h.sessions.Suspend(ctx, &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatal(err)
	}
	retired, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: name, Reason: "superseded"})
	if err != nil || retired.GetState() != v1.HarnessState_HARNESS_STATE_RETIRED || retired.GetRetireTime() == nil {
		t.Fatalf("retire = %v, %v", retired, err)
	}

	// Restart: the registration and its state come back from the store.
	h.stop()
	h = startRegistryHost(t, path)

	got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: name})
	if err != nil || got.GetState() != v1.HarnessState_HARNESS_STATE_RETIRED || got.GetRetireReason() != "superseded" || got.GetMetadata().GetUid() != uid {
		t.Fatalf("after restart: %v, %v", got, err)
	}
	if _, err := createOn(ctx, h.sessions, name); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("new session on a retired harness: want FailedPrecondition, got %v", err)
	}
	if err := execOn(ctx, h.sessions, "", name); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec auto-create on a retired harness: want FailedPrecondition, got %v", err)
	}

	// The existing session resumes, runs a turn and forks.
	if r, err := h.sessions.Resume(ctx, &v1.ResumeRequest{Session: sess}); err != nil || r.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
		t.Fatalf("resume existing session: %v, %v", r.GetComputeState(), err)
	}
	cur, err := h.sessions.GetSession(ctx, &v1.GetSessionRequest{Uid: sess})
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, h.sessions, sess, "two", cur.GetLastSeq()); len(outs) != 1 || outs[0] != "echo:two" {
		t.Fatalf("turn 2 outputs = %v", outs)
	}
	fr, err := h.sessions.Fork(ctx, &v1.ForkRequest{Session: sess})
	if err != nil {
		t.Fatalf("fork existing session: %v", err)
	}
	child := fr.GetChildren()[0]
	if child.GetHarness() != name {
		t.Fatalf("fork child harness = %q, want %q", child.GetHarness(), name)
	}
	if err := execOn(ctx, h.sessions, child.GetMetadata().GetUid(), ""); err != nil {
		t.Fatalf("turn on the fork child of a retired harness: %v", err)
	}

	// Re-registering the identical spec reactivates it, under the same uid.
	re := registerHarness(t, h, name, remoteSpec("127.0.0.1:9000"))
	if re.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_REACTIVATED || re.GetHarness().GetMetadata().GetUid() != uid || re.GetHarness().GetRetireTime() != nil {
		t.Fatalf("re-register = %v; want REACTIVATED with uid %s and no retire time", re, uid)
	}
	if _, err := createOn(ctx, h.sessions, name); err != nil {
		t.Fatalf("new session after reactivation: %v", err)
	}
}

// TestRetireHarnessRacesCreate runs creates on a harness while it is retired. Once RetireHarness
// has returned no session may land on the harness, so the count of its sessions read right then
// must be the final count, and every create that reported success must be one of them. Run it
// with -race -count=20.
func TestRetireHarnessRacesCreate(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	const name = "racer"
	registerHarness(t, h, name, remoteSpec("127.0.0.1:9000"))

	const creators = 8
	var (
		created, refused atomic.Int64
		retiredDone      atomic.Bool
		wg               sync.WaitGroup
		unexpected       = make(chan error, creators)
	)
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Keep creating until this goroutine has been refused a few times after the retire
			// returned, so creates both straddle the retire and follow it.
			for after := 0; after < 3; {
				if retiredDone.Load() {
					after++
				}
				_, err := h.svc.CreateSession(ctx, &v1.CreateSessionRequest{Session: &v1.Session{Harness: name}})
				switch status.Code(err) {
				case codes.OK:
					created.Add(1)
				case codes.FailedPrecondition:
					refused.Add(1)
				default:
					unexpected <- err
					return
				}
			}
		}()
	}
	// Let the creators get going so the retire lands among them.
	for created.Load() < creators && len(unexpected) == 0 {
		runtime.Gosched()
	}
	if _, err := h.reg.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
	atRetire := countSessionsOn(t, h.svc, name)
	retiredDone.Store(true)
	wg.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Fatalf("create during retire: %v", err)
	}

	final := countSessionsOn(t, h.svc, name)
	if final != atRetire {
		t.Fatalf("%d sessions landed on %q after RetireHarness returned", final-atRetire, name)
	}
	if int64(final) != created.Load() {
		t.Fatalf("store has %d sessions on %q, creates reported %d successes", final, name, created.Load())
	}
	if refused.Load() < creators*3 {
		t.Fatalf("only %d creates were refused; the retire did not take effect", refused.Load())
	}
}

func countSessionsOn(t *testing.T, svc *session.Service, harness string) int {
	t.Helper()
	n, token := 0, ""
	for {
		page, err := svc.ListSessions(context.Background(), &v1.ListSessionsRequest{PageSize: int32(sqlitelog.MaxPageSize), PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range page.GetSessions() {
			if s.GetHarness() == harness {
				n++
			}
		}
		if token = page.GetNextPageToken(); token == "" {
			return n
		}
	}
}

// A session on a registered harness cannot run a turn on another harness, and a session on a static
// harness cannot borrow a registered one. Static-to-static overrides keep working.
func TestRegisteredHarnessPinsSessions(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	registerHarness(t, h, "reg-a", remoteSpec("127.0.0.1:9000"))
	registerHarness(t, h, "reg-b", remoteSpec("127.0.0.1:9001"))

	onReg, err := createOn(ctx, h.sessions, "reg-a")
	if err != nil {
		t.Fatal(err)
	}
	onStatic, err := createOn(ctx, h.sessions, "echo")
	if err != nil {
		t.Fatal(err)
	}
	// A registration another host made, which this one has not loaded.
	if _, _, err := h.store.RegisterHarness(sqlitelog.HarnessRecord{Name: "row-only", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Fatal(err)
	}
	// Rows written before harness selection (no harness) or on a built-in this host is not
	// serving keep the documented static override.
	for uid, harness := range map[string]string{"legacy-none": "", "legacy-chat": "chat"} {
		if err := h.store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: harness}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		sess, override string
		want           codes.Code
	}{
		{"legacy-none", "echo2", codes.OK},
		{"legacy-chat", "echo", codes.OK},
		{"legacy-chat", "reg-a", codes.FailedPrecondition},
		{onReg, "reg-a", codes.OK},
		{onReg, "", codes.OK},
		{onReg, "echo", codes.FailedPrecondition},
		{onReg, "reg-b", codes.FailedPrecondition},
		{onStatic, "reg-a", codes.FailedPrecondition},
		{onStatic, "echo2", codes.OK},
		{onStatic, "nope", codes.InvalidArgument},
		{"legacy-none", "nope", codes.InvalidArgument},
		{"missing", "nope", codes.InvalidArgument},
		{"missing", "echo", codes.NotFound},
		// The pin is checked before the override is resolved: a session on a registered harness
		// refuses any other name, served or not.
		{onReg, "nope", codes.FailedPrecondition},
		// An override onto a registered harness is refused the same way, loaded here or not.
		{onStatic, "row-only", codes.FailedPrecondition},
		{"legacy-none", "row-only", codes.FailedPrecondition},
	} {
		if err := execOn(ctx, h.sessions, tc.sess, tc.override); status.Code(err) != tc.want {
			t.Errorf("exec %s with harness %q: got %v, want %v", tc.sess, tc.override, err, tc.want)
		}
	}
}

// A registration that commits and loads after the pin check has looked for it still pins: the
// override onto it is refused, not run on a harness the session is not recorded on.
func TestOverrideRegisteredDuringPinCheckIsRefused(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	onStatic, err := createOn(ctx, h.sessions, "echo")
	if err != nil {
		t.Fatal(err)
	}
	var regErr error
	var once sync.Once
	h.svc.SetAfterPinCheck(func() {
		once.Do(func() {
			_, regErr = h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "reg-late", Spec: remoteSpec("127.0.0.1:9000")})
		})
	})
	err = execOn(ctx, h.sessions, onStatic, "reg-late")
	if regErr != nil {
		t.Fatalf("register during the pin check: %v", regErr)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec %s with harness registered during the pin check: got %v, want FailedPrecondition", onStatic, err)
	}
	info, err := h.store.SessionInfo(onStatic)
	if err != nil {
		t.Fatal(err)
	}
	if info.Harness != "echo" {
		t.Fatalf("session harness = %q, want echo", info.Harness)
	}
}

// Static harnesses are read-only through the registry, and reserved names cannot be taken even when
// the static harness they belong to is not configured.
func TestStaticHarnessesAreReadOnly(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	for _, name := range []string{"echo", "echo2", "chat"} {
		if _, err := h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: name, Spec: remoteSpec("127.0.0.1:9000")}); status.Code(err) != codes.AlreadyExists {
			t.Errorf("register %q: want AlreadyExists, got %v", name, err)
		}
	}
	if _, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: "echo"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("retire static: want FailedPrecondition, got %v", err)
	}
	got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "echo"})
	if err != nil || got.GetSource() != v1.HarnessSource_HARNESS_SOURCE_STATIC || got.GetState() != v1.HarnessState_HARNESS_STATE_ACTIVE || got.GetSpec() != nil {
		t.Fatalf("get static = %v, %v", got, err)
	}
	if _, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "chat"}); status.Code(err) != codes.NotFound {
		t.Errorf("get reserved but unconfigured: want NotFound, got %v", err)
	}
	if _, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: "nope"}); status.Code(err) != codes.NotFound {
		t.Errorf("retire unknown: want NotFound, got %v", err)
	}
}

// A stored registration whose name became static refuses startup, rather than one name meaning two
// harnesses.
func TestStartupRefusesRegistrationShadowingStaticName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "chat", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Fatal(err)
	}
	b := local.New(echoagent.Harness{})
	defer b.Close()
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(b, echoagent.Model)}, placement.WithReservedNames("chat"))
	if err != nil {
		t.Fatal(err)
	}
	factory := func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		t.Fatal("factory called for a colliding name")
		return nil, nil, nil
	}
	if _, err := session.NewHarnessRegistry(store, registry, factory); err == nil {
		t.Fatal("startup accepted a registration named like a static harness")
	}
	if err := session.ReserveStaticHarnessNames(store, registry); !errors.Is(err, sqlitelog.ErrHarnessNameCollision) {
		t.Fatalf("ReserveStaticHarnessNames on a registration named like a reserved harness: %v, want ErrHarnessNameCollision", err)
	}
}

// A host that serves only Sessions reserves its names too. A journal holding a registration under
// one of them, active or retired, is refused; one using other names is not, and once the names are
// reserved no host can register them.
func TestReserveStaticHarnessNames(t *testing.T) {
	b := local.New(echoagent.Harness{})
	defer b.Close()
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(b, echoagent.Model)}, placement.WithReservedNames("chat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		retire  bool
		wantErr bool
	}{
		{name: "echo", wantErr: true},
		{name: "echo", retire: true, wantErr: true},
		{name: "chat", wantErr: true},
		{name: "other"},
		{name: "other", retire: true},
	} {
		store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: tc.name, UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
			t.Fatal(err)
		}
		if tc.retire {
			if _, err := store.RetireHarness(tc.name, ""); err != nil {
				t.Fatal(err)
			}
		}
		if err := session.ReserveStaticHarnessNames(store, registry); (err != nil) != tc.wantErr {
			t.Errorf("registration %q (retired %v): got %v, want error %v", tc.name, tc.retire, err, tc.wantErr)
		}
		store.Close()
	}

	store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 2; i++ { // reserving again is a no-op
		if err := session.ReserveStaticHarnessNames(store, registry); err != nil {
			t.Fatalf("empty journal: %v", err)
		}
	}
	for _, name := range []string{"echo", "chat"} {
		if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: name, UID: "u", Spec: "{}", SpecDigest: "d"}); !errors.Is(err, sqlitelog.ErrHarnessNameReserved) {
			t.Errorf("register reserved %q: %v, want ErrHarnessNameReserved", name, err)
		}
	}
	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "other", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Errorf("register unreserved name: %v", err)
	}
}

// Two hosts share a journal while both run. The second has different static names, so its
// registry would accept "echo" and "chat" on its own; the first host's reservations make it refuse
// them, and the first host's static echo keeps serving new and existing sessions.
func TestLiveHostsCannotRegisterAnotherHostsStaticName(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	first := startRegistryHost(t, path)
	before, err := createOn(ctx, first.sessions, "echo")
	if err != nil {
		t.Fatal(err)
	}

	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b := local.New(echoagent.Harness{})
	defer b.Close()
	registry, err := placement.NewRegistry("solo", map[string]*placement.Placer{"solo": placement.New(b, echoagent.Model)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.NewHarnessRegistry(store, registry, func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		b := local.New(echoagent.Harness{})
		return placement.New(b, echoagent.Model), func() { _ = b.Close() }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, name := range []string{"echo", "echo2", "chat"} {
		_, err := second.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: name, Spec: remoteSpec("127.0.0.1:9000")})
		if status.Code(err) != codes.AlreadyExists {
			t.Errorf("second host registers %q: got %v, want AlreadyExists", name, err)
		}
	}
	// And the first host refuses the second host's static name in turn.
	if _, err := first.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "solo", Spec: remoteSpec("127.0.0.1:9000")}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("first host registers %q: got %v, want AlreadyExists", "solo", err)
	}
	if err := execOn(ctx, first.sessions, before, ""); err != nil {
		t.Fatalf("existing session on static echo: %v", err)
	}
	after, err := createOn(ctx, first.sessions, "echo")
	if err != nil {
		t.Fatalf("new session on static echo: %v", err)
	}
	if err := execOn(ctx, first.sessions, after, ""); err != nil {
		t.Fatalf("new session on static echo: %v", err)
	}
}

// A host that did not reserve its names fails closed when a registration under one of its static
// names appears while it runs: creating a session on that name, and Exec, Fork or Resume of a turn
// that would run on it, are refused, active or retired, rather than run on the static harness or
// gated by the registration's state. Resume checks the harness the resumed turn runs on, so a
// pending turn recorded on another static harness still recovers
// (TestResumeStaticOverrideWhileStoredNameCollides).
func TestStaticNameRegisteredLaterFailsClosed(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	echo, echo2 := local.New(echoagent.Harness{}), local.New(echoagent.Harness{})
	defer echo.Close()
	defer echo2.Close()
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo":  placement.New(echo, echoagent.Model),
		"echo2": placement.New(echo2, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := serveSessions(t, session.NewService(store, registry))
	onEcho, err := createOn(ctx, c, "echo")
	if err != nil {
		t.Fatal(err)
	}
	onEcho2, err := createOn(ctx, c, "echo2")
	if err != nil {
		t.Fatal(err)
	}
	// Another host, whose static names differ, registers "echo".
	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "echo", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Fatal(err)
	}
	check := func(stage string) {
		t.Helper()
		for _, harness := range []string{"echo", ""} {
			if _, err := createOn(ctx, c, harness); status.Code(err) != codes.FailedPrecondition {
				t.Errorf("%s: create on %q: got %v, want FailedPrecondition", stage, harness, err)
			}
		}
		if _, err := c.Fork(ctx, &v1.ForkRequest{Session: onEcho, Count: 1}); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("%s: fork a session on echo: got %v, want FailedPrecondition", stage, err)
		}
		for _, tc := range []struct {
			sess, override string
			want           codes.Code
		}{
			{onEcho, "", codes.FailedPrecondition},
			{onEcho, "echo2", codes.FailedPrecondition},
			{onEcho2, "echo", codes.FailedPrecondition},
			{onEcho2, "", codes.OK},
		} {
			if err := execOn(ctx, c, tc.sess, tc.override); status.Code(err) != tc.want {
				t.Errorf("%s: exec %s with harness %q: got %v, want %v", stage, tc.sess, tc.override, err, tc.want)
			}
		}
		// Resume routes through the registry's recorded-harness lookup, not placerFor, and is
		// refused the same way.
		if _, err := c.Resume(ctx, &v1.ResumeRequest{Session: onEcho}); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("%s: resume a session on echo: got %v, want FailedPrecondition", stage, err)
		}
		if _, err := c.Resume(ctx, &v1.ResumeRequest{Session: onEcho2}); err != nil {
			t.Errorf("%s: resume a session on echo2: %v", stage, err)
		}
	}
	check("active registration")
	if _, err := store.RetireHarness("echo", ""); err != nil {
		t.Fatal(err)
	}
	check("retired registration")
}

func TestListHarnessesMergesStaticAndRegistered(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	for i, n := range []string{"zeta", "alpha", "eta"} {
		registerHarness(t, h, n, remoteSpec("127.0.0.1:900"+string(rune('0'+i))))
	}
	if _, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: "eta"}); err != nil {
		t.Fatal(err)
	}
	list := func(req *v1.ListHarnessesRequest) []string {
		t.Helper()
		var names []string
		for {
			resp, err := h.harnesses.ListHarnesses(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range resp.GetHarnesses() {
				names = append(names, r.GetMetadata().GetName())
			}
			if resp.GetNextPageToken() == "" {
				return names
			}
			req.PageToken = resp.GetNextPageToken()
		}
	}
	want := []string{"alpha", "echo", "echo2", "zeta"}
	if got := list(&v1.ListHarnessesRequest{}); !equalStrings(got, want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	if got := list(&v1.ListHarnessesRequest{PageSize: 1}); !equalStrings(got, want) {
		t.Fatalf("paged by 1 = %v, want %v", got, want)
	}
	wantAll := []string{"alpha", "echo", "echo2", "eta", "zeta"}
	if got := list(&v1.ListHarnessesRequest{PageSize: 2, IncludeRetired: true}); !equalStrings(got, wantAll) {
		t.Fatalf("with retired, paged by 2 = %v, want %v", got, wantAll)
	}
	if _, err := h.harnesses.ListHarnesses(ctx, &v1.ListHarnessesRequest{PageToken: "!!"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad page token: want InvalidArgument, got %v", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// spec_digest is defined over bytes any implementation can produce: SHA-256 of the RFC 8785 form
// of the spec's proto3-JSON. This pins it against a hand-written canonical form.
func TestSpecDigestIsCanonical(t *testing.T) {
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	resp := registerHarness(t, h, "digest", remoteSpec("unix:///run/h.sock"))
	canonical := `{"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"},"remote":{"address":"unix:///run/h.sock"}}`
	sum := sha256.Sum256([]byte(canonical))
	if want := "sha256:" + hex.EncodeToString(sum[:]); resp.GetHarness().GetSpecDigest() != want {
		t.Fatalf("spec_digest = %s, want %s", resp.GetHarness().GetSpecDigest(), want)
	}
}

func TestRegisterHarnessRejectsBadInput(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	unknown := remoteSpec("127.0.0.1:9000")
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	long := make([]byte, 5000)
	for i := range long {
		long[i] = 'a'
	}
	for _, tc := range []struct {
		desc string
		req  *v1.RegisterHarnessRequest
		want codes.Code
	}{
		{"empty name", &v1.RegisterHarnessRequest{Spec: remoteSpec("a:1")}, codes.InvalidArgument},
		{"uppercase name", &v1.RegisterHarnessRequest{Name: "Bad", Spec: remoteSpec("a:1")}, codes.InvalidArgument},
		{"64-char name", &v1.RegisterHarnessRequest{Name: string(long[:64]), Spec: remoteSpec("a:1")}, codes.InvalidArgument},
		{"no spec", &v1.RegisterHarnessRequest{Name: "h"}, codes.InvalidArgument},
		{"no placement", &v1.RegisterHarnessRequest{Name: "h", Spec: &v1.HarnessSpec{Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}}}, codes.InvalidArgument},
		{"empty address", &v1.RegisterHarnessRequest{Name: "h", Spec: remoteSpec("")}, codes.InvalidArgument},
		{"no resumability", &v1.RegisterHarnessRequest{Name: "h", Spec: &v1.HarnessSpec{Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "a:1"}}}}, codes.InvalidArgument},
		{"unknown field", &v1.RegisterHarnessRequest{Name: "h", Spec: unknown}, codes.InvalidArgument},
		{"unknown resumability", &v1.RegisterHarnessRequest{Name: "h", Spec: &v1.HarnessSpec{
			Placement:    &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: "a:1"}},
			Capabilities: &v1.Capabilities{Resumability: v1.Resumability(99)},
		}}, codes.InvalidArgument},
		{"oversized spec", &v1.RegisterHarnessRequest{Name: "h", Spec: remoteSpec(string(long))}, codes.InvalidArgument},
		{"unservable placement", &v1.RegisterHarnessRequest{Name: "h", Spec: &v1.HarnessSpec{
			Placement:    &v1.HarnessSpec_Substrate{Substrate: &v1.SubstratePlacement{Atespace: "ns", Template: "t"}},
			Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_REQUIRES_MEMORY_SNAPSHOT},
		}}, codes.FailedPrecondition},
	} {
		if _, err := h.harnesses.RegisterHarness(ctx, tc.req); status.Code(err) != tc.want {
			t.Errorf("%s: got %v, want %v", tc.desc, err, tc.want)
		}
	}
	// Nothing refused was stored.
	if _, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "h"}); status.Code(err) != codes.NotFound {
		t.Fatalf("a refused registration was stored: %v", err)
	}
}

// observe reports what the harness says about itself now, for static and registered harnesses.
func TestGetHarnessObserves(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	registerHarness(t, h, "seen", remoteSpec("127.0.0.1:9000"))
	want, err := echoagent.Harness{}.Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"seen", "echo"} {
		got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: name, Observe: true})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetObserveError() != "" || got.GetObserved().GetId() != want.ID {
			t.Fatalf("%s observed = %v, error %q; want id %q", name, got.GetObserved(), got.GetObserveError(), want.ID)
		}
		plain, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: name})
		if err != nil || plain.GetObserved() != nil {
			t.Fatalf("%s without observe = %v, %v", name, plain, err)
		}
	}
}

// toolHarness is echo with declared tools, so observe can be checked for the full descriptor.
type toolHarness struct{ echoagent.Harness }

func (toolHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	d, err := echoagent.Harness{}.Describe(ctx)
	d.Tools = []api.ToolSpec{
		{Name: "lookup", Description: "Look up a record", Mediation: api.MediationControllerMediated},
		{Name: "charge", Description: "Request a charge", Mediation: api.MediationRequiresApproval},
	}
	return d, err
}

// observe returns the harness's declared tools with the rest of its descriptor.
func TestGetHarnessObservesDeclaredTools(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	b := local.New(toolHarness{})
	t.Cleanup(func() { _ = b.Close() })
	registry, err := placement.NewRegistry("tools", map[string]*placement.Placer{"tools": placement.New(b, echoagent.Model)})
	if err != nil {
		t.Fatal(err)
	}
	noRegistrations := func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		return nil, nil, errors.New("this test registers nothing")
	}
	reg, err := session.NewHarnessRegistry(store, registry, noRegistrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	got, err := reg.GetHarness(ctx, &v1.GetHarnessRequest{Name: "tools", Observe: true})
	if err != nil {
		t.Fatal(err)
	}
	want, err := toolHarness{}.Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tools []api.ToolSpec
	for _, tool := range got.GetObserved().GetTools() {
		tools = append(tools, wire.ToolSpecFromProto(tool))
	}
	if got.GetObserveError() != "" || !reflect.DeepEqual(tools, want.Tools) {
		t.Fatalf("observed tools = %v, error %q; want %v", tools, got.GetObserveError(), want.Tools)
	}
}

// A factory that returns no Placer and no error is a host bug. RegisterHarness refuses it with
// nothing stored and the factory's resources released, and startup refuses it for a stored row.
func TestRegisterHarnessRefusesNilPlacer(t *testing.T) {
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
	nilFactory := func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		return nil, func() { released.Add(1) }, nil
	}
	reg, err := session.NewHarnessRegistry(store, newRegistry(), nilFactory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "h", Spec: remoteSpec("127.0.0.1:9000")}); status.Code(err) != codes.Internal {
		t.Fatalf("register with a nil placer: got %v, want Internal", err)
	}
	if got := released.Load(); got != 1 {
		t.Fatalf("release called %d times, want 1", got)
	}
	if _, err := store.Harness("h"); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatalf("a registration with no placer was stored: %v", err)
	}
	reg.Close()

	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "h", UID: "u", Spec: `{"remote":{"address":"a:1"},"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"}}`, SpecDigest: "d"}); err != nil {
		t.Fatal(err)
	}
	r, err := session.NewHarnessRegistry(store, newRegistry(), nilFactory)
	if err != nil {
		t.Fatalf("startup must isolate a factory returning no placer: %v", err)
	}
	got, err := r.GetHarness(ctx, &v1.GetHarnessRequest{Name: "h"})
	if err != nil || !strings.Contains(got.GetUnservableReason(), "no placer") {
		t.Fatalf("factory failure status = %v, %v", got, err)
	}
	if got := released.Load(); got != 2 {
		t.Fatalf("release called %d times after failed startup load, want 2", got)
	}
	r.Close()
	if got := released.Load(); got != 2 {
		t.Fatalf("Close released failed factory resources again: %d", got)
	}
}

// A v0.1.2 journal has sessions on static harnesses and no reservations, because version 1 had no
// registry. Here the first host to open it serves only "solo", so its own reservations do not cover
// "echo", and it tries to register "echo". The migration reserved the name from the old sessions,
// so the registration is refused, and a host that serves "echo" then starts on the same journal and
// resumes the old session on its static echo.
func TestMigrationReservesV1SessionHarnesses(t *testing.T) {
	ctx := context.Background()
	fixture, err := os.ReadFile(filepath.Join("..", "sqlitelog", "testdata", "v0.1.2.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "journal.db")
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	// The fixture's own sessions ran a turn before execution IDs were journaled, which this build
	// cannot replay, so add the row v0.1.2's CreateSession writes for a session with no turn yet.
	const legacy = "sess-v012-created"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	if _, err := db.Exec(`INSERT INTO sessions(session, fence, project, name, harness, model, parent_uid, fork_seq,
		compute_state, created_at, updated_at, labels, annotations, origin, identity)
		VALUES(?, 0, 'default', '', 'echo', '', '', 0, 'NONE', ?, ?, '', '', '', '')`, legacy, now, now); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("journal user_version = %d (%v), want 1", version, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := sqlitelog.Open(path) // migrates the journal to the current schema
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b := local.New(echoagent.Harness{})
	defer b.Close()
	registry, err := placement.NewRegistry("solo", map[string]*placement.Placer{"solo": placement.New(b, echoagent.Model)})
	if err != nil {
		t.Fatal(err)
	}
	solo, err := session.NewHarnessRegistry(store, registry, func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
		b := local.New(echoagent.Harness{})
		return placement.New(b, echoagent.Model), func() { _ = b.Close() }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer solo.Close()
	if _, err := solo.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "echo", Spec: remoteSpec("127.0.0.1:9000")}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("register %q on a migrated v0.1.2 journal: got %v, want AlreadyExists", "echo", err)
	}
	if _, err := store.Harness("echo"); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Errorf("registration row for %q: %v, want ErrHarnessNotFound", "echo", err)
	}

	echo := startRegistryHost(t, path)
	r, err := echo.sessions.Resume(ctx, &v1.ResumeRequest{Session: legacy})
	if err != nil || r.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
		t.Fatalf("resume v0.1.2 session: %v, %v", r.GetComputeState(), err)
	}
	if r.GetHarness() != "echo" {
		t.Fatalf("resumed v0.1.2 session on harness %q, want echo", r.GetHarness())
	}
	if err := execOn(ctx, echo.sessions, legacy, ""); err != nil {
		t.Fatalf("exec v0.1.2 session: %v", err)
	}
}
