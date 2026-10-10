package session_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
)

// A stored spec from a newer host must not prevent this host's static and compatible harnesses
// from starting. The unknown field is deliberately seeded below RegisterHarness validation.
func TestStoredHarnessUnknownFieldDoesNotStopStartup(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "retired"}[retired], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store, err := sqlitelog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range []sqlitelog.HarnessRecord{
				{Name: "a-good", UID: "uid-good", Spec: `{"remote":{"address":"127.0.0.1:9000"},"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"}}`, SpecDigest: "good-digest"},
				{Name: "b-bad", UID: "uid-bad", Spec: `{"remote":{"address":"127.0.0.1:9001"},"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"},"futureField":true}`, SpecDigest: "original-digest"},
				{Name: "c-good", UID: "uid-more", Spec: `{"remote":{"address":"127.0.0.1:9002"},"capabilities":{"resumability":"RESUMABILITY_STATELESS_REPLAY"}}`, SpecDigest: "more-digest"},
				{Name: "d-enum", UID: "uid-enum", Spec: `{"remote":{"address":"127.0.0.1:9003"},"capabilities":{"resumability":99}}`, SpecDigest: "enum-digest"},
			} {
				if _, _, err := store.RegisterHarness(rec); err != nil {
					t.Fatal(err)
				}
			}
			if retired {
				if _, err := store.RetireHarness("b-bad", "old harness"); err != nil {
					t.Fatal(err)
				}
			}
			for _, uid := range []string{"old-bad", "old-echo"} {
				name := "b-bad"
				if uid == "old-echo" {
					name = "echo"
				}
				if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: name}); err != nil {
					t.Fatal(err)
				}
			}
			seedRoutingInvocation(t, store.Session("old-echo"), &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "b-bad"}, false)
			store.Close()

			h := startRegistryHost(t, path)
			ctx := context.Background()
			assertRefusal := func(op string, err error) {
				t.Helper()
				if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "possible host/schema version skew") {
					t.Fatalf("%s = %v, want FailedPrecondition with upgrade hint", op, err)
				}
			}
			for _, name := range []string{"echo", "a-good", "c-good"} {
				sess, err := createOn(ctx, h.sessions, name)
				if err != nil {
					t.Fatalf("create on %s: %v", name, err)
				}
				if outs := execOutputs(t, h.sessions, sess, "one", 0); len(outs) != 1 || outs[0] != "echo:one" {
					t.Fatalf("%s outputs = %v", name, outs)
				}
			}
			got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "b-bad", Observe: true})
			if err != nil || got.GetSpecDigest() != "original-digest" || got.GetMetadata().GetUid() != "uid-bad" || got.GetSpec() != nil || !strings.Contains(got.GetUnservableReason(), "possible host/schema version skew") || got.GetObserved() != nil || got.GetObserveError() == "" {
				t.Fatalf("bad registration projection and observation = %v, %v", got, err)
			}
			wantState := v1.HarnessState_HARNESS_STATE_ACTIVE
			if retired {
				wantState = v1.HarnessState_HARNESS_STATE_RETIRED
				if got.GetRetireReason() != "old harness" || got.GetRetireTime() == nil {
					t.Fatalf("retirement metadata lost: %v", got)
				}
			}
			if got.GetState() != wantState || got.GetMetadata().GetCreateTime() == nil || got.GetMetadata().GetUpdateTime() == nil {
				t.Fatalf("registration state or timestamps lost: %v", got)
			}
			enum, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "d-enum"})
			if err != nil || enum.GetSpec() != nil || !strings.Contains(enum.GetUnservableReason(), "possible host/schema version skew") || !strings.Contains(enum.GetUnservableReason(), "99") {
				t.Fatalf("unknown numeric enum accepted: %v, %v", enum, err)
			}
			// The incompatible row must not truncate a list page, even when it falls between
			// compatible registrations or is RETIRED.
			var names []string
			token := ""
			for {
				page, err := h.harnesses.ListHarnesses(ctx, &v1.ListHarnessesRequest{PageSize: 2, PageToken: token, IncludeRetired: true})
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range page.GetHarnesses() {
					names = append(names, entry.GetMetadata().GetName())
					if entry.GetMetadata().GetName() == "b-bad" && (entry.GetSpecDigest() != "original-digest" || entry.GetState() != wantState || entry.GetUnservableReason() == "") {
						t.Fatalf("bad row mid-page = %v", entry)
					}
				}
				if token = page.GetNextPageToken(); token == "" {
					break
				}
			}
			if !equalStrings(names, []string{"a-good", "b-bad", "c-good", "d-enum", "echo", "echo2"}) {
				t.Fatalf("paged registrations = %v", names)
			}
			if retired {
				page, err := h.harnesses.ListHarnesses(ctx, &v1.ListHarnessesRequest{})
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range page.GetHarnesses() {
					if entry.GetMetadata().GetName() == "b-bad" {
						t.Fatal("retired row visible without include_retired")
					}
				}
			}
			ret, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: "b-bad", Reason: "new reason"})
			if err != nil || ret.GetState() != v1.HarnessState_HARNESS_STATE_RETIRED || ret.GetUnservableReason() == "" || ret.GetSpecDigest() != "original-digest" {
				t.Fatalf("retire incompatible row = %v, %v", ret, err)
			}
			if retired && ret.GetRetireReason() != "old harness" {
				t.Fatalf("retire replaced original reason: %v", ret)
			}
			if _, err := h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "b-bad", Spec: remoteSpec("127.0.0.1:9000")}); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("conflicting registration = %v, want AlreadyExists", err)
			}
			for _, name := range []string{"b-bad", "d-enum"} {
				_, err := createOn(ctx, h.sessions, name)
				assertRefusal("Create("+name+")", err)
				assertRefusal("auto Exec("+name+")", execOn(ctx, h.sessions, "", name))
			}
			if n := countSessionsOn(t, h.svc, "b-bad"); n != 1 {
				t.Fatalf("refused creates stored sessions: %d", n)
			}
			before := headOf(t, h, "old-bad")
			assertRefusal("existing Exec", execOn(ctx, h.sessions, "old-bad", ""))
			_, err = h.sessions.Suspend(ctx, &v1.SuspendRequest{Session: "old-bad"})
			assertRefusal("Suspend", err)
			_, err = h.sessions.Fork(ctx, &v1.ForkRequest{Session: "old-bad"})
			assertRefusal("Fork", err)
			if after := headOf(t, h, "old-bad"); after != before {
				t.Fatalf("Exec/Suspend/Fork wrote to old-bad: %d -> %d", before, after)
			}
			for _, uid := range []string{"old-bad", "old-echo"} {
				before := headOf(t, h, uid)
				_, err := h.sessions.Resume(ctx, &v1.ResumeRequest{Session: uid})
				assertRefusal("Resume("+uid+")", err)
				if after := headOf(t, h, uid); after != before {
					t.Fatalf("refused calls wrote to %s: %d -> %d", uid, before, after)
				}
			}
			if n := countSessionsOn(t, h.svc, "b-bad"); n != 1 {
				t.Fatalf("refused fork stored children: %d", n)
			}
		})
	}
}

// Describing a live remote backend would dial its address. No unservable entry may get this far.
type countedDescribeBackend struct {
	placement.Backend
	calls *atomic.Int32
}

func (b countedDescribeBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	b.calls.Add(1)
	return b.Backend.Describe(ctx)
}

// Factory failures (even when a factory returns a Placer alongside its error) and descriptor
// mismatches release allocations at load time. An identical repeat on the name must neither
// attempt Add a second time nor imply a failed placement became servable.
func TestStoredHarnessFactoryFailuresAreIsolated(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	first := startRegistryHost(t, path)
	for _, name := range []string{"fail", "healthy"} {
		registerHarness(t, first, name, remoteSpec("127.0.0.1:9000"))
	}
	registerHarness(t, first, "wrong", specExpecting("127.0.0.1:9000", "echo"))
	first.stop()

	var failedRelease, wrongRelease, healthyRelease, failedLoads, badDescribes atomic.Int32
	factory := func(name string, spec *v1.HarnessSpec) (*placement.Placer, func(), error) {
		switch name {
		case "fail":
			failedLoads.Add(1)
			backend := local.New(echoagent.Harness{})
			p := placement.New(countedDescribeBackend{backend, &badDescribes}, echoagent.Model, placement.WithDescriptorID(spec.GetDescriptorId()))
			return p, func() { failedRelease.Add(1); _ = backend.Close() }, errNoSubstrate
		case "wrong":
			backend := local.New(echoagent.Harness{})
			return placement.New(countedDescribeBackend{backend, &badDescribes}, echoagent.Model), func() { wrongRelease.Add(1); _ = backend.Close() }, nil
		default:
			p, close, err := echoFactory(name, spec)
			return p, func() { healthyRelease.Add(1); close() }, err
		}
	}
	h := startRegistryHostWith(t, path, factory)
	if failedRelease.Load() != 1 || wrongRelease.Load() != 1 || healthyRelease.Load() != 0 {
		t.Fatalf("startup release counts: failed=%d wrong=%d healthy=%d", failedRelease.Load(), wrongRelease.Load(), healthyRelease.Load())
	}
	for _, tc := range []struct{ name, cause string }{{"fail", errNoSubstrate.Error()}, {"wrong", "WithDescriptorID"}} {
		got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: tc.name, Observe: true})
		if err != nil || got.GetSpec() == nil || !strings.Contains(got.GetUnservableReason(), tc.cause) || got.GetObserved() != nil || got.GetObserveError() == "" {
			t.Fatalf("%s projection/observation = %v, %v", tc.name, got, err)
		}
		if _, err := createOn(ctx, h.sessions, tc.name); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("create on %s = %v, want FailedPrecondition", tc.name, err)
		}
	}
	if badDescribes.Load() != 0 {
		t.Fatalf("observing unservable registrations described the backend %d times", badDescribes.Load())
	}
	unchanged, err := h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "fail", Spec: remoteSpec("127.0.0.1:9000")})
	if err != nil || unchanged.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_UNCHANGED || unchanged.GetHarness().GetUnservableReason() == "" || failedLoads.Load() != 1 {
		t.Fatalf("repeat unservable registration = %v, %v (factory called %d times)", unchanged, err, failedLoads.Load())
	}
	if _, err := h.harnesses.RetireHarness(ctx, &v1.RetireHarnessRequest{Name: "fail"}); err != nil {
		t.Fatal(err)
	}
	reactivated, err := h.harnesses.RegisterHarness(ctx, &v1.RegisterHarnessRequest{Name: "fail", Spec: remoteSpec("127.0.0.1:9000")})
	if err != nil || reactivated.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_REACTIVATED || reactivated.GetHarness().GetUnservableReason() == "" || failedLoads.Load() != 1 {
		t.Fatalf("reactivated unservable registration = %v, %v (factory called %d times)", reactivated, err, failedLoads.Load())
	}
	good, err := createOn(ctx, h.sessions, "healthy")
	if err != nil {
		t.Fatal(err)
	}
	if outs := execOutputs(t, h.sessions, good, "live", 0); len(outs) != 1 || outs[0] != "echo:live" {
		t.Fatalf("healthy registered harness did not execute: %v", outs)
	}
	h.stop()
	if failedRelease.Load() != 1 || wrongRelease.Load() != 1 || healthyRelease.Load() != 1 {
		t.Fatalf("Close release counts: failed=%d wrong=%d healthy=%d", failedRelease.Load(), wrongRelease.Load(), healthyRelease.Load())
	}
}

// A valid registration made on another host since startup is visible but not locally loaded.
// The Sessions service treats the name as unknown until an identical compatible registration
// loads it locally; that call need not restart the host or alter the immutable stored row.
func TestStoredHarnessAddedSinceStartupIsNotLoaded(t *testing.T) {
	ctx := context.Background()
	h := startRegistryHost(t, filepath.Join(t.TempDir(), "journal.db"))
	spec := remoteSpec("a:1")
	canonical, err := canon.Proto(spec)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(canonical))
	if _, _, err := h.store.RegisterHarness(sqlitelog.HarnessRecord{
		Name: "later", UID: "uid-later", Spec: string(canonical), SpecDigest: digest,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := h.harnesses.GetHarness(ctx, &v1.GetHarnessRequest{Name: "later", Observe: true})
	if err != nil || got.GetSpec() == nil || got.GetSpecDigest() != digest || !strings.Contains(got.GetUnservableReason(), "restart") || !strings.Contains(got.GetUnservableReason(), "register the identical compatible spec") || got.GetObserved() != nil || got.GetObserveError() == "" {
		t.Fatalf("newly registered row projected as loaded: %v, %v", got, err)
	}
	if _, err := createOn(ctx, h.sessions, "later"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Create on not-loaded row = %v, want InvalidArgument", err)
	}
	if err := execOn(ctx, h.sessions, "", "later"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("auto Exec on not-loaded row = %v, want InvalidArgument", err)
	}
	if n := countSessionsOn(t, h.svc, "later"); n != 0 {
		t.Fatalf("refused calls stored %d sessions on not-loaded row", n)
	}
	repeat := registerHarness(t, h, "later", spec)
	if repeat.GetOutcome() != v1.RegisterOutcome_REGISTER_OUTCOME_UNCHANGED || repeat.GetHarness().GetMetadata().GetUid() != "uid-later" || repeat.GetHarness().GetSpecDigest() != digest || repeat.GetHarness().GetUnservableReason() != "" {
		t.Fatalf("identical local registration did not load stored row: %v", repeat)
	}
	sess, err := createOn(ctx, h.sessions, "later")
	if err != nil {
		t.Fatalf("Create after identical registration: %v", err)
	}
	if outs := execOutputs(t, h.sessions, sess, "works", 0); len(outs) != 1 || outs[0] != "echo:works" {
		t.Fatalf("locally loaded harness did not execute: %v", outs)
	}
}
