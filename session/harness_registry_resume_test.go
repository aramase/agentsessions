package session_test

import (
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

// A pending turn recorded on a harness other than the session's ran as an override, so Resume
// applies the pin rule Exec applies to that override: a registered harness cannot be borrowed for
// one turn, and a session on a registered harness cannot resume a turn on another one. Both hold
// when the name was registered after the turn was recorded. A refused Resume runs nothing and
// journals nothing.
func TestResumeRecordedOverrideHonorsRegisteredPin(t *testing.T) {
	for _, tc := range []struct {
		name             string
		stored, recorded string
	}{
		// alias-b was static on the host that ran the turn; this host does not serve it statically,
		// and it was registered after the turn was recorded.
		{name: "override name registered later", stored: "alias-a", recorded: "alias-b"},
		// A session pinned to a registered harness with a turn recorded on a static one.
		{name: "session on a registered harness", stored: "alias-b", recorded: "alias-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			const uid = "routing-session"
			if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
				t.Fatal(err)
			}
			log := store.Session(uid)
			seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: tc.recorded}, false)

			a, b := &routingHarness{id: "descriptor-a"}, &routingHarness{id: "descriptor-b"}
			var aModels, aTools, bModels, bTools atomic.Int32
			registry := routingRegistry(t, map[string]*placement.Placer{"alias-a": routingPlacer(t, a, "receipt-a", &aModels, &aTools)})
			reg, err := session.NewHarnessRegistry(store, registry, func(string, *v1.HarnessSpec) (*placement.Placer, func(), error) {
				return routingPlacer(t, b, "receipt-b", &bModels, &bTools), func() {}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reg.Close)
			if _, err := reg.RegisterHarness(t.Context(), &v1.RegisterHarnessRequest{Name: "alias-b", Spec: remoteSpec("127.0.0.1:9000")}); err != nil {
				t.Fatal(err)
			}
			if tc.stored != "alias-a" {
				if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: tc.stored}); err != nil {
					t.Fatal(err)
				}
			}
			before := routingRecords(t, log)

			client := serveRegistry(t, store, registry)
			if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("Resume = %v, want FailedPrecondition", err)
			}
			if a.runs.Load() != 0 || b.runs.Load() != 0 || aModels.Load() != 0 || bModels.Load() != 0 || aTools.Load() != 0 || bTools.Load() != 0 {
				t.Fatalf("refused Resume executed harness/model/tool: A=%d/%d/%d B=%d/%d/%d", a.runs.Load(), aModels.Load(), aTools.Load(), b.runs.Load(), bModels.Load(), bTools.Load())
			}
			if after := routingRecords(t, log); !reflect.DeepEqual(before, after) {
				t.Fatalf("refused Resume changed records: before=%+v after=%+v", before, after)
			}
		})
	}
}

// A turn recorded on another static harness still recovers when the session's own static harness
// has a colliding registration row from a host that did not reserve its names. The turn runs on the
// recorded harness, which has no row; the next Exec on the session's harness is still refused.
func TestResumeStaticOverrideWhileStoredNameCollides(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b"}, false)
	a, b := &routingHarness{id: "descriptor-a"}, &routingHarness{id: "descriptor-b"}
	var aModels, aTools, bModels, bTools atomic.Int32
	client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{
		"alias-a": routingPlacer(t, a, "receipt-a", &aModels, &aTools),
		"alias-b": routingPlacer(t, b, "receipt-b", &bModels, &bTools),
	}))
	if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: "alias-a", UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("resume static override: %v", err)
	}
	if a.runs.Load() != 0 || b.runs.Load() != 1 || bTools.Load() != 1 {
		t.Fatalf("Resume ran A=%d B=%d/%d; want A=0 B=1/1", a.runs.Load(), b.runs.Load(), bTools.Load())
	}
	if err := execOn(t.Context(), client, uid, ""); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("exec on the colliding stored harness: %v, want FailedPrecondition", err)
	}
}
