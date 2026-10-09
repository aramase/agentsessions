package session_test

import (
	"context"
	"io"
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
	"github.com/aramase/agentsessions/sqlitelog"
)

// A recorded name is a recovery precondition, not a new caller request: no fallback is safe.
// Version checks must reject before Run and before journaling an ERROR or RESUME marker.
func TestResumeRecordedHarnessPreconditionsHaveNoExecutionEffects(t *testing.T) {
	for _, tc := range []struct {
		name, recordedName, recordedVersion, servedVersion string
		serveB                                             bool
	}{
		{name: "recorded alias unserved", recordedName: "alias-b", serveB: false},
		{name: "descriptor ID is not registry alias", recordedName: "descriptor-b", serveB: true},
		{name: "recorded version changed", recordedName: "alias-b", recordedVersion: "v1", servedVersion: "v2", serveB: true},
		{name: "recorded version served empty", recordedName: "alias-b", recordedVersion: "v1", serveB: true},
		{name: "missing name still guards version", recordedVersion: "v1", servedVersion: "v2", serveB: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			const uid = "routing-session"
			if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
				t.Fatal(err)
			}
			log := store.Session(uid)
			seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: tc.recordedName, HarnessVersion: tc.recordedVersion}, false)
			before := routingRecords(t, log)
			a := &routingHarness{id: "descriptor-a", version: tc.servedVersion}
			b := &routingHarness{id: "descriptor-b", version: tc.servedVersion}
			var aModels, aTools, bModels, bTools atomic.Int32
			entries := map[string]*placement.Placer{"alias-a": routingPlacer(t, a, "receipt-a", &aModels, &aTools)}
			if tc.serveB {
				entries["alias-b"] = routingPlacer(t, b, "receipt-b", &bModels, &bTools)
			}
			client := serveRegistry(t, store, routingRegistry(t, entries))
			_, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			if status.Code(err) != codes.FailedPrecondition {
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

type countedRoutingBackend struct {
	*local.Backend
	describes atomic.Int32
	restores  atomic.Int32
}

func (b *countedRoutingBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	b.describes.Add(1)
	return b.Backend.Describe(ctx)
}

func (b *countedRoutingBackend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	b.restores.Add(1)
	return b.Backend.Restore(ctx, ref)
}

func TestResumeUnservedRecordedHarnessDoesNotPlaceFallback(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b"}, false)
	before := routingRecords(t, log)
	h := &routingHarness{id: "descriptor-a"}
	backend := &countedRoutingBackend{Backend: local.New(h)}
	t.Cleanup(func() { _ = backend.Close() })
	var dials, models atomic.Int32
	p := placement.New(backend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		return api.ModelResponse{}, nil
	}, placement.WithDialer(func(string) (api.Harness, func() error, error) {
		dials.Add(1)
		return h, func() error { return nil }, nil
	}))
	client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{"alias-a": p}))
	_, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unserved recorded alias = %v, want registry refusal", err)
	}
	if backend.describes.Load() != 0 || backend.restores.Load() != 0 || dials.Load() != 0 || models.Load() != 0 || h.runs.Load() != 0 || !reflect.DeepEqual(before, routingRecords(t, log)) {
		t.Fatal("unserved recorded alias attempted fallback placement or changed journal")
	}
}

func seedRoutingInvocation(t *testing.T, log eventlog.Store, invocation *api.ExecutionStart, completed bool) {
	t.Helper()
	call := routingToolCall()
	var events []api.Event
	if invocation != nil {
		events = append(events, api.Event{Kind: api.EventExecutionStart, ExecutionStart: invocation})
	}
	events = append(events, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "charge")}, api.Event{Kind: api.EventToolCall, ToolCall: &call})
	if completed {
		events = append(events, api.Event{Kind: api.EventToolResult, Result: &api.ToolResult{ID: "routing-call", Output: map[string]any{"receipt": "recorded"}}},
			api.Event{Kind: api.EventOutput, Message: api.TextMessage("assistant", "routing-call:recorded")},
			api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}})
	}
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events {
		event.ExecutionID = "crashed-override"
		if _, err := log.Append(int64(i), fence, event); err != nil {
			t.Fatal(err)
		}
	}
}

func routingRecords(t *testing.T, log eventlog.Store) []eventlog.Record {
	t.Helper()
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func routingEvent(t *testing.T, recs []eventlog.Record, kind api.EventKind) api.Event {
	t.Helper()
	for _, record := range recs {
		if record.Event.Kind == kind {
			return record.Event
		}
	}
	t.Fatalf("journal has no %s event", kind)
	return api.Event{}
}

func TestResumeRecordedHarnessLegacyFields(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invocation *api.ExecutionStart
		wantB      bool
		wantCount  int
	}{
		{"missing name", &api.ExecutionStart{InputCount: proto.Int64(1), HarnessVersion: "v1"}, false, 7},
		{"missing version", &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b"}, true, 7},
		{"both missing", &api.ExecutionStart{InputCount: proto.Int64(1)}, false, 7},
		{"legacy markerless", nil, false, 6},
		{"exact version", &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b", HarnessVersion: "v1"}, true, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			const uid = "routing-session"
			if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
				t.Fatal(err)
			}
			log := store.Session(uid)
			seedRoutingInvocation(t, log, tc.invocation, false)
			a, b := &routingHarness{id: "descriptor-a", version: "v1"}, &routingHarness{id: "descriptor-b", version: "v1"}
			var aModels, aTools, bModels, bTools atomic.Int32
			client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{
				"alias-a": routingPlacer(t, a, "receipt-a", &aModels, &aTools),
				"alias-b": routingPlacer(t, b, "receipt-b", &bModels, &bTools),
			}))
			if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
				t.Fatal(err)
			}
			wantReceipt := "routing-call:receipt-a"
			if tc.wantB {
				wantReceipt = "routing-call:receipt-b"
			}
			if tc.wantB && (a.runs.Load() != 0 || aTools.Load() != 0 || b.runs.Load() != 1 || bTools.Load() != 1) || !tc.wantB && (a.runs.Load() != 1 || aTools.Load() != 1 || b.runs.Load() != 0 || bTools.Load() != 0) || aModels.Load() != 0 || bModels.Load() != 0 {
				t.Fatalf("legacy Resume wrong route: A=%d/%d/%d B=%d/%d/%d", a.runs.Load(), aModels.Load(), aTools.Load(), b.runs.Load(), bModels.Load(), bTools.Load())
			}
			recs := routingRecords(t, log)
			output := routingEvent(t, recs, api.EventOutput).Message
			lifecycle := routingEvent(t, recs, api.EventLifecycle).Lifecycle
			if len(recs) != tc.wantCount || output == nil || output.Text() != wantReceipt || lifecycle == nil || lifecycle.Kind != api.LifecycleResume {
				t.Fatalf("legacy Resume changed record count or result: %+v", recs)
			}
		})
	}
}

func TestResumeUnknownStoredDefaultRetainsInvalidArgument(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "unserved-default"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	before := routingRecords(t, log)
	h := &routingHarness{id: "descriptor-a"}
	var models, tools atomic.Int32
	client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{"alias-a": routingPlacer(t, h, "receipt", &models, &tools)}))
	_, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown stored default Resume=%v, want InvalidArgument", err)
	}
	if h.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || !reflect.DeepEqual(before, routingRecords(t, log)) {
		t.Fatal("unknown stored default attempted fallback execution or changed journal")
	}
}

func TestCompletedResumeUsesStoredHarnessNotRecordedOverride(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "unserved-b", HarnessVersion: "unserved-version"}, true)
	before := routingRecords(t, log)
	a := &routingHarness{id: "descriptor-a", version: "different-version"}
	var models, tools atomic.Int32
	client := serveRegistry(t, store, routingRegistry(t, map[string]*placement.Placer{"alias-a": routingPlacer(t, a, "receipt-a", &models, &tools)}))
	resumed, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if err != nil {
		t.Fatal(err)
	}
	after := routingRecords(t, log)
	if len(after) != len(before)+1 || !reflect.DeepEqual(before, after[:len(before)]) || after[len(before)].Event.Lifecycle == nil || after[len(before)].Event.Lifecycle.Kind != api.LifecycleResume {
		t.Fatalf("completed Resume must only append RESUME: %+v", after)
	}
	if a.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || resumed.GetComputeState() != v1.ComputeState_COMPUTE_LIVE {
		t.Fatalf("completed Resume executed the turn or lost lifecycle: Run/model/tool=%d/%d/%d response=%v", a.runs.Load(), models.Load(), tools.Load(), resumed)
	}
}

// Setting registry identity on a shared Placer would let the last alias overwrite the first.
func TestExecRecordsResolvedAliasesWithoutChangingStoredDefault(t *testing.T) {
	store := openStore(t, ":memory:")
	h := &routingHarness{id: "not-an-alias", version: "v1"}
	var models, tools atomic.Int32
	p := routingPlacer(t, h, "receipt", &models, &tools)
	registry := routingRegistry(t, map[string]*placement.Placer{"alias-a": p, "alias-b": p})
	client := serveRegistry(t, store, registry)
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ override, want string }{{"alias-a", "alias-a"}, {"alias-b", "alias-b"}, {"", "alias-a"}} {
		stream, err := client.Exec(t.Context(), &v1.ExecRequest{Session: uid, Harness: tc.override})
		if err != nil {
			t.Fatal(err)
		}
		var marker *v1.ExecutionStart
		for {
			update, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if record := update.GetRecord(); record != nil && record.GetEvent().GetExecutionStart() != nil {
				marker = record.GetEvent().GetExecutionStart()
			}
		}
		if marker == nil || marker.GetHarness() != tc.want || marker.GetHarnessVersion() != "v1" {
			t.Fatalf("Exec alias %q marker = %v, want %q/v1", tc.override, marker, tc.want)
		}
	}
	info, err := store.SessionInfo(uid)
	if err != nil || info.Harness != "alias-a" {
		t.Fatalf("Exec override changed stored default: %+v %v", info, err)
	}
}

type unavailableRoutingHarness struct {
	*routingHarness
}

func (unavailableRoutingHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{}, status.Error(codes.Unavailable, "connected harness unavailable")
}

// Admission may succeed while the connected harness is no longer reachable for identity checking.
func TestResumeConnectedHarnessOutageRemainsUnavailable(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b", HarnessVersion: "v1"}, false)
	before := routingRecords(t, log)
	a, b := &routingHarness{id: "descriptor-a"}, &routingHarness{id: "descriptor-b", version: "v1"}
	var models, tools atomic.Int32
	aPlacer := routingPlacer(t, a, "receipt-a", &models, &tools)
	backend := local.New(b)
	t.Cleanup(func() { _ = backend.Close() })
	var closes atomic.Int32
	bPlacer := placement.New(backend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		return api.ModelResponse{}, nil
	}, placement.WithDialer(func(string) (api.Harness, func() error, error) {
		return unavailableRoutingHarness{b}, func() error { closes.Add(1); return nil }, nil
	}), placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
		tools.Add(1)
		return api.ToolResult{}, nil
	}))
	svc := session.NewService(store, routingRegistry(t, map[string]*placement.Placer{"alias-a": aPlacer, "alias-b": bPlacer}))
	_, err := svc.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("connected harness outage = %v, want Unavailable", err)
	}
	if a.runs.Load() != 0 || b.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || closes.Load() != 1 || !reflect.DeepEqual(before, routingRecords(t, log)) {
		t.Fatal("connected harness outage ran effects, leaked connection or changed journal")
	}
}

func TestResumeRecordedHarnessOutageRemainsUnavailable(t *testing.T) {
	store := openStore(t, ":memory:")
	const uid = "routing-session"
	if err := store.PutSession(sqlitelog.SessionMeta{UID: uid, Harness: "alias-a"}); err != nil {
		t.Fatal(err)
	}
	log := store.Session(uid)
	seedRoutingInvocation(t, log, &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "alias-b"}, false)
	before := routingRecords(t, log)
	a, b := &routingHarness{id: "descriptor-a"}, &routingHarness{id: "descriptor-b"}
	var models, tools atomic.Int32
	aPlacer := routingPlacer(t, a, "receipt-a", &models, &tools)
	backend := local.New(b)
	t.Cleanup(func() { _ = backend.Close() })
	bBackend := &describeFails{Backend: backend, peerErr: status.Error(codes.Unavailable, "recorded harness unavailable")}
	bPlacer := placement.New(bBackend, func(context.Context, api.ModelRequest) (api.ModelResponse, error) {
		models.Add(1)
		return api.ModelResponse{}, nil
	})
	svc := session.NewService(store, routingRegistry(t, map[string]*placement.Placer{"alias-a": aPlacer, "alias-b": bPlacer}))
	_, err := svc.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("recorded harness outage = %v, want Unavailable", err)
	}
	if a.runs.Load() != 0 || b.runs.Load() != 0 || models.Load() != 0 || tools.Load() != 0 || !reflect.DeepEqual(before, routingRecords(t, log)) {
		t.Fatal("recorded harness outage fell back or changed journal")
	}
}
