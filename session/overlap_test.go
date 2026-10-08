package session_test

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// Only the first turn waits, allowing a missing guard to reveal itself instead of deadlocking.
type overlapHarness struct {
	entered chan struct{}
	release chan struct{}
	started atomic.Bool
}

func (h *overlapHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return (echoagent.Harness{}).Describe(ctx)
}

func (h *overlapHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.started.CompareAndSwap(false, true) {
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return (echoagent.Harness{}).Run(ctx, start, sink)
}

// Holding Create parks Exec inside the shared Registry guard before it writes a new invocation.
// Resume must report contention before inspecting a stale incomplete or unserved-name prefix.
type blockedCreateBackend struct {
	*local.Backend
	entered chan struct{}
	release chan struct{}
	started atomic.Bool
}

func (b *blockedCreateBackend) Create(ctx context.Context, spec *api.SessionSpec) (api.Incarnation, error) {
	if b.started.CompareAndSwap(false, true) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return api.Incarnation{}, ctx.Err()
		}
	}
	return b.Backend.Create(ctx, spec)
}

func TestResumeOverlapPrecedesPendingInvocationResolution(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invocation *api.ExecutionStart
	}{
		{"incomplete old invocation", &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "recorded"}},
		{"unserved recorded alias", &api.ExecutionStart{InputCount: proto.Int64(0), Harness: "no-longer-served"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			recorded := &countedRoutingBackend{Backend: local.New(echoagent.Harness{})}
			override := &blockedCreateBackend{Backend: local.New(echoagent.Harness{}), entered: make(chan struct{}), release: make(chan struct{})}
			unblock := sync.OnceFunc(func() { close(override.release) })
			t.Cleanup(unblock)
			t.Cleanup(func() { _ = recorded.Close(); _ = override.Close() })
			registry, err := placement.NewRegistry("recorded", map[string]*placement.Placer{
				"recorded": placement.New(recorded, echoagent.Model),
				"override": placement.New(override, echoagent.Model),
			})
			if err != nil {
				t.Fatal(err)
			}
			client := serveRegistry(t, store, registry)
			uid := mustCreate(t, client)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := log.Append(0, fence, api.Event{Kind: api.EventExecutionStart, ExecutionID: "old-interrupted", ExecutionStart: tc.invocation}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := client.Exec(ctx, &v1.ExecRequest{Session: uid, Harness: "override", Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "retry"))}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-override.entered:
			case <-ctx.Done():
				t.Fatal("Exec did not acquire the shared guard and reach Create")
			}
			before := routingRecords(t, log)
			_, err = client.Resume(ctx, &v1.ResumeRequest{Session: uid})
			if status.Code(err) != codes.Aborted {
				t.Errorf("overlapping Resume inspected pending evidence before checking busy: %v, want Aborted", err)
			}
			if !reflect.DeepEqual(before, routingRecords(t, log)) || recorded.describes.Load() != 0 || recorded.restores.Load() != 0 {
				t.Error("overlapping Resume changed journal or attempted default placement")
			}
			unblock()
			if err := drainExec(stream); err != nil {
				t.Fatalf("overlapping Resume disrupted Exec: %v", err)
			}
			if _, err := client.Resume(ctx, &v1.ResumeRequest{Session: uid}); err != nil {
				t.Fatalf("completed replacement invocation did not Resume after guard release: %v", err)
			}
		})
	}
}

func TestSessionOverlapReturnsAborted(t *testing.T) {
	h := &overlapHarness{entered: make(chan struct{}), release: make(chan struct{}, 1)}
	t.Cleanup(func() { close(h.release) })
	c := newClientWith(t, local.New(h))
	testSessionOverlap(t, c, h, "")
}

func TestSessionOverlapAcrossRegistryPlacers(t *testing.T) {
	h := &overlapHarness{entered: make(chan struct{}), release: make(chan struct{}, 1)}
	t.Cleanup(func() { close(h.release) })
	recorded, override := local.New(echoagent.Harness{}), local.New(h)
	t.Cleanup(func() { _ = recorded.Close() })
	t.Cleanup(func() { _ = override.Close() })
	registry, err := placement.NewRegistry("recorded", map[string]*placement.Placer{
		"recorded": placement.New(recorded, echoagent.Model),
		"override": placement.New(override, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}
	testSessionOverlap(t, newClientWithRegistry(t, registry), h, "override")
}

// A Placer added at runtime shares the session guard of the Placers given to NewRegistry. A turn
// through the added Placer holds the session, so the service's calls, which route to the recorded
// static harness, are refused with Aborted instead of running beside it.
func TestSessionOverlapWithAddedPlacer(t *testing.T) {
	h := &overlapHarness{entered: make(chan struct{}), release: make(chan struct{}, 1)}
	t.Cleanup(func() { close(h.release) })
	recorded, added := local.New(echoagent.Harness{}), local.New(h)
	t.Cleanup(func() { _ = recorded.Close() })
	t.Cleanup(func() { _ = added.Close() })
	registry, err := placement.NewRegistry("recorded", map[string]*placement.Placer{
		"recorded": placement.New(recorded, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}
	addedPlacer := placement.New(added, echoagent.Model)
	if err := registry.Add("added", addedPlacer); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := serveRegistry(t, store, registry)

	uid := mustCreate(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log := store.Session(uid)
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := addedPlacer.Exec(ctx, log, uid, []api.Message{*api.TextMessage("user", "first")}, head)
		first <- err
	}()
	select {
	case <-h.entered:
	case err := <-first:
		t.Fatalf("first execution ended before reaching the harness: %v", err)
	case <-ctx.Done():
		t.Fatal("first execution did not reach the harness")
	}
	before, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"Exec", "Suspend", "Resume"} {
		t.Run(operation, func(t *testing.T) {
			var err error
			switch operation {
			case "Exec":
				stream, execErr := c.Exec(ctx, &v1.ExecRequest{
					Session: uid,
					Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "overlap"))},
				})
				err = execErr
				if err == nil {
					err = drainExec(stream)
				}
			case "Suspend":
				_, err = c.Suspend(ctx, &v1.SuspendRequest{Session: uid})
			case "Resume":
				_, err = c.Resume(ctx, &v1.ResumeRequest{Session: uid})
			}
			if status.Code(err) != codes.Aborted {
				t.Errorf("overlapping %s: want Aborted, got %v", operation, err)
			}
			after, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
			if err != nil || after.GetLastSeq() != before.GetLastSeq() || after.GetComputeState() != before.GetComputeState() {
				t.Errorf("overlapping %s changed the journal projection: %v, %v", operation, after, err)
			}
		})
	}
	h.release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("rejected overlaps disrupted the first execution: %v", err)
	}
	if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: uid}); err != nil {
		t.Fatalf("suspend after execution: %v", err)
	}
}

// Exec uses the requested override; Suspend and Resume resolve the recorded harness instead.
func testSessionOverlap(t *testing.T, c v1.SessionsClient, h *overlapHarness, harness string) {
	t.Helper()
	uid := mustCreate(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := c.Exec(ctx, &v1.ExecRequest{
		Session: uid,
		Harness: harness,
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "first"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.entered:
	case <-ctx.Done():
		t.Fatal("first execution did not reach the harness")
	}
	before, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"Exec", "Suspend", "Resume"} {
		t.Run(operation, func(t *testing.T) {
			var err error
			switch operation {
			case "Exec":
				stream, execErr := c.Exec(ctx, &v1.ExecRequest{
					Session: uid,
					Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "overlap"))},
				})
				err = execErr
				if err == nil {
					err = drainExec(stream)
				}
			case "Suspend":
				_, err = c.Suspend(ctx, &v1.SuspendRequest{Session: uid})
			case "Resume":
				_, err = c.Resume(ctx, &v1.ResumeRequest{Session: uid})
			}
			if status.Code(err) != codes.Aborted {
				t.Errorf("overlapping %s: want Aborted, got %v", operation, err)
			}
			after, err := c.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
			if err != nil || after.GetLastSeq() != before.GetLastSeq() || after.GetComputeState() != before.GetComputeState() {
				t.Errorf("overlapping %s changed the journal projection: %v, %v", operation, after, err)
			}
		})
	}
	h.release <- struct{}{}
	if err := drainExec(first); err != nil {
		t.Fatalf("rejected overlaps disrupted the first execution: %v", err)
	}
	if _, err := c.Suspend(ctx, &v1.SuspendRequest{Session: uid}); err != nil {
		t.Fatalf("suspend after execution: %v", err)
	}
}
