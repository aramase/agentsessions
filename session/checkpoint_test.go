package session_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// memoryBackend presents the local backend as memory-capable and its harness as
// REQUIRES_MEMORY_SNAPSHOT, so a Fork of a session checkpoints the parent: the one operation that
// supersedes a running turn. onSnapshot runs inside Snapshot, standing in for a runtime checkpoint
// in progress.
type memoryBackend struct {
	*local.Backend
	onSnapshot func()
}

func (b *memoryBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	desc, err := b.Backend.Describe(ctx)
	desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
	return desc, err
}

func (b *memoryBackend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: true}
}

func (b *memoryBackend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	ref, err := b.Backend.Snapshot(ctx, in, kind)
	ref.ExternalURI, ref.Memory = "snap-"+in.ID, true
	return ref, err
}

// parkingModel parks the first model call until its turn is cancelled and answers the rest.
type parkingModel struct {
	once   sync.Once
	called chan struct{}
}

func (m *parkingModel) model(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
	first := false
	m.once.Do(func() { first = true; close(m.called) })
	if !first {
		return echoagent.Model(ctx, req)
	}
	<-ctx.Done()
	return api.ModelResponse{}, ctx.Err()
}

func checkpointClient(t *testing.T, backend placement.Backend, model *parkingModel) v1.SessionsClient {
	t.Helper()
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": placement.New(backend, model.model)})
	if err != nil {
		t.Fatal(err)
	}
	return serveRegistry(t, store, r)
}

func execHi(c v1.SessionsClient, sess string) error {
	stream, err := c.Exec(context.Background(), &v1.ExecRequest{
		Session: sess,
		Inputs:  []*v1.Message{wire.MessageToProto(api.TextMessage("user", "hi"))},
	})
	if err != nil {
		return err
	}
	return drainExec(stream)
}

func forkParent(c v1.SessionsClient, sess string) error {
	_, err := c.Fork(context.Background(), &v1.ForkRequest{Session: sess})
	return err
}

// A turn that a stateful fork supersedes is a lost race with a newer writer, which callers retry. It
// must surface as Aborted, as a fenced turn always has, not as Internal because its stream was
// closed.
func TestExecSupersededByAStatefulForkIsAborted(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	model := &parkingModel{called: make(chan struct{})}
	c := checkpointClient(t, &memoryBackend{Backend: inner}, model)
	sess := mustCreate(t, c)

	execErr := make(chan error, 1)
	go func() { execErr <- execHi(c, sess) }()
	select {
	case <-model.called:
	case <-time.After(10 * time.Second):
		t.Fatal("turn never reached the model call")
	}
	if err := forkParent(c, sess); err != nil {
		t.Fatalf("fork: %v", err)
	}
	select {
	case err := <-execErr:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("superseded Exec: want Aborted, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("superseded Exec never returned")
	}
}

// A turn that arrives while the session is being checkpointed is refused, retryably: by the session
// guard during a Suspend, by the checkpoint mark during a stateful fork.
func TestExecDuringACheckpointIsAborted(t *testing.T) {
	for _, tc := range []struct {
		name       string
		checkpoint func(v1.SessionsClient, string) error
	}{
		{"suspend", func(c v1.SessionsClient, sess string) error {
			_, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess})
			return err
		}},
		{"fork", forkParent},
	} {
		name, checkpoint := tc.name, tc.checkpoint
		t.Run(name, func(t *testing.T) {
			inner := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = inner.Close() })
			backend := &memoryBackend{Backend: inner}
			model := &parkingModel{called: make(chan struct{})}
			c := checkpointClient(t, backend, model)
			sess := mustCreate(t, c)

			var execErr error
			backend.onSnapshot = func() { execErr = execHi(c, sess) }
			if err := checkpoint(c, sess); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if status.Code(execErr) != codes.Aborted {
				t.Fatalf("Exec during the checkpoint: want Aborted, got %v", execErr)
			}
		})
	}
}

// liveBackend describes the harness without dialing it but reports a live harness, so every turn
// checks the harness again on its own connection before it mints a fence (placement.LiveDescriber).
type liveBackend struct{ *memoryBackend }

func (liveBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	desc, err := echoagent.Harness{}.Describe(ctx)
	desc.Capabilities.Resumability = api.ResumabilityRequiresMemorySnapshot
	return desc, err
}

func (liveBackend) DescribesLiveHarness() bool { return true }

// parkedDescribe is a harness whose Describe, which only the turn's re-check calls here, waits until
// the call is cancelled.
type parkedDescribe struct {
	echoagent.Harness
	once   sync.Once
	parked chan struct{}
}

func (h *parkedDescribe) Describe(ctx context.Context) (api.Descriptor, error) {
	h.once.Do(func() { close(h.parked) })
	<-ctx.Done()
	return api.Descriptor{}, ctx.Err()
}

// A stateful fork that ends a turn while the turn is still checking the harness on its connection
// supersedes it like any other turn: Aborted, not the Canceled of a caller that gave up.
func TestExecSupersededDuringTheHarnessCheckIsAborted(t *testing.T) {
	h := &parkedDescribe{parked: make(chan struct{})}
	inner := local.New(h)
	t.Cleanup(func() { _ = inner.Close() })
	c := checkpointClient(t, liveBackend{&memoryBackend{Backend: inner}}, &parkingModel{called: make(chan struct{})})
	sess := mustCreate(t, c)

	execErr := make(chan error, 1)
	go func() { execErr <- execHi(c, sess) }()
	select {
	case <-h.parked:
	case err := <-execErr:
		t.Fatalf("turn returned before checking the harness: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("turn never checked the harness on its connection")
	}
	if err := forkParent(c, sess); err != nil {
		t.Fatalf("fork: %v", err)
	}
	select {
	case err := <-execErr:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("Exec superseded during the harness check: want Aborted, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("superseded Exec never returned")
	}
}
