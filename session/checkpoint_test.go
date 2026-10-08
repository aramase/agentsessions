package session_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

// hookedBackend runs onSnapshot inside Snapshot, standing in for a runtime checkpoint in progress,
// and fails Fork with forkErr when it is set.
type hookedBackend struct {
	*local.Backend
	onSnapshot func()
	forkErr    error
}

func (b *hookedBackend) Fork(ctx context.Context, ref api.SnapshotRef, opts api.ForkOpts) (api.Incarnation, error) {
	if b.forkErr != nil {
		return api.Incarnation{}, b.forkErr
	}
	return b.Backend.Fork(ctx, ref, opts)
}

func (b *hookedBackend) Snapshot(ctx context.Context, in api.Incarnation, kind api.SnapshotKind) (api.SnapshotRef, error) {
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	return b.Backend.Snapshot(ctx, in, kind)
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

// A turn that Suspend supersedes is a lost race with a newer writer, which callers retry. It must
// surface as Aborted, as a fenced turn always has, not as Internal because its stream was closed.
func TestExecSupersededBySuspendIsAborted(t *testing.T) {
	backend := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = backend.Close() })
	model := &parkingModel{called: make(chan struct{})}
	c := checkpointClient(t, backend, model)
	sess := mustCreate(t, c)

	execErr := make(chan error, 1)
	go func() { execErr <- execHi(c, sess) }()
	select {
	case <-model.called:
	case <-time.After(10 * time.Second):
		t.Fatal("turn never reached the model call")
	}
	if _, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
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

// A turn that arrives while the session is being checkpointed is refused, retryably.
func TestExecDuringACheckpointIsAborted(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	backend := &hookedBackend{Backend: inner}
	model := &parkingModel{called: make(chan struct{})}
	c := checkpointClient(t, backend, model)
	sess := mustCreate(t, c)

	var execErr error
	backend.onSnapshot = func() { execErr = execHi(c, sess) }
	if _, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if status.Code(execErr) != codes.Aborted {
		t.Fatalf("Exec during the checkpoint: want Aborted, got %v", execErr)
	}
}

// A runtime that finds the fork's source snapshot superseded (the parent moved on between the
// checkpoint and the clone) has lost a race the caller can retry at the new head. It must surface as
// Aborted, not Internal.
func TestForkWithASupersededSnapshotIsAborted(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	backend := &hookedBackend{Backend: inner, forkErr: fmt.Errorf("runtime: tag parent: %w", api.ErrSnapshotSuperseded)}
	c := checkpointClient(t, backend, &parkingModel{called: make(chan struct{})})
	sess := mustCreate(t, c)

	_, err := c.Fork(context.Background(), &v1.ForkRequest{Session: sess})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("fork with a superseded snapshot: want Aborted, got %v", err)
	}
}

// statefulBackend reports a REQUIRES_MEMORY_SNAPSHOT harness on a memory-capable runtime, so a Fork
// takes the stateful path that checkpoints the parent.
type statefulBackend struct{ *hookedBackend }

func (statefulBackend) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}, nil
}

func (statefulBackend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: true}
}

// A Suspend or a stateful Fork that arrives while another checkpoint owns the session is refused
// with no side effects. The caller retries once the checkpoint is done, so both surface as Aborted.
func TestSuspendAndForkDuringACheckpointAreAborted(t *testing.T) {
	inner := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = inner.Close() })
	hooked := &hookedBackend{Backend: inner}
	c := checkpointClient(t, statefulBackend{hooked}, &parkingModel{called: make(chan struct{})})
	sess := mustCreate(t, c)

	var suspendErr, forkErr error
	var ran atomic.Bool // not sync.Once: a nested checkpoint that is not refused re-enters this hook
	hooked.onSnapshot = func() {
		if !ran.CompareAndSwap(false, true) {
			return
		}
		_, suspendErr = c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess})
		_, forkErr = c.Fork(context.Background(), &v1.ForkRequest{Session: sess})
	}
	if _, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if status.Code(suspendErr) != codes.Aborted {
		t.Fatalf("Suspend during the checkpoint: want Aborted, got %v", suspendErr)
	}
	if status.Code(forkErr) != codes.Aborted {
		t.Fatalf("Fork during the checkpoint: want Aborted, got %v", forkErr)
	}
}

// liveBackend describes the harness without dialing it but reports a live harness, so every turn
// checks the harness again on its own connection before it mints a fence (placement.LiveDescriber).
type liveBackend struct{ *local.Backend }

func (liveBackend) Describe(ctx context.Context) (api.Descriptor, error) {
	return echoagent.Harness{}.Describe(ctx)
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

// A checkpoint that ends a turn while the turn is still checking the harness on its connection
// supersedes it like any other turn: Aborted, not the Canceled of a caller that gave up.
func TestExecSupersededDuringTheHarnessCheckIsAborted(t *testing.T) {
	h := &parkedDescribe{parked: make(chan struct{})}
	inner := local.New(h)
	t.Cleanup(func() { _ = inner.Close() })
	c := checkpointClient(t, liveBackend{inner}, &parkingModel{called: make(chan struct{})})
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
	if _, err := c.Suspend(context.Background(), &v1.SuspendRequest{Session: sess}); err != nil {
		t.Fatalf("suspend: %v", err)
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
