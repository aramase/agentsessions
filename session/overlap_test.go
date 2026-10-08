package session_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/runtime/local"
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

func TestSessionOverlapReturnsAborted(t *testing.T) {
	h := &overlapHarness{entered: make(chan struct{}), release: make(chan struct{}, 1)}
	t.Cleanup(func() { close(h.release) })
	c := newClientWith(t, local.New(h))
	uid := mustCreate(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := c.Exec(ctx, &v1.ExecRequest{
		Session: uid,
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
				t.Fatalf("overlapping %s: want Aborted, got %v", operation, err)
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
