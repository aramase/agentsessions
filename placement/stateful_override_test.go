package placement_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/harness/counteragent"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// memLocal is the local backend presented as one that can snapshot memory, so CanPlace admits a
// REQUIRES_MEMORY_SNAPSHOT harness and only the override rule can refuse it. It counts Create and
// Restore calls, and reports itself as live-describing when live is set.
type memLocal struct {
	*local.Backend
	live      bool
	provision *atomic.Int32
}

func (b memLocal) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: true}
}

func (b memLocal) DescribesLiveHarness() bool { return b.live }

func (b memLocal) Create(ctx context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	b.provision.Add(1)
	return b.Backend.Create(ctx, s)
}

func (b memLocal) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	b.provision.Add(1)
	return b.Backend.Restore(ctx, ref)
}

// A turn that overrides the session's harness cannot run on a REQUIRES_MEMORY_SNAPSHOT harness:
// Suspend and Fork route by the session's own harness and would leave that turn's RAM state
// unsnapshotted and uncloned. The Placer refuses it with ErrStatefulOverride before it provisions
// compute or writes to the log, whether the backend declares the tier or the turn's own connection
// reports it (a live-describing backend). Without WithOverride the same harness runs.
func TestOverrideOntoAMemoryHarnessIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		live bool
	}{
		{name: "declared by the backend"},
		{name: "reported on the turn's connection", live: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The live backend describes a stateless harness; the turn's connection reaches the
			// memory harness, so only the re-check on that connection sees the tier.
			var described api.Harness = &counteragent.Harness{}
			if tc.live {
				described = echoagent.Harness{}
			}
			inner := local.New(described)
			t.Cleanup(func() { _ = inner.Close() })
			var provision atomic.Int32
			dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return &counteragent.Harness{}, func() error { return nil }, nil
			})
			p := placement.New(memLocal{Backend: inner, live: tc.live, provision: &provision}, echoagent.Model, dial)
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			log := store.Session("s")

			_, err = p.Exec(t.Context(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0, placement.WithOverride())
			if !errors.Is(err, placement.ErrStatefulOverride) {
				t.Fatalf("override Exec onto a memory harness: got %v, want ErrStatefulOverride", err)
			}
			if head, _ := log.Head(); head != 0 {
				t.Fatalf("a refused override wrote to the log: head %d", head)
			}
			if !tc.live && provision.Load() != 0 {
				t.Fatalf("a refused override provisioned compute %d time(s)", provision.Load())
			}

			if _, err := p.Exec(t.Context(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
				t.Fatalf("Exec on the session's own memory harness: %v", err)
			}
		})
	}
}

// Registry.Resume of a pending turn recorded on a memory harness other than the session's own is
// refused the same way, before anything is restored. A pending turn recorded under an alias of the
// session's harness shares its Placer and is not an override.
func TestRegistryResumeRefusesAMemoryOverrideTurn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session string
		want    error
	}{
		{name: "session on another harness", session: "echo", want: placement.ErrStatefulOverride},
		{name: "session on an alias of the memory harness", session: "mem-alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			echo := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = echo.Close() })
			inner := local.New(&counteragent.Harness{})
			t.Cleanup(func() { _ = inner.Close() })
			var provision atomic.Int32
			dial := placement.WithDialer(func(api.Incarnation) (api.Harness, func() error, error) {
				return &counteragent.Harness{}, func() error { return nil }, nil
			})
			mem := placement.New(memLocal{Backend: inner, provision: &provision}, echoagent.Model, dial)
			r, err := placement.NewRegistry("echo", map[string]*placement.Placer{
				"echo":      placement.New(echo, echoagent.Model),
				"mem":       mem,
				"mem-alias": mem,
			})
			if err != nil {
				t.Fatal(err)
			}
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			log := store.Session("s")
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range []api.Event{
				{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "mem"}},
				{Kind: api.EventInput, Message: api.TextMessage("user", "hi")},
			} {
				ev.ExecutionID = "pending"
				if _, err := log.Append(int64(i), fence, ev); err != nil {
					t.Fatal(err)
				}
			}
			head, err := log.Head()
			if err != nil {
				t.Fatal(err)
			}

			err = r.Resume(t.Context(), log, "s", tc.session)
			if tc.want == nil {
				if errors.Is(err, placement.ErrStatefulOverride) {
					t.Fatalf("Resume under an alias of the session's harness: got %v, want no ErrStatefulOverride", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Resume: got %v, want %v", err, tc.want)
			}
			if got, _ := log.Head(); got != head {
				t.Fatalf("a refused Resume wrote to the log: head %d -> %d", head, got)
			}
			if provision.Load() != 0 {
				t.Fatalf("a refused Resume restored compute %d time(s)", provision.Load())
			}
		})
	}
}
