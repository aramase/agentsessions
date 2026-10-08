package placement_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// Pause the real first Read after taking its snapshot, exposing the routing-to-operation window.
// Registry.Resume must already hold the same guard used by the other Placer's Exec.
type pausedResumeQuery struct {
	eventlog.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *pausedResumeQuery) Read(from int64) ([]eventlog.Record, error) {
	recs, err := l.Store.Read(from)
	l.once.Do(func() {
		close(l.entered)
		<-l.release
	})
	return recs, err
}

func TestRegistryResumeGuardsInvocationSelectionThroughRecovery(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("registry-resume")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	call := hostToolCall("original-key")
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "recorded-alias"}},
		{Kind: api.EventInput, Message: api.TextMessage("user", "charge")},
		{Kind: api.EventToolCall, ToolCall: &call},
	}
	for i, ev := range events {
		ev.ExecutionID = "pending"
		if _, err := log.Append(int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
	}
	before := toolRecords(t, log)
	recorded := newLocalPlacer(t, hostToolHarness{key: "original-key"}, placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
		return api.ToolResult{Output: map[string]any{"receipt": "recovered"}}, nil
	}))
	override := newPlacer(t)
	r, err := placement.NewRegistry("override", map[string]*placement.Placer{"override": override, "recorded-alias": recorded})
	if err != nil {
		t.Fatal(err)
	}
	query := &pausedResumeQuery{Store: log, entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(query.release) })
	defer unblock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Resume(ctx, query, "registry-resume", "override") }()
	select {
	case <-query.entered:
	case <-ctx.Done():
		t.Fatal("Resume did not reach invocation selection")
	}
	if _, err := override.Exec(ctx, log, "registry-resume", nil, int64(len(before)), placement.WithHarness("override")); !errors.Is(err, placement.ErrSessionBusy) {
		t.Errorf("Exec raced Resume's routing query: %v, want ErrSessionBusy", err)
	}
	if !reflect.DeepEqual(before, toolRecords(t, log)) {
		t.Error("contending Exec superseded the journal used to select Resume's harness")
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Resume lost its recorded route: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Resume did not complete")
	}
	recs := toolRecords(t, log)
	output := toolEvent(t, recs, api.EventOutput).Message
	if output == nil || output.Text() != "call-1:recovered" {
		t.Fatalf("Resume did not recover the selected invocation: %+v", recs)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := override.Exec(ctx, log, "registry-resume", nil, head, placement.WithHarness("override")); err != nil {
		t.Fatalf("Registry Resume retained its guard after completion: %v", err)
	}
}

// WithResolvedHarnessCheck sees the name Resume resolved, the recorded one rather than the
// session's default, and its error stops recovery before anything is restored or journaled.
func TestRegistryResumeResolvedHarnessCheck(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("checked")
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	events := []api.Event{
		{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: proto.Int64(1), Harness: "recorded-alias"}},
		{Kind: api.EventInput, Message: api.TextMessage("user", "hi")},
	}
	for i, ev := range events {
		ev.ExecutionID = "pending"
		if _, err := log.Append(int64(i), fence, ev); err != nil {
			t.Fatal(err)
		}
	}
	r, err := placement.NewRegistry("default", map[string]*placement.Placer{"default": newPlacer(t), "recorded-alias": newPlacer(t)})
	if err != nil {
		t.Fatal(err)
	}
	refused := errors.New("refused by the host")
	var seen []string
	check := func(name string) error {
		seen = append(seen, name)
		return refused
	}
	if err := r.Resume(t.Context(), log, "checked", "default", placement.WithResolvedHarnessCheck(check)); err != refused {
		t.Fatalf("Resume = %v, want the check's error unchanged", err)
	}
	if !reflect.DeepEqual(seen, []string{"recorded-alias"}) {
		t.Fatalf("check saw %v, want the recorded name only", seen)
	}
	if head, err := log.Head(); err != nil || head != int64(len(events)) {
		t.Fatalf("a refused Resume changed the log: head %d, %v", head, err)
	}
	// The guard is released: a Resume without the refusal proceeds past routing.
	if err := r.Resume(t.Context(), log, "checked", "default", placement.WithResolvedHarnessCheck(func(string) error { return nil })); errors.Is(err, placement.ErrSessionBusy) {
		t.Fatalf("the refused Resume kept the session guard: %v", err)
	}
}
