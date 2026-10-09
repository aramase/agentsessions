package placement_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// A fork at TOOL_CALL must not re-drive the parent's effect in the child's dedup namespace.
func TestForkedToolIntentResumeFailsClosed(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, resultLocation := range []string{"none", "before fork", "after fork"} {
			withResult := resultLocation != "none"
			name := "start-marker"
			if legacy {
				name = "legacy"
			}
			name += "/" + resultLocation
			t.Run(name, func(t *testing.T) {
				store, err := sqlitelog.Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				tool := openDurableTool(t, filepath.Join(t.TempDir(), "tool.db"))
				p := newLocalPlacer(t, hostToolHarness{key: "shared-key"}, placement.WithToolExecutor(tool.exec))
				parent := store.Session("parent")
				if _, err := p.Exec(t.Context(), parent, "parent", []api.Message{*api.TextMessage("user", "charge")}, 0); err != nil {
					t.Fatal(err)
				}
				if legacy {
					// Older writers omitted EXECUTION_START; preserve the rest of a real execution.
					old := store.Session("legacy-parent")
					fence, err := old.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					var seq int64
					for _, rec := range toolRecords(t, parent) {
						if rec.Event.Kind == api.EventExecutionStart {
							continue
						}
						if _, err := old.Append(seq, fence, rec.Event); err != nil {
							t.Fatal(err)
						}
						seq++
					}
					parent = old
				}
				kind := api.EventToolCall
				if resultLocation == "before fork" {
					kind = api.EventToolResult
				}
				var cut int64
				for _, rec := range toolRecords(t, parent) {
					if rec.Event.Kind == kind {
						cut = rec.Seq
					}
				}
				if cut == 0 {
					t.Fatalf("no %s fork point", kind)
				}
				child := store.Session("child")
				if err := controller.Fork(parent, child, cut); err != nil {
					t.Fatal(err)
				}
				if resultLocation == "after fork" {
					// v0.1.x recovery could write the inherited intent's receipt after FORK.
					fence, err := child.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					for _, rec := range toolRecords(t, parent) {
						if rec.Event.Kind == api.EventToolResult {
							if _, err := child.Append(cut+1, fence, rec.Event); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				c, err := controller.New(child, echoagent.Model, controller.WithSessionUID("child"), controller.WithToolExecutor(tool.exec))
				if err != nil {
					t.Fatal(err)
				}
				resumed, resumeErr := c.Resume(t.Context(), hostToolHarness{key: "shared-key"})
				var effects int
				if err := tool.db.QueryRow("SELECT COUNT(*) FROM effects").Scan(&effects); err != nil {
					t.Fatal(err)
				}
				if effects != 1 {
					t.Fatalf("fork duplicated durable effect: effects=%d, Resume=%v, %v", effects, resumed, resumeErr)
				}
				if withResult {
					if resumeErr != nil || !resumed {
						t.Fatalf("completed pair Resume = %v, %v", resumed, resumeErr)
					}
					if outputs, err := c.Outputs(); err != nil || !reflect.DeepEqual(outputs, []string{"call-1:receipt-1"}) {
						t.Fatalf("inherited receipt Outputs = %v, %v", outputs, err)
					}
					if outputs, err := c.Replay(t.Context(), hostToolHarness{key: "shared-key"}); err != nil || !reflect.DeepEqual(outputs, []string{"call-1:receipt-1"}) {
						t.Fatalf("inherited receipt Replay = %v, %v", outputs, err)
					}
				} else {
					if resumed || !errors.Is(resumeErr, controller.ErrInheritedToolIntent) {
						t.Fatalf("inherited intent Resume = %v, %v; want inherited tool intent error", resumed, resumeErr)
					}
					if head, err := child.Head(); err != nil || head != cut+1 {
						t.Fatalf("rejected Resume changed child journal: head=%d, err=%v", head, err)
					}
				}
				toolRecords(t, child)
			})
		}
	}
}

// A later successful call must not hide an earlier handled failure's unresolved intent.
type handledToolFailureHarness struct{ hostToolHarness }

var errInterruptedTools = errors.New("interrupted after handling tool failure")
var errHandledTool = errors.New("tool completed its effect but failed to return")

func (handledToolFailureHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, _ = sink.ToolCall(ctx, hostToolCall("failed-key"))
	second := hostToolCall("successful-key")
	second.ID = "call-2"
	if _, err := sink.ToolCall(ctx, second); err != nil {
		return err
	}
	return errInterruptedTools
}

func TestForkedEarlierUnresolvedToolIntentFailsClosed(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tool := openDurableTool(t, filepath.Join(t.TempDir(), "tool.db"))
	executor := func(ctx context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
		result, err := tool.exec(ctx, scope, call)
		if err == nil && call.IdempotencyKey == "failed-key" {
			return api.ToolResult{}, errHandledTool
		}
		return result, err
	}
	parent := store.Session("parent")
	har := handledToolFailureHarness{}
	c, err := controller.New(parent, echoagent.Model, controller.WithSessionUID("parent"), controller.WithToolExecutor(executor))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(t.Context(), har, nil, 0); !errors.Is(err, errInterruptedTools) {
		t.Fatalf("Exec = %v, want interrupted tools", err)
	}
	head, err := parent.Head()
	if err != nil {
		t.Fatal(err)
	}
	child := store.Session("child")
	if err := controller.Fork(parent, child, head); err != nil {
		t.Fatal(err)
	}
	recovery, err := controller.New(child, echoagent.Model, controller.WithSessionUID("child"), controller.WithToolExecutor(executor))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := recovery.Resume(t.Context(), har); resumed || !errors.Is(err, controller.ErrInheritedToolIntent) {
		t.Errorf("Resume = %v, %v; want inherited intent error", resumed, err)
	}
	var effects int
	if err := tool.db.QueryRow("SELECT COUNT(*) FROM effects").Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 2 {
		t.Fatalf("inherited handled failure duplicated an effect: effects=%d, want 2", effects)
	}
}

func TestForkChildOwnToolIntentResumeDedups(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tool := openDurableTool(t, filepath.Join(t.TempDir(), "tool.db"))
	p := newLocalPlacer(t, hostToolHarness{key: "shared-key"}, placement.WithToolExecutor(tool.exec))
	parent := store.Session("parent")
	if _, err := p.Exec(t.Context(), parent, "parent", nil, 0); err != nil {
		t.Fatal(err)
	}
	head, err := parent.Head()
	if err != nil {
		t.Fatal(err)
	}
	child := store.Session("child")
	if err := controller.Fork(parent, child, head); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(t.Context(), &failToolResultLog{Store: child}, "child", nil, head+1); !errors.Is(err, errToolResultAppend) {
		t.Fatalf("child Exec = %v, want result append failure", err)
	}
	if err := p.Resume(t.Context(), child, "child"); err != nil {
		t.Fatalf("child-owned intent must remain resumable: %v", err)
	}
	var effects int
	if err := tool.db.QueryRow("SELECT COUNT(*) FROM effects").Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 2 {
		t.Fatalf("child re-drive duplicated effect: effects=%d, want 2", effects)
	}
	toolRecords(t, child)
}
