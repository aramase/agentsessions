package approvalfixture

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
)

// Reopening the executor must retain dedup, but never share a harness key across sessions.
func TestEffectsDurableSessionKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.db")
	e, err := OpenEffects(path)
	if err != nil {
		t.Fatal(err)
	}
	call := api.ToolCall{ID: "fixture-call", Tool: "fixture-effect", IdempotencyKey: "shared-key", Args: map[string]any{"input": "original"}}
	first, err := e.Execute(t.Context(), controller.ToolCallContext{SessionUID: "one"}, call)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEffects(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	changed := call
	changed.Args = map[string]any{"input": "replacement", "extra": "must-not-leak"}
	retry, err := e.Execute(t.Context(), controller.ToolCallContext{SessionUID: "one"}, changed)
	if err != nil || !reflect.DeepEqual(first, retry) {
		t.Fatalf("retry=%+v %v", retry, err)
	}
	if _, err := e.Execute(t.Context(), controller.ToolCallContext{SessionUID: "two"}, call); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{"one", "two"} {
		if n, err := e.Count(uid); err != nil || n != 1 {
			t.Fatalf("%s effects=%d %v", uid, n, err)
		}
	}
}
