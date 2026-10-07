package controller_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

func TestMarkerlessLegacyInvocationResumesAndReplaysWithDefaults(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	// A literal old-writer prefix, not an invocation emitted by the current controller.
	if _, err := log.Append(0, fence, api.Event{
		ExecutionID: "legacy", Kind: api.EventInput, Message: api.TextMessage("user", "hello"),
	}); err != nil {
		t.Fatal(err)
	}
	c, err := controller.New(log, echoModel, controller.WithStart([]byte("replacement"), 99))
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	har := executionConfigHarness{&starts}
	if resumed, err := c.Resume(t.Context(), har); err != nil || !resumed {
		t.Fatalf("legacy Resume = %v, %v", resumed, err)
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Replay(t.Context(), har); err != nil {
		t.Fatalf("legacy Replay = %v", err)
	}
	if len(starts) != 2 {
		t.Fatalf("harness runs = %d, want Resume and Replay", len(starts))
	}
	for _, start := range starts {
		if start.ExecutionID != "legacy" || !bytes.Equal(start.Config, nil) || start.ResumeFromSeq != 0 ||
			!reflect.DeepEqual(messageTexts(start.Inputs), []string{"hello"}) || len(start.History) != 0 {
			t.Fatalf("legacy Start = %#v", start)
		}
	}
	after, err := log.Read(1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy Replay changed log: %v", err)
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}
