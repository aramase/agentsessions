package e2e

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

// TestConcurrentSessionsReachTheirOwnActors runs two sessions' calls through the router at once and
// requires each to reach its own actor. Every actor shares the router's address, so the
// ate-target-actor metadata on each call is all that keeps the sessions apart.
//
// It uses the in-RAM counter because its answer says which actor served the call: the two sessions
// are driven to different counts first (A to 2, B to 1), so a call that reached the other session's
// actor answers with the wrong number. With a router connection open for each session, each then
// gets one more turn, both started together, and each host-side Output is held until the other
// session's Output has arrived too, so both calls are in flight through the router at the same time.
func TestConcurrentSessionsReachTheirOwnActors(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-concurrent"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	backend := f.backend(t, counterTemplateSpec, counterDescriptor)
	store := journal(t)
	p := placement.New(backend, echoagent.Model)

	sessions := []struct {
		uid   string
		turns int
		inc   api.Incarnation
	}{
		{uid: uniqueUID("concurrent-a"), turns: 2},
		{uid: uniqueUID("concurrent-b"), turns: 1},
	}
	for i := range sessions {
		s := &sessions[i]
		defer stopQuietly(t, backend, s.uid)
		log := store.Session(s.uid)
		for turn := range s.turns {
			head, err := log.Head()
			if err != nil {
				t.Fatalf("head: %v", err)
			}
			if s.inc, err = p.Exec(ctx, log, s.uid, []api.Message{*api.TextMessage("user", "inc")}, head); err != nil {
				t.Fatalf("session %s turn %d of %d: %v", s.uid, turn+1, s.turns, err)
			}
		}
		if got, want := lastOutput(t, log), strconv.Itoa(s.turns); got != want {
			t.Fatalf("session %s count=%s want %s before the concurrent turns", s.uid, got, want)
		}
		t.Logf("session %s: actor %v at count %d", s.uid, s.inc.CallMetadata, s.turns)
	}

	// Open both router connections before either call starts, so the two harness clients are live
	// side by side, as they are in a host serving both sessions.
	harnesses := make([]api.Harness, len(sessions))
	for i, s := range sessions {
		har, closeHar, err := dialActor(s.inc)
		if err != nil {
			t.Fatalf("dial session %s's actor: %v", s.uid, err)
		}
		defer func() { _ = closeHar() }()
		harnesses[i] = har
	}

	// release opens once every session's Output has reached the host.
	var arrivedMu sync.Mutex
	arrived := 0
	release := make(chan struct{})
	outputs := make([]string, len(sessions))
	errs := make([]error, len(sessions))
	var wg sync.WaitGroup
	for i := range sessions {
		wg.Go(func() {
			sink := &heldOutputSink{onOutput: func(ctx context.Context, text string) error {
				outputs[i] = text
				arrivedMu.Lock()
				arrived++
				if arrived == len(sessions) {
					close(release)
				}
				arrivedMu.Unlock()
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}}
			start := &api.Start{ExecutionID: "concurrent-" + strconv.Itoa(i), Inputs: []api.Message{*api.TextMessage("user", "inc")}}
			errs[i] = harnesses[i].Run(ctx, start, sink)
		})
	}
	wg.Wait()

	for i, s := range sessions {
		if errs[i] != nil {
			t.Fatalf("concurrent turn on session %s: %v", s.uid, errs[i])
		}
		if want := strconv.Itoa(s.turns + 1); outputs[i] != want {
			t.Errorf("session %s answered %q, want %q: the call did not reach its own actor", s.uid, outputs[i], want)
		}
	}
	t.Logf("concurrent turns through the router: %s=%s, %s=%s", sessions[0].uid, outputs[0], sessions[1].uid, outputs[1])
}

var errUnexpectedCall = errors.New("the counter harness only emits Output")

// heldOutputSink hands each Output to onOutput and refuses every other call: the counter harness only
// emits Output.
type heldOutputSink struct {
	onOutput func(context.Context, string) error
}

func (s *heldOutputSink) Model(context.Context, api.ModelRequest) (api.ModelResponse, error) {
	return api.ModelResponse{}, errUnexpectedCall
}
func (s *heldOutputSink) Output(ctx context.Context, text string) error { return s.onOutput(ctx, text) }
func (s *heldOutputSink) ToolCall(context.Context, api.ToolCall) (api.ToolResult, error) {
	return api.ToolResult{}, errUnexpectedCall
}
func (s *heldOutputSink) Report(context.Context, api.ToolResult) error { return errUnexpectedCall }
func (s *heldOutputSink) Usage(context.Context, api.Usage) error       { return nil }
