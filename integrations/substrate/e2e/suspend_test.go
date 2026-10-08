package e2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
)

// TestSessionSuspendResumeOnGVisor exercises session-level suspension on real substrate:
// the snapshot must retain the actor that explicit Resume needs, then allow another turn.
func TestSessionSuspendResumeOnGVisor(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-suspend"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(t, echoTemplateSpec, desc)
	session := uniqueUID("suspend")
	defer stopQuietly(t, backend, session)
	log := journal(t).Session(session)
	p := placement.New(backend, echoagent.Model)

	inc, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "before")}, 0)
	if err != nil {
		t.Fatalf("pre-suspend exec: %v", err)
	}
	if got, want := lastOutput(t, log), "echo:before"; got != want {
		t.Fatalf("pre-suspend output=%q want %q", got, want)
	}
	beforeSuspend, err := log.Head()
	if err != nil {
		t.Fatalf("head before suspend: %v", err)
	}

	ref, err := p.Suspend(ctx, log, session)
	if err != nil {
		t.Fatalf("session suspend: %v", err)
	}
	if ref.Local != session || ref.ExternalURI == "" {
		t.Fatalf("suspend ref must retain actor %q and external snapshot: %+v", session, ref)
	}
	state, err := backend.Status(ctx, inc)
	if err != nil {
		t.Fatalf("actor status after suspend: %v", err)
	}
	if state != api.ComputeCold {
		t.Fatalf("actor state after suspend=%s want %s", state, api.ComputeCold)
	}
	recs, err := log.Read(beforeSuspend + 1)
	if err != nil {
		t.Fatalf("read suspend boundary: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("suspend appended %d records, want one SUSPEND", len(recs))
	}
	ev := recs[0].Event
	if ev.Kind != api.EventLifecycle || ev.Lifecycle == nil || ev.Lifecycle.Kind != api.LifecycleSuspend || ev.Lifecycle.Snapshot == nil || *ev.Lifecycle.Snapshot != ref {
		t.Fatalf("SUSPEND must journal the exact returned snapshot ref: %+v", ev)
	}

	if err := p.Resume(ctx, log, session); err != nil {
		t.Fatalf("session resume from retained actor: %v", err)
	}
	state, err = backend.Status(ctx, inc)
	if err != nil {
		t.Fatalf("actor status after resume: %v", err)
	}
	if state != api.ComputeLive {
		t.Fatalf("actor state after resume=%s want %s", state, api.ComputeLive)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatalf("head after resume: %v", err)
	}
	if _, err := p.Exec(ctx, log, session, []api.Message{*api.TextMessage("user", "after")}, head); err != nil {
		t.Fatalf("post-resume exec: %v", err)
	}
	recs, err = log.Read(1)
	if err != nil {
		t.Fatalf("read journal after continuation: %v", err)
	}
	if got, want := outputsOf(recs), []string{"echo:before", "echo:after"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("outputs across suspend/resume=%v want %v", got, want)
	}
	var kinds []string
	for _, rec := range recs[beforeSuspend:] {
		if rec.Event.Kind == api.EventLifecycle && rec.Event.Lifecycle != nil {
			kinds = append(kinds, string(rec.Event.Lifecycle.Kind))
		} else {
			kinds = append(kinds, string(rec.Event.Kind))
		}
	}
	if want := []string{"SUSPEND", "RESUME", "EXECUTION_START", "INPUT", "MODEL_CALL", "OUTPUT", "END"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("suspend/resume record order=%v want %v", kinds, want)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("chain verify across suspend/resume and continuation: %v", err)
	}
	t.Log("actor retained cold, explicitly resumed, continued with expected outputs and verified chain")
}
