package e2e

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/wire"
)

// TestHarnessStreamIdlePastRouteTimeout holds one harnesswire Connect stream open and idle through
// the atenet-router for longer than the router's route timeout (5m by default, --route-timeout), the
// way a turn parked on a slow model call does: the harness has sent its model call and waits for the
// host's reply, and nothing crosses the stream until the host answers.
//
// It drives the stream with a raw harnesswire client rather than the Placer so it can keep a Recv
// pending the whole time and timestamp the moment the stream ends. (The Placer does not read while
// the model call is in flight, so through it the turn only fails when the host next sends.)
//
// It records what the router does to such a stream and fails only on a hang. It is opt-in because it
// runs for the whole gap: set E2E_IDLE_GAP (for example 5m30s) to run it.
func TestHarnessStreamIdlePastRouteTimeout(t *testing.T) {
	gapSetting := env("E2E_IDLE_GAP", "")
	if gapSetting == "" {
		t.Skip("set E2E_IDLE_GAP (e.g. 5m30s) to hold a harness stream idle across the router's route timeout")
	}
	gap, err := time.ParseDuration(gapSetting)
	if err != nil {
		t.Fatalf("E2E_IDLE_GAP=%q: %v", gapSetting, err)
	}
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-idle"))
	ctx, cancel := context.WithTimeout(context.Background(), gap+10*time.Minute)
	defer cancel()

	desc := api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}
	backend := f.backend(t, echoTemplateSpec, desc)
	session := uniqueUID("idle")
	inc, err := backend.Create(ctx, &api.SessionSpec{SessionUID: session})
	if inc.ID != "" {
		defer stopQuietly(t, backend, inc.ID)
	}
	if err != nil {
		t.Fatalf("place actor: %v", err)
	}

	conn, err := grpc.NewClient(inc.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	streamCtx := metadata.NewOutgoingContext(ctx, metadata.New(inc.CallMetadata))
	stream, err := v1.NewHarnessClient(conn).Connect(streamCtx)
	if err != nil {
		t.Fatalf("open Connect through the router: %v", err)
	}
	const execID = "idle-exec"
	if err := stream.Send(&v1.ControllerFrame{
		ExecutionId: execID,
		Frame: &v1.ControllerFrame_Start{Start: &v1.Start{
			Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "idle"))},
		}},
	}); err != nil {
		t.Fatalf("send Start: %v", err)
	}
	call, err := stream.Recv()
	if err != nil || call.GetKind() != v1.EventKind_EVENT_MODEL_CALL {
		t.Fatalf("want the harness's model call, got %v, err=%v", call, err)
	}
	idleSince := time.Now()
	t.Logf("harness sent its model call; holding the stream idle for %s", gap)

	type recvResult struct {
		ev  *v1.Event
		err error
		at  time.Duration
	}
	ended := make(chan recvResult, 1)
	go func() {
		ev, err := stream.Recv()
		ended <- recvResult{ev: ev, err: err, at: time.Since(idleSince)}
	}()

	select {
	case r := <-ended:
		st, _ := status.FromError(r.err)
		t.Logf("OUTCOME: stream torn down while idle, after %s. grpc code=%s message=%q (event=%v)",
			r.at.Round(time.Second), st.Code(), st.Message(), r.ev)
		if r.err == nil {
			t.Fatalf("the harness sent %v unprompted while waiting for its model result", r.ev)
		}
		return
	case <-time.After(gap):
	case <-ctx.Done():
		t.Fatal("test deadline expired while the stream was idle")
	}

	// Still open after the gap: answer the model call and require the turn to finish normally.
	if err := stream.Send(&v1.ControllerFrame{
		ExecutionId: execID,
		Frame: &v1.ControllerFrame_Model{Model: &v1.ModelResult{
			ModelCallId: call.GetModel().GetId(),
			Message:     wire.MessageToProto(api.TextMessage("assistant", "late")),
		}},
	}); err != nil {
		t.Fatalf("stream looked open after %s but the reply failed: %v", gap, err)
	}
	select {
	case r := <-ended:
		t.Logf("OUTCOME: stream survived %s idle; after the reply the harness sent %v (err=%v)", gap, r.ev.GetKind(), r.err)
	case <-time.After(time.Minute):
		t.Fatal("the harness did not finish within a minute of a late model reply")
	}
}
