package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ActorTemplates are substrate API resources living in an atespace, not Kubernetes objects, so the
// suite creates the ones it needs through Control instead of the workflow applying manifests. The
// WorkerPools they select are still Kubernetes objects (deploy/substrate/*-workerpool.yaml).
//
// Both templates run the same harnessnode image (HARNESS_IMAGE, pinned by digest as substrate
// requires); HARNESS_KIND picks the echo or the counter harness inside it.

// harnessContainer is the harnessnode container every template runs.
func harnessContainer(kind string) *atepb.Container {
	c := &atepb.Container{
		Name:    "harness",
		Image:   env("HARNESS_IMAGE", ""),
		Command: []string{"/ko-app/harnessnode"},
		// ResumeActor blocks on this probe, so a placed actor's harness is already serving. It also
		// lets the template controller take the golden snapshot as soon as the harness is ready.
		WakeupProbe: &atepb.ContainerWakeupProbe{
			HttpGet:        &atepb.HTTPGetAction{Path: "/readyz", Port: 8081},
			TimeoutSeconds: 120,
		},
		Resources: &atepb.Resources{Limits: []*atepb.Limits{
			{Name: "cpu", Quantity: "1"},
			{Name: "memory", Quantity: "512Mi"},
		}},
	}
	if kind != "" {
		c.Env = []*atepb.EnvVar{{Name: "HARNESS_KIND", Value: kind}}
	}
	return c
}

// echoTemplateSpec is the stateless tier: the echo harness on gVisor. A STATELESS_REPLAY harness keeps
// nothing in RAM the journal lacks, so the snapshot scope barely matters; FULL is what lets the
// template build the golden snapshot new actors start from.
func echoTemplateSpec(atespace string) *atepb.ActorTemplate {
	return &atepb.ActorTemplate{
		Metadata:       &atepb.ResourceMetadata{Atespace: atespace, Name: echoTemplate},
		WorkerSelector: &atepb.Selector{MatchLabels: map[string]string{"workload": "agentsessions-echo-harness"}},
		Containers:     []*atepb.Container{harnessContainer("")},
		SnapshotConfig: &atepb.SnapshotConfig{
			OnCommit:        atepb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			StorageLocation: env("SNAPSHOT_LOCATION", "gs://ate-snapshots/agentsessions/") + echoTemplate + "/",
		},
		SandboxConfig: &atepb.SandboxConfig{
			SandboxClass: atepb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   env("ECHO_SANDBOX_CONFIG", "gvisor-default"),
		},
	}
}

// counterTemplateSpec is the stateful tier: the in-RAM counter. FULL capture is load-bearing here:
// the count exists only in guest RAM, so only a memory snapshot carries it across suspend and fork.
// CI runs it on a micro-VM; COUNTER_SANDBOX_CLASS=gvisor runs it on gVisor, whose FULL checkpoint
// also captures memory, for hosts without KVM.
func counterTemplateSpec(atespace string) *atepb.ActorTemplate {
	class := atepb.SandboxClass_SANDBOX_CLASS_MICROVM
	config := env("COUNTER_SANDBOX_CONFIG", "microvm")
	pool := "agentsessions-counter-microvm"
	if env("COUNTER_SANDBOX_CLASS", "microvm") == "gvisor" {
		class = atepb.SandboxClass_SANDBOX_CLASS_GVISOR
		config = env("COUNTER_SANDBOX_CONFIG", "gvisor-default")
		pool = "agentsessions-counter-gvisor"
	}
	return &atepb.ActorTemplate{
		Metadata:       &atepb.ResourceMetadata{Atespace: atespace, Name: counterTemplate},
		WorkerSelector: &atepb.Selector{MatchLabels: map[string]string{"workload": pool}},
		Containers:     []*atepb.Container{harnessContainer("counter")},
		SnapshotConfig: &atepb.SnapshotConfig{
			OnCommit:        atepb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			StorageLocation: env("SNAPSHOT_LOCATION", "gs://ate-snapshots/agentsessions/") + counterTemplate + "/",
		},
		SandboxConfig: &atepb.SandboxConfig{SandboxClass: class, ConfigName: config},
	}
}

var templateOnce sync.Map // template name -> *sync.Once, so each binary builds a template once

// ensureTemplate makes sure the template exists with this run's harness image and that its golden
// snapshot is built. Waiting for the golden matters for more than speed: the template controller
// builds it on a temporary actor that holds one of the pool's workers until it is done.
func (f *fixture) ensureTemplate(t *testing.T, spec func(string) *atepb.ActorTemplate) {
	t.Helper()
	want := spec(f.tmplAtespace)
	once, _ := templateOnce.LoadOrStore(want.GetMetadata().GetName(), &sync.Once{})
	var failure string
	once.(*sync.Once).Do(func() { failure = f.createTemplate(t, want) })
	if failure != "" {
		t.Fatal(failure)
	}
}

func (f *fixture) createTemplate(t *testing.T, want *atepb.ActorTemplate) string {
	if want.GetContainers()[0].GetImage() == "" {
		return "HARNESS_IMAGE is not set; the workflow passes the digest-pinned harnessnode image it built"
	}
	ctl := atepb.NewControlClient(f.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, err := ctl.CreateAtespace(ctx, &atepb.CreateAtespaceRequest{
		Atespace: &atepb.Atespace{Metadata: &atepb.ResourceMetadata{Name: f.tmplAtespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return "ensure template atespace: " + err.Error()
	}
	ref := &atepb.ObjectRef{Atespace: f.tmplAtespace, Name: want.GetMetadata().GetName()}
	got, err := ctl.GetActorTemplate(ctx, &atepb.GetActorTemplateRequest{ActorTemplate: ref})
	switch {
	case status.Code(err) == codes.NotFound:
		if _, err := ctl.CreateActorTemplate(ctx, &atepb.CreateActorTemplateRequest{ActorTemplate: want}); err != nil {
			return "create template " + ref.GetName() + ": " + err.Error()
		}
		t.Logf("created ActorTemplate %s/%s (image %s)", ref.GetAtespace(), ref.GetName(), want.GetContainers()[0].GetImage())
	case err != nil:
		return "get template " + ref.GetName() + ": " + err.Error()
	case got.GetContainers()[0].GetImage() != want.GetContainers()[0].GetImage():
		// Templates are immutable. Running against a stale image would test the wrong harness.
		return "ActorTemplate " + ref.GetAtespace() + "/" + ref.GetName() + " exists with image " +
			got.GetContainers()[0].GetImage() + ", not this run's " + want.GetContainers()[0].GetImage() +
			"; delete it (or use a fresh ACTORTEMPLATE_ATESPACE) and rerun"
	}
	for {
		got, err := ctl.GetActorTemplate(ctx, &atepb.GetActorTemplateRequest{ActorTemplate: ref})
		if err != nil {
			return "poll template " + ref.GetName() + ": " + err.Error()
		}
		golden := got.GetStatus().GetGoldenSnapshotStatus()
		if msg := golden.GetErrorMessage(); msg != "" {
			return "template " + ref.GetName() + " golden snapshot failed: " + msg
		}
		if golden.GetGoldenTag().GetName() != "" {
			t.Logf("ActorTemplate %s/%s golden snapshot ready (tag %s/%s)", ref.GetAtespace(), ref.GetName(),
				golden.GetGoldenTag().GetAtespace(), golden.GetGoldenTag().GetName())
			return ""
		}
		select {
		case <-ctx.Done():
			return "timed out waiting for template " + ref.GetName() + " golden snapshot"
		case <-time.After(5 * time.Second):
		}
	}
}
