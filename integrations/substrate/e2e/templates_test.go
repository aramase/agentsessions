package e2e

import (
	"context"
	"fmt"
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

// tierSandbox is where one tier's template runs: the sandbox class, the SandboxConfig it names, and
// the `workload` label of the WorkerPool that serves it (deploy/substrate/<tier>-<class>-workerpool.yaml).
type tierSandbox struct {
	class  atepb.SandboxClass
	config string
	pool   string
}

// sandboxFor resolves a tier's sandbox from <PREFIX>_SANDBOX_CLASS. Both tiers default to the
// micro-VM class, so the suite exercises the same isolation class end to end. gvisor is an explicit
// opt-in for hosts without KVM; it selects the tier's gVisor pool. <PREFIX>_SANDBOX_CONFIG overrides
// the SandboxConfig name. Any other value is an error rather than a silent fallback, so a typo cannot
// run a tier on a class the caller did not ask for.
func sandboxFor(prefix, tier string) (tierSandbox, error) {
	key := prefix + "_SANDBOX_CLASS"
	switch v := env(key, "microvm"); v {
	case "microvm":
		return tierSandbox{
			class:  atepb.SandboxClass_SANDBOX_CLASS_MICROVM,
			config: env(prefix+"_SANDBOX_CONFIG", "microvm"),
			pool:   "agentsessions-" + tier + "-microvm",
		}, nil
	case "gvisor":
		return tierSandbox{
			class:  atepb.SandboxClass_SANDBOX_CLASS_GVISOR,
			config: env(prefix+"_SANDBOX_CONFIG", "gvisor-default"),
			pool:   "agentsessions-" + tier + "-gvisor",
		}, nil
	default:
		return tierSandbox{}, fmt.Errorf("%s=%q: want microvm or gvisor", key, v)
	}
}

// templateSpec builds a tier's ActorTemplate on the sandbox sandboxFor resolves. Every template
// captures FULL on commit, which is what lets the template build the golden snapshot new actors
// start from.
func templateSpec(t *testing.T, atespace, name, prefix, tier, harnessKind string) *atepb.ActorTemplate {
	t.Helper()
	sb, err := sandboxFor(prefix, tier)
	if err != nil {
		t.Fatal(err)
	}
	return &atepb.ActorTemplate{
		Metadata:       &atepb.ResourceMetadata{Atespace: atespace, Name: name},
		WorkerSelector: &atepb.Selector{MatchLabels: map[string]string{"workload": sb.pool}},
		Containers:     []*atepb.Container{harnessContainer(harnessKind)},
		SnapshotConfig: &atepb.SnapshotConfig{
			OnCommit:        atepb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			StorageLocation: env("SNAPSHOT_LOCATION", "gs://ate-snapshots/agentsessions/") + name + "/",
		},
		SandboxConfig: &atepb.SandboxConfig{SandboxClass: sb.class, ConfigName: sb.config},
	}
}

// echoTemplateSpec is the stateless tier: the echo harness. A STATELESS_REPLAY harness keeps nothing
// in RAM the journal lacks, so the snapshot scope barely matters. It runs on a micro-VM unless
// ECHO_SANDBOX_CLASS=gvisor.
func echoTemplateSpec(t *testing.T, atespace string) *atepb.ActorTemplate {
	return templateSpec(t, atespace, echoTemplate, "ECHO", "echo", "")
}

// counterTemplateSpec is the stateful tier: the in-RAM counter. FULL capture is load-bearing here:
// the count exists only in guest RAM, so only a memory snapshot carries it across suspend and fork.
// It runs on a micro-VM unless COUNTER_SANDBOX_CLASS=gvisor, whose FULL checkpoint also captures
// memory.
func counterTemplateSpec(t *testing.T, atespace string) *atepb.ActorTemplate {
	return templateSpec(t, atespace, counterTemplate, "COUNTER", "counter", "counter")
}

var templateOnce sync.Map // template name -> *sync.Once, so each binary builds a template once

// ensureTemplate makes sure the template exists with this run's harness image and sandbox, that its
// golden snapshot is built, and returns its name. Waiting for the golden matters for more than speed:
// the template controller builds it on a temporary actor that holds one of the pool's workers until
// it is done.
func (f *fixture) ensureTemplate(t *testing.T, spec func(*testing.T, string) *atepb.ActorTemplate) string {
	t.Helper()
	want := spec(t, f.tmplAtespace)
	once, _ := templateOnce.LoadOrStore(want.GetMetadata().GetName(), &sync.Once{})
	var failure string
	once.(*sync.Once).Do(func() { failure = f.createTemplate(t, want) })
	if failure != "" {
		t.Fatal(failure)
	}
	return want.GetMetadata().GetName()
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
	case got.GetSandboxConfig().GetSandboxClass() != want.GetSandboxConfig().GetSandboxClass() ||
		got.GetWorkerSelector().GetMatchLabels()["workload"] != want.GetWorkerSelector().GetMatchLabels()["workload"]:
		// Same reason: a template left by a run on another sandbox class would silently test that class.
		return "ActorTemplate " + ref.GetAtespace() + "/" + ref.GetName() + " exists on sandbox class " +
			got.GetSandboxConfig().GetSandboxClass().String() + " (pool " + got.GetWorkerSelector().GetMatchLabels()["workload"] +
			"), not this run's " + want.GetSandboxConfig().GetSandboxClass().String() + " (pool " +
			want.GetWorkerSelector().GetMatchLabels()["workload"] + "); delete it (or use a fresh ACTORTEMPLATE_ATESPACE) and rerun"
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

// TestSandboxFor pins the tier sandbox selection without a cluster: micro-VM by default, gVisor only
// when asked for, and an unknown class refused rather than mapped to either.
func TestSandboxFor(t *testing.T) {
	for _, tc := range []struct {
		name, class, config string
		want                tierSandbox
		wantErr             bool
	}{
		{name: "default", want: tierSandbox{atepb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm", "agentsessions-echo-microvm"}},
		{name: "microvm", class: "microvm", want: tierSandbox{atepb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm", "agentsessions-echo-microvm"}},
		{name: "gvisor opt-in", class: "gvisor", want: tierSandbox{atepb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor-default", "agentsessions-echo-gvisor"}},
		{name: "config override", class: "gvisor", config: "custom", want: tierSandbox{atepb.SandboxClass_SANDBOX_CLASS_GVISOR, "custom", "agentsessions-echo-gvisor"}},
		{name: "wrong case", class: "gVisor", wantErr: true},
		{name: "unknown", class: "kata", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ECHO_SANDBOX_CLASS", tc.class)
			t.Setenv("ECHO_SANDBOX_CONFIG", tc.config)
			got, err := sandboxFor("ECHO", "echo")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("sandboxFor = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("sandboxFor = %+v, want %+v", got, tc.want)
			}
		})
	}
}
