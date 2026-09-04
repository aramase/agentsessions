package placement_test

import (
	"errors"
	"testing"

	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
)

func newPlacer(t *testing.T) *placement.Placer {
	t.Helper()
	b := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = b.Close() })
	return placement.New(b, echoagent.Model)
}

// A registry must resolve each name to its own placer, since that mapping is the whole reason
// Session.harness means anything.
func TestRegistryResolvesByName(t *testing.T) {
	echo, counter := newPlacer(t), newPlacer(t)
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": echo, "counter": counter,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.For("counter")
	if err != nil {
		t.Fatal(err)
	}
	if got != counter {
		t.Fatal("For(counter) returned the wrong placer")
	}
	if got, err := r.For("echo"); err != nil || got != echo {
		t.Fatalf("For(echo) = %v, %v", got, err)
	}
}

// An empty name selects the default, which is what keeps a caller that does not care about
// harness selection working unchanged.
func TestRegistryEmptyNameUsesDefault(t *testing.T) {
	echo := newPlacer(t)
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": echo, "counter": newPlacer(t)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.For("")
	if err != nil {
		t.Fatal(err)
	}
	if got != echo {
		t.Fatal("empty name did not resolve to the default harness")
	}
	if r.Default() != "echo" {
		t.Fatalf("Default() = %q, want echo", r.Default())
	}
}

// An unknown harness must fail rather than fall back. Substituting a different harness would run
// the session on something other than what was asked for, and the log would record it as intended.
func TestRegistryUnknownHarnessIsAnError(t *testing.T) {
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{"echo": newPlacer(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.For("nope"); !errors.Is(err, placement.ErrUnknownHarness) {
		t.Fatalf("For(nope) = %v, want ErrUnknownHarness", err)
	}
}

// A default that is not itself registered must fail at construction. Deferring it to call time
// would turn every unqualified request into a runtime error on a host that started up clean.
func TestNewRegistryRejectsUnregisteredDefault(t *testing.T) {
	_, err := placement.NewRegistry("missing", map[string]*placement.Placer{"echo": newPlacer(t)})
	if err == nil {
		t.Fatal("NewRegistry accepted a default that is not registered")
	}
}

func TestNewRegistryRejectsEmptyRegistry(t *testing.T) {
	if _, err := placement.NewRegistry("echo", nil); err == nil {
		t.Fatal("NewRegistry accepted an empty registry")
	}
}

// Names is what the daemon logs at startup and what an error message lists, so it must be sorted
// rather than map-ordered.
func TestRegistryNamesAreSorted(t *testing.T) {
	r, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": newPlacer(t), "counter": newPlacer(t), "alpha": newPlacer(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := r.Names()
	want := []string{"alpha", "counter", "echo"}
	if len(got) != len(want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names() = %v, want %v", got, want)
		}
	}
}
