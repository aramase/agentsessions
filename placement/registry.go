package placement

import (
	"errors"
	"fmt"
	"sort"
)

// ErrUnknownHarness is returned for a harness name the host does not serve. session.Service
// surfaces it as InvalidArgument: naming a harness that does not exist is a caller mistake, not a
// host fault, and failing is the point. Silently substituting a different harness would run the
// session on something other than what was asked for, and the log would record that as if it had
// been intended.
var ErrUnknownHarness = errors.New("placement: unknown harness")

// Registry resolves a harness name to the Placer that runs it.
//
// A Placer owns exactly one backend and one model, so a harness and the compute it runs on are
// chosen together rather than separately. That is why the registry sits above the Placer instead
// of inside it: which harness to run is a routing decision, while placing one harness on one
// runtime is what a Placer already does.
type Registry struct {
	byName         map[string]*Placer
	defaultHarness string
}

// NewRegistry builds a registry over the named placers. defaultHarness names the entry used when a
// caller does not specify one, and must itself be registered: a default that resolves to nothing
// would turn every unqualified request into an error at call time rather than at startup.
func NewRegistry(defaultHarness string, placers map[string]*Placer) (*Registry, error) {
	if len(placers) == 0 {
		return nil, errors.New("placement: registry needs at least one harness")
	}
	for name, p := range placers {
		if name == "" {
			return nil, errors.New("placement: harness name cannot be empty")
		}
		if p == nil {
			return nil, fmt.Errorf("placement: harness %q has no placer", name)
		}
	}
	if defaultHarness == "" {
		return nil, errors.New("placement: registry needs a default harness")
	}
	if _, ok := placers[defaultHarness]; !ok {
		return nil, fmt.Errorf("placement: default harness %q is not registered", defaultHarness)
	}
	byName := make(map[string]*Placer, len(placers))
	for name, p := range placers {
		byName[name] = p
	}
	return &Registry{byName: byName, defaultHarness: defaultHarness}, nil
}

// For returns the Placer for a harness name. An empty name selects the default, which is how a
// caller that does not care about harness selection keeps working unchanged.
func (r *Registry) For(harness string) (*Placer, error) {
	if harness == "" {
		harness = r.defaultHarness
	}
	p, ok := r.byName[harness]
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownHarness, harness, r.Names())
	}
	return p, nil
}

// Default is the harness used when a caller names none. session.Service records it on a session
// created without one, so a stored harness always refers to something the host can actually run.
func (r *Registry) Default() string { return r.defaultHarness }

// Names lists the registered harnesses in sorted order, for startup logs and error messages.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.byName))
	for name := range r.byName {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
