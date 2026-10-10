package placement

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrUnknownHarness is returned for a name unknown to this host. session.Service surfaces it as
// InvalidArgument: naming a harness that does not exist is a caller mistake, not a
// host fault, and failing is the point. Silently substituting a different harness would run the
// session on something other than what was asked for, and the log would record that as if it had
// been intended.
var ErrUnknownHarness = errors.New("placement: unknown harness")

// ErrHarnessUnservable means a registered name is known to this host but cannot run here.
// Unlike ErrHarnessUnavailable (a transport/backend outage on a usable Placer), this is a
// registration-load failure: the name remains reserved and visible to operators.
var ErrHarnessUnservable = errors.New("placement: registered harness is unservable on this host")

// ErrRecordedHarnessNotServed refuses recovery when the pending invocation names a registry entry
// this host no longer serves. Unlike a caller's unknown name, it is a recovery precondition.
var ErrRecordedHarnessNotServed = errors.New("placement: recorded harness is not served")

// ErrHarnessExists is returned by Add for a name that already resolves.
var ErrHarnessExists = errors.New("placement: harness name already in use")

// Registry resolves a harness name to the Placer that runs it.
//
// A Placer owns exactly one backend and one model, so a harness and the compute it runs on are
// chosen together rather than separately. That is why the registry sits above the Placer instead
// of inside it: which harness to run is a routing decision, while placing one harness on one
// runtime is what a Placer already does.
//
// The entries given to NewRegistry are static: built into the host or configured when it starts.
// Add puts a registered harness in later, on a running host; MarkUnavailable reserves one that
// cannot be loaded. An entry is never replaced or removed once added, because a session's turns,
// streams and fences are coordinated through the Placer it resolves to, and a second Placer for the
// same name would split that coordination.
type Registry struct {
	mu             sync.RWMutex
	byName         map[string]*Placer
	unavailable    map[string]error // registered names that cannot run on this host
	static         map[string]bool  // immutable after NewRegistry
	reserved       map[string]bool  // immutable after NewRegistry
	defaultHarness string

	// guard is shared by every Placer in byName, static or added, because session.Service can
	// route one session through different Placers (ExecRequest.harness vs the recorded harness).
	guard *sessionGuard
}

// RegistryOption configures a Registry.
type RegistryOption func(*Registry)

// WithReservedNames reserves harness names for static harnesses that this host can be configured
// with but is not serving now, such as a built-in that needs a flag. Reserving them keeps a
// registration from taking a name that would collide the next time the host starts with that
// configuration.
func WithReservedNames(names ...string) RegistryOption {
	return func(r *Registry) {
		for _, n := range names {
			r.reserved[n] = true
		}
	}
}

// NewRegistry builds a registry over the named placers. defaultHarness names the entry used when a
// caller does not specify one, and must itself be registered: a default that resolves to nothing
// would turn every unqualified request into an error at call time rather than at startup.
//
// It wires the Registry's shared session guard into the supplied Placer pointers; Add does the same
// for a Placer added later. Construct the Registry before using any of those Placers, and do not
// register them in another Registry: rewiring running or already-registered Placers is not
// supported. Separate Registries do not fence each other.
func NewRegistry(defaultHarness string, placers map[string]*Placer, opts ...RegistryOption) (*Registry, error) {
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
	r := &Registry{
		byName:         make(map[string]*Placer, len(placers)),
		unavailable:    make(map[string]error),
		static:         make(map[string]bool, len(placers)),
		reserved:       map[string]bool{},
		defaultHarness: defaultHarness,
		guard:          new(sessionGuard),
	}
	for name, p := range placers {
		p.guard = r.guard
		r.byName[name] = p
		r.static[name] = true
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Add makes a registered harness resolvable. It refuses a reserved name and a name already in use,
// static or not: the caller decides whether a repeat registration is the same harness before it
// builds a Placer, and an existing entry is never replaced.
//
// On success p shares the Registry's session guard, so an operation on a session through p and one
// through any other Placer in this Registry refuse to overlap with ErrSessionBusy, exactly as for
// the Placers given to NewRegistry. p must not have been used or registered elsewhere; a refused
// p is left untouched.
func (r *Registry) Add(name string, p *Placer) error {
	if name == "" || p == nil {
		return errors.New("placement: Add needs a name and a placer")
	}
	if r.Reserved(name) {
		return fmt.Errorf("%w: %q is reserved for a static harness", ErrHarnessExists, name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; ok {
		return fmt.Errorf("%w: %q", ErrHarnessExists, name)
	}
	if _, ok := r.unavailable[name]; ok {
		return fmt.Errorf("%w: %q", ErrHarnessExists, name)
	}
	p.guard = r.guard
	r.byName[name] = p
	return nil
}

// MarkUnavailable reserves a registered name whose stored spec or local placement cannot be
// loaded. This records a permanent host-local registration failure, not a transient backend
// outage (ErrHarnessUnavailable). It never replaces a live Placer or a previously recorded failure.
func (r *Registry) MarkUnavailable(name string, cause error) error {
	if name == "" || cause == nil {
		return errors.New("placement: MarkUnavailable needs a name and a cause")
	}
	if r.Reserved(name) {
		return fmt.Errorf("%w: %q is reserved for a static harness", ErrHarnessExists, name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; ok {
		return fmt.Errorf("%w: %q", ErrHarnessExists, name)
	}
	if _, ok := r.unavailable[name]; ok {
		return fmt.Errorf("%w: %q", ErrHarnessExists, name)
	}
	r.unavailable[name] = cause
	return nil
}

// Static reports whether name is a harness given to NewRegistry rather than one added later.
func (r *Registry) Static(name string) bool { return r.static[name] }

// Reserved reports whether name belongs to the host: a static harness, or a name reserved with
// WithReservedNames. A reserved name can never be added.
func (r *Registry) Reserved(name string) bool { return r.static[name] || r.reserved[name] }

// StaticNames lists the static harnesses in sorted order.
func (r *Registry) StaticNames() []string {
	out := make([]string, 0, len(r.static))
	for name := range r.static {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ReservedNames lists every reserved name, static harnesses included, in sorted order.
func (r *Registry) ReservedNames() []string {
	out := r.StaticNames()
	for name := range r.reserved {
		if !r.static[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Has reports whether this host knows the name, including registrations it cannot serve.
// For, not Has, determines whether a usable Placer exists.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, served := r.byName[name]
	_, unavailable := r.unavailable[name]
	return served || unavailable
}

// For returns the Placer for a harness name. An empty name selects the default, which is how a
// caller that does not care about harness selection keeps working unchanged.
func (r *Registry) For(harness string) (*Placer, error) {
	if harness == "" {
		harness = r.defaultHarness
	}
	r.mu.RLock()
	p, ok := r.byName[harness]
	cause, unavailable := r.unavailable[harness]
	r.mu.RUnlock()
	if unavailable {
		return nil, fmt.Errorf("%w %q: %w", ErrHarnessUnservable, harness, cause)
	}
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownHarness, harness, r.Names())
	}
	return p, nil
}

// RegistryResumeOption configures one Registry.Resume call.
type RegistryResumeOption func(*registryResumeConfig)

type registryResumeConfig struct {
	check func(name string) error
}

// WithResolvedHarnessCheck runs check on the harness name Registry.Resume resolved, under the
// session guard and before anything is restored. A non-nil error is returned unchanged and
// nothing is resumed. The host uses it to apply routing rules that live above the Registry.
func WithResolvedHarnessCheck(check func(name string) error) RegistryResumeOption {
	return func(c *registryResumeConfig) { c.check = check }
}

// Resume resolves and recovers a pending invocation under the Registry's shared session guard.
// Selection cannot race with Exec or Suspend through this Registry: contention wins over stale
// invocation errors, and routing plus recovery share one local critical section.
// Legacy markerless and completed journals retain sessionDefaultName (empty = registry default).
func (r *Registry) Resume(ctx context.Context, log eventlog.Store, sessionUID, sessionDefaultName string, opts ...RegistryResumeOption) error {
	var cfg registryResumeConfig
	for _, o := range opts {
		o(&cfg)
	}
	release, err := r.guard.tryLock(sessionUID)
	if err != nil {
		return err
	}
	defer release()

	invocation, err := controller.ResumeInvocation(log)
	if err != nil {
		return err
	}
	name := sessionDefaultName
	recordedName := invocation != nil && invocation.Harness != ""
	if recordedName {
		name = invocation.Harness
	}
	if name == "" {
		name = r.defaultHarness
	}
	p, err := r.For(name)
	if err != nil {
		if recordedName && errors.Is(err, ErrUnknownHarness) {
			return fmt.Errorf("%w: %w", ErrRecordedHarnessNotServed, err)
		}
		return err
	}
	if cfg.check != nil {
		if err := cfg.check(name); err != nil {
			return err
		}
	}
	return p.resume(ctx, log, sessionUID, resumeConfig{harness: name})
}

// Default is the harness used when a caller names none. session.Service records it on a session
// created without one, so a stored harness always refers to something the host can actually run.
func (r *Registry) Default() string { return r.defaultHarness }

// Names lists the served and unservable known harnesses in sorted order, for startup logs and
// error messages. Has/Names include unavailable names, while For alone resolves a usable Placer.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byName)+len(r.unavailable))
	for name := range r.byName {
		out = append(out, name)
	}
	for name := range r.unavailable {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
