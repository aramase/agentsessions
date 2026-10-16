package placement

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/eventlog"
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

// ErrHarnessExists is returned by Add and MarkUnavailable for a name already in use or reserved
// for a static harness. This includes unservable names, which remain in use but do not resolve.
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

	// guard and sessions are shared by every Placer in byName, static or added, because
	// session.Service can route one session through different Placers (ExecRequest.harness vs the
	// recorded harness).
	guard    *sessionGuard
	sessions *sessionSet
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
// The placers are switched to the Registry's shared session guard and its shared set of
// per-session connections and checkpoint marks; Add does the same for a Placer added later.
// session.Service routes a turn by ExecRequest.harness but a Suspend or Fork by the session's
// recorded harness, so the two can land on different Placers; sharing them is what lets Suspend
// refuse a turn (ErrSessionBusy), and a stateful fork end and refuse one, whichever Placer runs it.
// Separate Registries do not fence each other.
//
// NewRegistry rejects a Placer that already belongs to a registry, and one with an operation in
// progress: an Exec, Suspend or Resume holding the session guard, an open harness connection, or a
// checkpoint mark. Replacing its guard and set would orphan them, so a later Suspend would not be
// refused by that operation and a later checkpoint would neither end nor refuse its turn. A Placer
// that has run operations and is now idle holds nothing and is accepted. The check cannot see an
// operation that starts while NewRegistry runs, and the guard and set are replaced without
// synchronization, so NewRegistry must not run concurrently with any method of the placers it is
// given: build the registry before serving.
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
		sessions:       &sessionSet{shared: true},
	}
	for name, p := range placers {
		if p.sessions.shared {
			return nil, fmt.Errorf("placement: harness %q: placer already belongs to another registry", name)
		}
		if p.sessions.busy() || p.guard.busy() {
			return nil, fmt.Errorf("placement: harness %q: placer has an operation in progress", name)
		}
	}
	for name, p := range placers {
		p.guard = r.guard
		p.sessions = r.sessions
		p.registry = r
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
// On success p shares the Registry's session guard and its per-session connections and checkpoint
// marks, exactly as the Placers given to NewRegistry do: an operation on a session through p and
// one through any other Placer in this Registry refuse to overlap with ErrSessionBusy, and a
// stateful fork through either ends and refuses a turn through the other. Add refuses a p that
// belongs to another Registry or has an operation in progress, as NewRegistry does; the check
// cannot see an operation that starts while Add runs, so p must not be in use until Add returns.
// Adding a Placer already in this Registry under another name shares nothing new. A refused p is
// left untouched.
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
	if p.sessions != r.sessions {
		if p.sessions.shared {
			return fmt.Errorf("placement: harness %q: placer already belongs to another registry", name)
		}
		if p.sessions.busy() || p.guard.busy() {
			return fmt.Errorf("placement: harness %q: placer has an operation in progress", name)
		}
		p.guard = r.guard
		p.sessions = r.sessions
		p.registry = r
	}
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

// lookup returns the Placer registered under name, without For's default for an empty name.
func (r *Registry) lookup(name string) (*Placer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byName[name]
	return p, ok
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
		return nil, fmt.Errorf("%w: harness %q: %w", ErrHarnessUnservable, harness, cause)
	}
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownHarness, harness, r.Names())
	}
	return p, nil
}

// WithSessionGuard runs operation synchronously under the same reclaimable guard as every
// Placer in this Registry. Contention returns ErrSessionBusy without calling operation.
// The callback must not call guarded Registry or Placer methods: the guard is not reentrant.
func (r *Registry) WithSessionGuard(sessionUID string, operation func() error) error {
	release, err := r.guard.tryDeleteLock(sessionUID)
	if err != nil {
		return err
	}
	defer release()
	return operation()
}

// WithSessionRead protects a routing read phase from deletion without serializing it with
// running turns or Fork rollback. Contention refuses the callback before it can wait for SQL.
// The callback must not call WithSessionGuard: shared admission cannot upgrade to exclusive.
func (r *Registry) WithSessionRead(sessionUID string, operation func() error) error {
	release, err := r.guard.tryReadLock(sessionUID)
	if err != nil {
		return err
	}
	defer release()
	return operation()
}

// RegistryResumeOption configures one Registry.Resume call.
type RegistryResumeOption func(*registryResumeConfig)

type registryResumeConfig struct {
	check           func(name string) error
	observer        controller.Observer
	expectedLastSeq *int64
}

// WithResumeExpectedLastSeq makes Registry.Resume refuse a different journal head with
// eventlog.ErrConflict, including both values. Checks run under the shared session guard before
// inspection, after an early fence before Restore/repair, and after a restored turn's regular
// writer fence before Run. A late refusal may already have restored compute. Either fence can
// supersede another writer even on refusal; pending approval queries need no fence. Without this
// option behavior is unchanged.
func WithResumeExpectedLastSeq(seq int64) RegistryResumeOption {
	return func(c *registryResumeConfig) { c.expectedLastSeq = &seq }
}

// WithRegistryResumeObserver reports newly committed recovery records and streaming chunks.
// This is a RegistryResumeOption, distinct from the Placer's WithResumeObserver option.
func WithRegistryResumeObserver(o controller.Observer) RegistryResumeOption {
	return func(c *registryResumeConfig) { c.observer = o }
}

// WithResolvedHarnessCheck runs check on the harness name Registry.Resume or Registry.Suspend
// resolved, under the session guard and before runtime IO. A non-nil error is returned unchanged.
// The host uses it to apply routing rules that live above the Registry.
func WithResolvedHarnessCheck(check func(name string) error) RegistryResumeOption {
	return func(c *registryResumeConfig) { c.check = check }
}

// Resume resolves and recovers an invocation under the Registry's shared session guard, running
// the resolved-harness check before runtime IO or call-only repair. Pending/repaired requests
// return *api.ApprovalParkedError without runtime IO; that alone is not cold-compute proof. The
// one exception is the repair of an override turn on a runtime with memory snapshots: its harness
// is described first, and a REQUIRES_MEMORY_SNAPSHOT one is refused (ErrStatefulOverride).
// Decided recovery delegates to the Placer: a new gate commits SUSPEND and returns park, while
// ordinary success records RESUME. Repair/handoff failures retain their actual cause.
// Contention through this Registry wins over stale invocation errors. Legacy markerless and
// completed journals retain sessionDefaultName (empty = registry default).
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

	if err := eventlog.CheckWritable(log); err != nil {
		return err
	}
	if err := checkResumeHead(log, cfg.expectedLastSeq); err != nil {
		return err
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil {
		return err
	}
	if state != nil && state.Inherited && state.Receipt == nil {
		return controller.ErrInheritedToolIntent
	}
	invocation, err := controller.ResumeInvocation(log)
	if err != nil {
		return err
	}
	own := sessionDefaultName
	if own == "" {
		own = r.defaultHarness
	}
	name := own
	recordedName := invocation != nil && invocation.Harness != ""
	if recordedName {
		name = invocation.Harness
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
	// A pending turn recorded on a Placer other than the session's own ran as an override. The
	// Placer refuses a memory-tier harness there (ErrStatefulOverride), before it repairs a missing
	// approval request too. An alias of the session's harness shares its Placer and is not an
	// override; a session harness this host cannot resolve is treated as another one.
	resume := resumeConfig{harness: name, observer: cfg.observer, expectedLastSeq: cfg.expectedLastSeq}
	if name != own {
		ownPlacer, err := r.For(own)
		resume.override = err != nil || ownPlacer != p
	}
	if handled, err := p.resumeApproval(log, sessionUID, state, resume, func() error {
		return p.refuseOverrideRepair(ctx, sessionUID, resume.override)
	}); handled {
		return err
	}
	return p.resume(ctx, log, sessionUID, resume)
}

func checkResumeHead(log eventlog.Store, expected *int64) error {
	if expected == nil {
		return nil
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	if head != *expected {
		return fmt.Errorf("%w: expected_last_seq %d, head %d", eventlog.ErrConflict, *expected, head)
	}
	return nil
}

// Approve journals only a decision under the shared session guard. The decision does not need
// a served/available harness or any compute, so routing and host recovery checks do not apply.
// Approve returns ErrCheckpointing while a stateful Fork holds the session's checkpoint mark.
func (r *Registry) Approve(ctx context.Context, log eventlog.Store, sessionUID string, decision api.ApprovalDecision) (eventlog.Record, error) {
	release, err := r.guard.tryLock(sessionUID)
	if err != nil {
		return eventlog.Record{}, err
	}
	defer release()
	if err := r.sessions.markCheckpoint(sessionUID); err != nil {
		return eventlog.Record{}, err
	}
	defer r.sessions.endCheckpoint(sessionUID)
	return controller.Approve(log, decision)
}

// Suspend routes an owned unreceipted approval to its recorded harness, including retries after an
// automatic snapshot/append failure. Inherited approvals and ordinary sessions retain their
// stored-default route.
func (r *Registry) Suspend(ctx context.Context, log eventlog.Store, sessionUID, sessionDefaultName string, opts ...RegistryResumeOption) (api.SnapshotRef, error) {
	var cfg registryResumeConfig
	for _, o := range opts {
		o(&cfg)
	}
	release, err := r.guard.tryLock(sessionUID)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	defer release()
	if err := eventlog.CheckWritable(log); err != nil {
		return api.SnapshotRef{}, err
	}
	state, err := controller.InspectApproval(log, 0)
	if err != nil {
		return api.SnapshotRef{}, err
	}
	name := sessionDefaultName
	recordedName := state != nil && !state.Inherited && state.Receipt == nil && state.Invocation != nil && state.Invocation.Harness != ""
	if recordedName {
		name = state.Invocation.Harness
	}
	if name == "" {
		name = r.defaultHarness
	}
	p, err := r.For(name)
	if err != nil {
		if recordedName && errors.Is(err, ErrUnknownHarness) {
			return api.SnapshotRef{}, fmt.Errorf("%w: %w", ErrRecordedHarnessNotServed, err)
		}
		return api.SnapshotRef{}, err
	}
	if cfg.check != nil {
		if err := cfg.check(name); err != nil {
			return api.SnapshotRef{}, err
		}
	}
	if err := r.sessions.markCheckpoint(sessionUID); err != nil {
		return api.SnapshotRef{}, err
	}
	defer r.sessions.endCheckpoint(sessionUID)
	return p.suspend(ctx, log, sessionUID, nil)
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
