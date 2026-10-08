package placement_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

// actorBackend models one session's actor the way substrate's does for these races: Create and
// Restore wake it, Snapshot suspends it under a new snapshot, Stop deletes it, and a clone (Fork)
// succeeds only while the actor is still suspended holding the snapshot the fork took. Every call
// runs its hook before it takes effect, so a test can hold the call at a gate and order another
// entry point against it.
type actorBackend struct {
	address string
	actor   string

	onCreate, onRestore, onSnapshot, onFork func()

	mu        sync.Mutex
	running   bool
	snapshot  string // the snapshot the actor holds; empty once deleted
	creates   int
	restores  int
	snapshots int
	clones    int
}

func (b *actorBackend) Describe(context.Context) (api.Descriptor, error) { return memDescriptor, nil }

func (b *actorBackend) incarnation(uid string) api.Incarnation {
	return api.Incarnation{
		ID:           uid,
		Address:      b.address,
		CallMetadata: map[string]string{"ate-target-actor": "space/" + uid},
		Runtime:      "actor",
	}
}

func (b *actorBackend) Create(_ context.Context, s *api.SessionSpec) (api.Incarnation, error) {
	if b.onCreate != nil {
		b.onCreate()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.creates++
	b.running = true
	return b.incarnation(s.SessionUID), nil
}

func (b *actorBackend) Restore(_ context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	if b.onRestore != nil {
		b.onRestore()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.restores++
	b.running = true
	return b.incarnation(ref.Local), nil
}

func (b *actorBackend) Snapshot(_ context.Context, in api.Incarnation, _ api.SnapshotKind) (api.SnapshotRef, error) {
	if b.onSnapshot != nil {
		b.onSnapshot()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.snapshots++
	b.running = false
	b.snapshot = fmt.Sprintf("snap-%d", b.snapshots)
	return api.SnapshotRef{Local: in.ID, ExternalURI: b.snapshot, Memory: true}, nil
}

func (b *actorBackend) Fork(_ context.Context, ref api.SnapshotRef, opts api.ForkOpts) (api.Incarnation, error) {
	if b.onFork != nil {
		b.onFork()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running || ref.ExternalURI != b.snapshot {
		return api.Incarnation{}, fmt.Errorf("actor: clone %q from %q, actor running=%v holding %q: %w",
			opts.ChildSessionUID, ref.ExternalURI, b.running, b.snapshot, api.ErrSnapshotSuperseded)
	}
	b.clones++
	return b.incarnation(opts.ChildSessionUID), nil
}

func (b *actorBackend) Stop(_ context.Context, in api.Incarnation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if in.ID == b.actor {
		b.running, b.snapshot = false, ""
	}
	return nil
}

func (b *actorBackend) Status(context.Context, api.Incarnation) (api.ComputeState, error) {
	return api.ComputeLive, nil
}

func (b *actorBackend) Capabilities() api.RuntimeCapabilities {
	return api.RuntimeCapabilities{MemorySnapshot: true}
}

// counts returns how many times each call took effect, and whether the actor is running.
func (b *actorBackend) counts() (creates, restores, snapshots, clones int, running bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.creates, b.restores, b.snapshots, b.clones, b.running
}

// gate holds the first call that waits on it until letGo, and lets later calls straight through.
type gate struct {
	calls     atomic.Int32
	arrived   chan struct{}
	release   chan struct{}
	letGoOnce sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{arrived: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(g.letGo) // never leave a call parked past the test
	return g
}

func (g *gate) wait() {
	if g.calls.Add(1) == 1 {
		close(g.arrived)
		<-g.release
	}
}

func (g *gate) letGo() { g.letGoOnce.Do(func() { close(g.release) }) }

// once runs fn on the first call only, so a hook that re-enters the backend cannot recurse. Unlike
// sync.Once, a call made while fn is still running returns at once instead of blocking on it.
func once(fn func()) func() {
	var ran atomic.Bool
	return func() {
		if ran.CompareAndSwap(false, true) {
			fn()
		}
	}
}

// serialRig is one session on an actorBackend, driven through a single Placer.
type serialRig struct {
	t       *testing.T
	store   *sqlitelog.Store
	log     *sqlitelog.Log
	backend *actorBackend
	p       *placement.Placer
	// attempts counts Fork calls, so every child has its own UID; forks counts the ones that succeeded.
	attempts, forks int
}

const serialUID = "s1"

func newSerialRig(t *testing.T) *serialRig {
	t.Helper()
	hs := startHarnessServer(t, echoagent.Harness{})
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	backend := &actorBackend{address: hs.addr, actor: serialUID}
	return &serialRig{t: t, store: store, log: store.Session(serialUID), backend: backend, p: placement.New(backend, echoagent.Model)}
}

func (r *serialRig) exec() error {
	head, err := r.log.Head()
	if err != nil {
		return err
	}
	_, err = r.p.Exec(context.Background(), r.log, serialUID, []api.Message{*api.TextMessage("user", "hi")}, head)
	return err
}

func (r *serialRig) resume() error { return r.p.Resume(context.Background(), r.log, serialUID) }

func (r *serialRig) suspend() error {
	_, err := r.p.Suspend(context.Background(), r.log, serialUID)
	return err
}

func (r *serialRig) fork() error {
	head, err := r.log.Head()
	if err != nil {
		return err
	}
	r.attempts++
	uid := fmt.Sprintf("%s-child-%d", serialUID, r.attempts)
	if err := r.p.Fork(context.Background(), r.log, serialUID, []placement.ForkChild{{UID: uid, Log: r.store.Session(uid)}}, head); err != nil {
		return err
	}
	r.forks++
	return nil
}

// suspended runs one turn and suspends the session, so the actor holds a snapshot and both an Exec
// (Create on a suspended actor) and a Resume (Restore) wake it.
func (r *serialRig) suspended() {
	r.t.Helper()
	if err := r.exec(); err != nil {
		r.t.Fatalf("setup exec: %v", err)
	}
	if err := r.suspend(); err != nil {
		r.t.Fatalf("setup suspend: %v", err)
	}
}

// turnKind is an entry point that places compute and runs a turn: Exec through Create, Resume
// through Restore.
type turnKind struct {
	name string
	run  func(*serialRig) error
	gate func(*serialRig, *gate) // installs g on the call that places the turn's compute
}

var (
	execTurn = turnKind{
		name: "Exec",
		run:  (*serialRig).exec,
		gate: func(r *serialRig, g *gate) { r.backend.onCreate = g.wait },
	}
	resumeTurn = turnKind{
		name: "Resume",
		run:  (*serialRig).resume,
		gate: func(r *serialRig, g *gate) { r.backend.onRestore = g.wait },
	}
)

// checkpointPhase is a point at which a checkpoint owns the session.
type checkpointPhase struct {
	name string
	// install installs fn on the backend call the checkpoint makes in this phase.
	install func(*serialRig, func())
}

var (
	// inSnapshot is Suspend's snapshot, or a stateful Fork's source checkpoint before its SUSPEND is
	// recorded.
	inSnapshot = checkpointPhase{name: "checkpointing", install: func(r *serialRig, fn func()) { r.backend.onSnapshot = once(fn) }}
	// inClone is a stateful Fork's clone, after its checkpoint is recorded.
	inClone = checkpointPhase{name: "forking", install: func(r *serialRig, fn func()) { r.backend.onFork = once(fn) }}
)

// checkpointKind is an entry point that checkpoints the session.
type checkpointKind struct {
	name string
	run  func(*serialRig) error
	// phases are the points at which the checkpoint owns the session.
	phases []checkpointPhase
	// done checks the effect of a checkpoint that succeeded.
	done func(*testing.T, *serialRig)
}

var (
	suspendCheckpointKind = checkpointKind{
		name:   "Suspend",
		run:    (*serialRig).suspend,
		phases: []checkpointPhase{inSnapshot},
		done: func(t *testing.T, r *serialRig) {
			if _, _, _, _, running := r.backend.counts(); running {
				t.Fatal("the actor is running after Suspend returned")
			}
		},
	}
	forkCheckpointKind = checkpointKind{
		name:   "Fork",
		run:    (*serialRig).fork,
		phases: []checkpointPhase{inSnapshot, inClone},
		done: func(t *testing.T, r *serialRig) {
			if _, _, _, clones, _ := r.backend.counts(); clones != r.forks {
				t.Fatalf("%d clone(s) for %d fork(s)", clones, r.forks)
			}
		},
	}
)

// Exec vs Suspend.
func TestSerializeExecAndSuspend(t *testing.T) {
	testSerializeTurnAndCheckpoint(t, execTurn, suspendCheckpointKind)
}

// Exec vs Fork: an Exec that passed its checks before a stateful Fork began must not wake the
// parent while the fork checkpoints it or clones from it.
func TestSerializeExecAndFork(t *testing.T) {
	testSerializeTurnAndCheckpoint(t, execTurn, forkCheckpointKind)
}

// Resume vs Fork.
func TestSerializeResumeAndFork(t *testing.T) {
	testSerializeTurnAndCheckpoint(t, resumeTurn, forkCheckpointKind)
}

// Resume vs Suspend.
func TestSerializeResumeAndSuspend(t *testing.T) {
	testSerializeTurnAndCheckpoint(t, resumeTurn, suspendCheckpointKind)
}

// testSerializeTurnAndCheckpoint orders a turn and a checkpoint on one session both ways.
//
// Turn first: the turn is held inside the runtime call that wakes the actor (Create or Restore).
// The checkpoint must be refused with ErrTurnStarting before it captures anything, because that call
// can complete after any snapshot taken now. Once the turn is placed and done, the checkpoint runs.
//
// Checkpoint first: in every phase in which the checkpoint owns the session, the turn must be
// refused with ErrCheckpointing before it reaches the runtime at all.
func testSerializeTurnAndCheckpoint(t *testing.T, turn turnKind, cp checkpointKind) {
	t.Run(turn.name+" placing compute first", func(t *testing.T) {
		r := newSerialRig(t)
		r.suspended()
		_, _, snapshotsBefore, _, _ := r.backend.counts()

		placing := newGate(t)
		turn.gate(r, placing)
		turnErr := make(chan error, 1)
		go func() { turnErr <- turn.run(r) }()
		<-placing.arrived

		if err := cp.run(r); !errors.Is(err, placement.ErrTurnStarting) {
			t.Fatalf("%s while the %s's runtime call was in flight returned %v, want ErrTurnStarting", cp.name, turn.name, err)
		}
		if _, _, snapshots, clones, _ := r.backend.counts(); snapshots != snapshotsBefore || clones != 0 {
			t.Fatalf("the refused %s took %d snapshot(s) and %d clone(s)", cp.name, snapshots-snapshotsBefore, clones)
		}

		placing.letGo()
		if err := <-turnErr; err != nil {
			t.Fatalf("%s after the refused %s returned %v", turn.name, cp.name, err)
		}
		if err := cp.run(r); err != nil {
			t.Fatalf("%s once the %s was done returned %v", cp.name, turn.name, err)
		}
		cp.done(t, r)
	})

	for _, phase := range cp.phases {
		t.Run(cp.name+" first, "+phase.name, func(t *testing.T) {
			r := newSerialRig(t)
			r.suspended()
			creates, restores, _, _, _ := r.backend.counts()

			var turnErr error
			phase.install(r, func() { turnErr = turn.run(r) })
			if err := cp.run(r); err != nil {
				t.Fatalf("%s: %v", cp.name, err)
			}
			if !errors.Is(turnErr, placement.ErrCheckpointing) {
				t.Fatalf("%s while the %s was %s returned %v, want ErrCheckpointing", turn.name, cp.name, phase.name, turnErr)
			}
			if c, rs, _, _, _ := r.backend.counts(); c != creates || rs != restores {
				t.Fatalf("the refused %s reached the runtime: %d Create(s), %d Restore(s)", turn.name, c-creates, rs-restores)
			}
			cp.done(t, r)
		})
	}
}

// Suspend vs Fork: neither may begin while the other owns the session. A Suspend during a fork's
// fan-out would replace (or, with Stop, delete) the checkpoint the remaining children are cloned
// from; a Fork during a Suspend would record a second checkpoint under the first.
func TestSerializeSuspendAndFork(t *testing.T) {
	t.Run("Fork while suspending", func(t *testing.T) {
		r := newSerialRig(t)
		if err := r.exec(); err != nil {
			t.Fatal(err)
		}
		var forkErr error
		r.backend.onSnapshot = once(func() { forkErr = r.fork() })
		if err := r.suspend(); err != nil {
			t.Fatalf("suspend: %v", err)
		}
		if !errors.Is(forkErr, placement.ErrCheckpointing) {
			t.Fatalf("Fork during the Suspend returned %v, want ErrCheckpointing", forkErr)
		}
		if _, _, snapshots, clones, running := r.backend.counts(); snapshots != 1 || clones != 0 || running {
			t.Fatalf("snapshots=%d clones=%d running=%v, want only the Suspend's checkpoint", snapshots, clones, running)
		}
	})

	t.Run("Suspend while the Fork checkpoints", func(t *testing.T) {
		r := newSerialRig(t)
		if err := r.exec(); err != nil {
			t.Fatal(err)
		}
		var suspendErr error
		r.backend.onSnapshot = once(func() { suspendErr = r.suspend() })
		if err := r.fork(); err != nil {
			t.Fatalf("fork: %v", err)
		}
		if !errors.Is(suspendErr, placement.ErrCheckpointing) {
			t.Fatalf("Suspend during the Fork's checkpoint returned %v, want ErrCheckpointing", suspendErr)
		}
		if _, _, snapshots, clones, _ := r.backend.counts(); snapshots != 1 || clones != 1 {
			t.Fatalf("snapshots=%d clones=%d, want the Fork's one checkpoint and one clone", snapshots, clones)
		}
	})

	t.Run("Suspend while forking", func(t *testing.T) {
		r := newSerialRig(t)
		if err := r.exec(); err != nil {
			t.Fatal(err)
		}
		var suspendErr error
		r.backend.onFork = once(func() { suspendErr = r.suspend() })
		if err := r.fork(); err != nil {
			t.Fatalf("fork: %v", err)
		}
		if !errors.Is(suspendErr, placement.ErrCheckpointing) {
			t.Fatalf("Suspend during the Fork's fan-out returned %v, want ErrCheckpointing", suspendErr)
		}
		if _, _, snapshots, clones, _ := r.backend.counts(); snapshots != 1 || clones != 1 {
			t.Fatalf("snapshots=%d clones=%d, want the Fork's one checkpoint and one clone", snapshots, clones)
		}
		if err := r.suspend(); err != nil {
			t.Fatalf("Suspend once the Fork returned: %v", err)
		}
	})
}
