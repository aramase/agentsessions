package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrCheckpointing is returned when an operation cannot proceed because its session is being
// checkpointed (Suspend, or the parent of a stateful Fork until every child is cloned): a turn that
// tries to start, a Suspend or Fork that tries to begin, or a turn whose harness connection the
// checkpoint ended. It is retryable once the checkpoint ends; session.Service maps it to
// codes.Aborted.
var ErrCheckpointing = errors.New("placement: session is being checkpointed")

// ErrTurnStarting is returned when a Suspend or a stateful Fork cannot begin because a turn on the
// session is still placing its compute (Runtime.Create or Runtime.Restore). It is retryable once the
// turn is placed; session.Service maps it to codes.Aborted.
var ErrTurnStarting = errors.New("placement: a turn on the session is still placing its compute")

// errSuperseded is the cancellation cause of a turn whose harness connection a checkpoint ended. The
// checkpoint mints its fence before it ends the connection, so the turn has been superseded exactly
// as a fenced writer is, and the error says both.
var errSuperseded = fmt.Errorf("%w: in-flight turn superseded and its harness connection closed: %w", ErrCheckpointing, eventlog.ErrFenced)

// turnError reports why a turn failed. When a checkpoint ended the turn's harness connection, the
// error the turn itself surfaced is incidental (a cancelled model call, a closed stream), so the
// checkpoint is reported as the cause and the incidental error is kept for diagnosis.
func turnError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, errSuperseded) && !errors.Is(err, errSuperseded) {
		return fmt.Errorf("%w (turn ended with: %w)", cause, err)
	}
	return err
}

// sessionSet holds the per-session state machine that serializes turns (Exec, Resume) against
// checkpoints (Suspend, stateful Fork). It is keyed by session UID alone, so Placers that share one
// (every Placer of a Registry) serialize against each other.
//
// A session is in one of five states, derived from its turns and its checkpoint mark:
//
//   - idle: no turn and no checkpoint.
//   - starting: a turn is placing compute (Runtime.Create or Restore is in flight).
//   - running: turns are placed and dial, mint their fence, or stream; none is starting.
//   - checkpointing: a Suspend, or the source checkpoint of a stateful Fork, owns the session.
//   - forking: a stateful Fork is cloning children from the checkpoint it just recorded.
//
// Every entry point checks the state and takes its transition in one critical section under mu, so
// no check can go stale before the operation it admits is visible to the others:
//
//   - admit (Exec, Resume): idle, starting or running -> starting. Refused with ErrCheckpointing
//     while checkpointing or forking. Concurrent turns are allowed; the log's fence and CAS order
//     them, as before.
//   - placed (the turn's Create or Restore returned): starting -> running once no other turn is
//     starting.
//   - beginCheckpoint (Suspend, stateful Fork): idle or running -> checkpointing. Running turns are
//     superseded: marked here, then ended and waited for by endTurns. Refused with ErrTurnStarting
//     while a turn is starting and with ErrCheckpointing while checkpointing or forking.
//   - markForking (stateful Fork, after the SUSPEND record): checkpointing -> forking.
//   - endCheckpoint: checkpointing or forking -> running if a superseded turn has not returned yet,
//     else idle.
//   - release (a turn returns): drops the turn.
//
// A checkpoint refuses a starting turn instead of superseding it because placement cannot be safely
// interrupted: a Create or Restore whose context is cancelled can return while the runtime still
// completes it, waking compute the checkpoint has just captured. Refusing keeps the rule simple: no
// runtime call that wakes the session's compute overlaps a checkpoint of it.
type sessionSet struct {
	mu       sync.Mutex
	sessions map[string]*sessionState
	// shared is set when a Registry owns the set, so a Placer cannot join two registries' sets.
	shared bool
}

// checkpointPhase is the checkpoint half of a session's state.
type checkpointPhase int

const (
	noCheckpoint checkpointPhase = iota
	checkpointing
	forking
)

func (c checkpointPhase) String() string {
	switch c {
	case checkpointing:
		return "held by a Suspend or a fork's checkpoint"
	case forking:
		return "held by a fork cloning its children"
	default:
		return "idle"
	}
}

// sessionState is one session's entry in the set. It exists only while the session is not idle.
type sessionState struct {
	turns      map[*turn]struct{} // admitted turns that have not returned
	starting   int                // turns among them that are still placing compute
	checkpoint checkpointPhase
}

// busy reports whether any session is not idle. Entries are deleted when a session goes idle, so an
// idle set is empty.
func (s *sessionSet) busy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions) > 0
}

// state returns the session's entry, creating it. The caller holds mu.
func (s *sessionSet) state(sessionUID string) *sessionState {
	if s.sessions == nil {
		s.sessions = map[string]*sessionState{}
	}
	st := s.sessions[sessionUID]
	if st == nil {
		st = &sessionState{turns: map[*turn]struct{}{}}
		s.sessions[sessionUID] = st
	}
	return st
}

// dropIfIdle deletes the session's entry once it is idle. The caller holds mu.
func (s *sessionSet) dropIfIdle(sessionUID string) {
	if st := s.sessions[sessionUID]; st != nil && len(st.turns) == 0 && st.checkpoint == noCheckpoint {
		delete(s.sessions, sessionUID)
	}
}

// admit registers a new turn on the session, in the starting state, unless a checkpoint owns it.
// The returned context is the turn's: a checkpoint that supersedes the turn cancels it.
func (s *sessionSet) admit(ctx context.Context, sessionUID string) (context.Context, *turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.sessions[sessionUID]; st != nil && st.checkpoint != noCheckpoint {
		return ctx, nil, fmt.Errorf("%w: session %q is %s", ErrCheckpointing, sessionUID, st.checkpoint)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	t := &turn{cancel: cancel, starting: true, minted: make(chan struct{})}
	st := s.state(sessionUID)
	st.turns[t] = struct{}{}
	st.starting++
	return ctx, t, nil
}

// placed records that the turn's Create or Restore returned, so a checkpoint may now supersede it.
func (s *sessionSet) placed(sessionUID string, t *turn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopStarting(sessionUID, t)
}

// stopStarting moves the turn out of the starting state. The caller holds mu.
func (s *sessionSet) stopStarting(sessionUID string, t *turn) {
	if t.starting {
		t.starting = false
		s.sessions[sessionUID].starting--
	}
}

// release drops a returning turn from the session.
func (s *sessionSet) release(sessionUID string, t *turn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopStarting(sessionUID, t)
	delete(s.sessions[sessionUID].turns, t)
	s.dropIfIdle(sessionUID)
}

// beginCheckpoint moves the session to checkpointing and returns its running turns, each marked
// superseded. Taking the mark and collecting the turns under one lock is what leaves no window for a
// turn to start in between. The caller mints its fence after this returns, so a turn that mints a
// newer fence still sees the superseded mark (checkSuperseded) and writes nothing. A turn stays
// marked even if the checkpoint aborts. Every successful call must be paired with endCheckpoint.
func (s *sessionSet) beginCheckpoint(sessionUID string) ([]*turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.sessions[sessionUID]; st != nil {
		if st.checkpoint != noCheckpoint {
			return nil, fmt.Errorf("%w: session %q is %s", ErrCheckpointing, sessionUID, st.checkpoint)
		}
		if st.starting > 0 {
			return nil, fmt.Errorf("%w: session %q", ErrTurnStarting, sessionUID)
		}
	}
	st := s.state(sessionUID)
	st.checkpoint = checkpointing
	open := make([]*turn, 0, len(st.turns))
	for t := range st.turns {
		t.superseded.Store(true)
		open = append(open, t)
	}
	return open, nil
}

// markForking moves a session its caller is checkpointing to forking.
func (s *sessionSet) markForking(sessionUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionUID].checkpoint = forking
}

// endCheckpoint lifts the checkpoint beginCheckpoint took.
func (s *sessionSet) endCheckpoint(sessionUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionUID].checkpoint = noCheckpoint
	s.dropIfIdle(sessionUID)
}

// turn is one admitted Exec or Resume: its place in the session's state and, once dialed, its
// harness connection and the means to end it from outside the turn.
type turn struct {
	cancel context.CancelCauseFunc
	// starting is guarded by sessionSet.mu.
	starting bool
	// superseded is set, under sessionSet.mu, by the checkpoint that collects this turn, BEFORE that
	// checkpoint mints its fence. A turn whose fence is newer than the checkpoint's is not fenced by
	// it, but is guaranteed to observe this flag, so it checks the flag after minting.
	superseded atomic.Bool
	// minted is closed once the turn can no longer mint a fence: right after its one NewFence call
	// returns, or when the turn returns without having made it. A checkpoint waits on it so that no
	// turn's fence can land after the fence the checkpoint records under.
	minted     chan struct{}
	mintedOnce sync.Once

	mu      sync.Mutex
	harness api.Harness
	close   func() error
	ended   bool
	err     error
}

// fenceMinted records that the turn has made its one NewFence call, successful or not.
func (t *turn) fenceMinted() {
	t.mintedOnce.Do(func() { close(t.minted) })
}

// attach records the turn's harness connection, so a checkpoint can end it. A turn a checkpoint has
// already claimed never opens a stream: the connection is closed and errSuperseded returned.
func (t *turn) attach(har api.Harness, closeHarness func() error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended || t.superseded.Load() {
		// Nothing was sent on the connection, so the close error carries no information.
		_ = closeHarness()
		return errSuperseded
	}
	t.harness, t.close = har, closeHarness
	return nil
}

// attached reports whether the turn holds a harness connection.
func (t *turn) attached() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.close != nil
}

// checkSuperseded fails with errSuperseded if a checkpoint has claimed this turn or already ended
// it. The turn calls it right after minting its fence and before it writes anything.
func (t *turn) checkSuperseded(ctx context.Context) error {
	if t.superseded.Load() {
		return errSuperseded
	}
	if ctx.Err() != nil {
		return turnError(ctx, ctx.Err())
	}
	return nil
}

// end cancels the turn's context, which ends its Connect stream, and closes the connection if one is
// attached. It is idempotent, so the owning turn and a checkpoint can both call it.
func (t *turn) end(cause error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.ended {
		t.ended = true
		t.cancel(cause)
		if t.close != nil {
			t.err = t.close()
		}
	}
	return t.err
}
