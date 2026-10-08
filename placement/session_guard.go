package placement

import "sync"

// sessionGuard rejects overlapping operations for one session without retaining idle entries.
// References include acquisition attempts, so releasing a holder cannot replace a lock that a
// contender is about to acquire with a second live lock for the same session.
type sessionGuard struct {
	mu      sync.Mutex
	entries map[string]*sessionGuardEntry
}

type sessionGuardEntry struct {
	mu   sync.Mutex
	refs int
}

func (g *sessionGuard) tryLock(uid string) (func(), error) {
	g.mu.Lock()
	entry := g.entries[uid]
	if entry == nil {
		entry = new(sessionGuardEntry)
		if g.entries == nil {
			g.entries = make(map[string]*sessionGuardEntry)
		}
		g.entries[uid] = entry
	}
	entry.refs++
	g.mu.Unlock()

	if !entry.mu.TryLock() {
		g.unref(uid, entry)
		return nil, ErrSessionBusy
	}
	return func() {
		// Unlock before dropping the reference: no live lock may be removed from the table.
		entry.mu.Unlock()
		g.unref(uid, entry)
	}, nil
}

func (g *sessionGuard) unref(uid string, entry *sessionGuardEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(g.entries, uid)
	}
}
