package controller

import (
	"errors"
	"sync"
)

// ErrSinkClosed rejects effects from a retained sink after its Run has returned.
var ErrSinkClosed = errors.New("controller: invocation sink is closed")

// sinkGuard serializes one invocation's effects, including its resume/live tail. Private effect
// helpers run under this guard; they must not reacquire it. A stop is sticky even if Run handles it.
type sinkGuard struct {
	mu     sync.Mutex
	closed bool
	stop   error
}

func (g *sinkGuard) err() error {
	if g.stop != nil {
		return g.stop
	}
	if g.closed {
		return ErrSinkClosed
	}
	return nil
}

func (g *sinkGuard) close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	return g.stop
}
