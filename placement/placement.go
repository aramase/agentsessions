// Package placement wires the Sessions layer to the Runtime compute SPI. The Placer owns the
// incarnation lifecycle for a session: it drives Runtime.Create, mints the fence from the log (the
// single fence authority), stamps it on the incarnation, and binds a controller to that fence. Keeping
// this seam out of session.Service keeps the gRPC layer thin and the SPI logic unit-testable, and lets
// cmd/agentnode reuse it.
package placement

import (
	"context"
	"errors"
	"fmt"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
)

// ErrUnplaceable is returned when a harness requires a capability the chosen backend cannot provide
// (e.g. REQUIRES_MEMORY_SNAPSHOT on a filesystem-only pod). The gate runs BEFORE Create, so an
// unplaceable harness never provisions compute or writes to the log; session.Service surfaces it as
// codes.FailedPrecondition.
var ErrUnplaceable = errors.New("placement: harness cannot be placed on this runtime")

// Backend is the compute Runtime the Placer drives. In the in-process M0 path it also provides the
// harness handle directly via Harness(); the socket variant (design note §10 step 6) will instead have
// the Placer dial Incarnation.Address to build a ClientHarness. runtime/local satisfies this.
type Backend interface {
	api.Runtime
	Harness() api.Harness
}

// Placer owns the incarnation lifecycle: Create the compute, mint+bind the fence, drive the controller.
type Placer struct {
	backend Backend
	model   controller.ModelFunc
}

// New builds a Placer over a compute backend and the live model.
func New(backend Backend, model controller.ModelFunc) *Placer {
	return &Placer{backend: backend, model: model}
}

// Exec places one turn: Create the incarnation, mint the fence from the log and stamp it on the
// incarnation, bind a controller to that same token, and drive the (placed) harness. The log stays the
// single fence authority; the returned incarnation carries the fence for Suspend/Resume (step 5).
func (p *Placer) Exec(ctx context.Context, log eventlog.Store, sessionUID string, inputs []api.Message, expectedLastSeq int64) (api.Incarnation, error) {
	// Placement gate (honest degradation): refuse a harness the backend cannot host BEFORE
	// provisioning any compute or writing to the log — e.g. a REQUIRES_MEMORY_SNAPSHOT harness on a
	// filesystem-only backend — so it fails fast at the API instead of mid-run.
	desc, err := p.backend.Harness().Describe(ctx)
	if err != nil {
		return api.Incarnation{}, err
	}
	if !controller.CanPlace(desc.Capabilities, p.backend.Capabilities()) {
		return api.Incarnation{}, fmt.Errorf("%w: harness %q needs %s but the runtime provides MemorySnapshot=%v",
			ErrUnplaceable, desc.ID, desc.Capabilities.Resumability, p.backend.Capabilities().MemorySnapshot)
	}
	inc, err := p.backend.Create(ctx, &api.SessionSpec{SessionUID: sessionUID})
	if err != nil {
		return api.Incarnation{}, err
	}
	fence, err := log.NewFence()
	if err != nil {
		return api.Incarnation{}, err
	}
	inc.FenceToken = fence // Placer-owned: the incarnation carries the token Suspend/Resume will need
	c, err := controller.New(log, p.model, controller.WithFence(fence))
	if err != nil {
		return api.Incarnation{}, err
	}
	if err := c.Exec(ctx, p.backend.Harness(), inputs, expectedLastSeq); err != nil {
		return inc, err
	}
	return inc, nil
}

// Harness and Model expose the placed harness and live model for lifecycle ops not yet migrated onto
// the Placer (Resume). Interim accessors — step 5 adds Placer.Resume/Suspend/Fork and removes these.
func (p *Placer) Harness() api.Harness        { return p.backend.Harness() }
func (p *Placer) Model() controller.ModelFunc { return p.model }
