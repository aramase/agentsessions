// Package echo is a minimal deterministic harness for the walking skeleton. It satisfies
// the I0 precondition (a deterministic function of its recorded inputs) and performs its
// one nondeterministic operation — the model call — through the host Effects interface,
// so the host can serve it from the journal on replay.
package echo

import "github.com/aramase/agentsessions/host"

// Harness echoes the model's response for the input.
type Harness struct{}

func (Harness) Run(e host.Effects, input string) error {
	resp, err := e.Model(host.ModelRequest{Prompt: input})
	if err != nil {
		return err
	}
	return e.Emit(resp.Text)
}
