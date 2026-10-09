package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/aramase/agentsessions/api"
)

const describeTimeout = 10 * time.Second

// Bound remote Describe as placement does, without extending an earlier caller deadline.
func describeHarness(ctx context.Context, har api.Harness) (api.Descriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()
	desc, err := har.Describe(ctx)
	if err != nil {
		return api.Descriptor{}, fmt.Errorf("%w: %w", ErrHarnessDescribeFailed, err)
	}
	return desc, nil
}

func (c *Controller) resolvedHarnessName(desc api.Descriptor) string {
	if c.harnessName != "" {
		return c.harnessName
	}
	return desc.ID
}

// checkHarness preflights the selected invocations against the single supplied harness. Legacy
// invocations without identity evidence do not introduce a Describe dependency. Version matching
// is opt-in per invocation; recorded nonempty versions require an exact match, even if served empty.
func (c *Controller) checkHarness(ctx context.Context, har api.Harness, executions []recordedExecution) error {
	needsDescription := false
	for _, execution := range executions {
		if execution.harness != "" || execution.harnessVersion != "" {
			needsDescription = true
			break
		}
	}
	if !needsDescription {
		return nil
	}
	desc, err := describeHarness(ctx, har)
	if err != nil {
		return err
	}
	name := c.resolvedHarnessName(desc)
	for _, execution := range executions {
		if execution.harness != "" && execution.harness != name {
			return fmt.Errorf("%w: execution %q recorded %q, supplied %q", ErrHarnessMismatch, execution.id, execution.harness, name)
		}
		if execution.harnessVersion != "" && execution.harnessVersion != desc.Version {
			return fmt.Errorf("%w: execution %q recorded %q, supplied %q", ErrHarnessVersionMismatch, execution.id, execution.harnessVersion, desc.Version)
		}
	}
	return nil
}
