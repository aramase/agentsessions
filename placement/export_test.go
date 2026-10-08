package placement

import (
	"testing"
	"time"
)

// SetDescribeTimeout shortens how long admission waits for Describe, for the duration of a test.
func SetDescribeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := describeTimeout
	describeTimeout = d
	t.Cleanup(func() { describeTimeout = old })
}
