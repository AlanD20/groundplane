package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// Rationale: recovery must run immediately at Controller startup, retry on the
// shared tick after a transient failure, and stop before owned infrastructure
// is closed.
func TestLocalAgentReconciliationRunsImmediatelyAndRetries(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLocalAgentLifecycle{called: make(chan struct{}, 2)}
	reconciliation, err := newLocalAgentReconciliation(
		lifecycle,
		30*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newLocalAgentReconciliation() error = %v", err)
	}
	if reconciliation.interval != 30*time.Second {
		t.Fatalf("reconciliation duration = %s, want 30s", reconciliation.interval)
	}
	ticks := make(chan time.Time, 1)
	reconciliation.after = func(time.Duration) <-chan time.Time { return ticks }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reconciliation.Run(ctx)
		close(done)
	}()

	awaitLocalAgentReconciliation(t, lifecycle.called)
	ticks <- time.Now()
	awaitLocalAgentReconciliation(t, lifecycle.called)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("local Agent reconciliation did not stop after cancellation")
	}
	if lifecycle.calls != 2 {
		t.Fatalf("Reconcile calls = %d, want 2", lifecycle.calls)
	}
}

type fakeLocalAgentLifecycle struct {
	calls  int
	called chan struct{}
}

func (lifecycle *fakeLocalAgentLifecycle) Reconcile(context.Context) error {
	lifecycle.calls++
	lifecycle.called <- struct{}{}
	return nil
}

func awaitLocalAgentReconciliation(t *testing.T, called <-chan struct{}) {
	t.Helper()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("local Agent reconciliation did not run")
	}
}
