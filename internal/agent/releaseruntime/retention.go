package releaseruntime

import "context"

// Retire settles the saved transport receipt through the operation journal.
// Failed Releases remain available until exact cleanup has been acknowledged.
func (executor *Executor) Retire(ctx context.Context, attemptID string) error {
	return executor.journal.SettleTerminalDelivery(ctx, attemptID)
}

func (executor *RecoveryExecutor) Retire(ctx context.Context, attemptID string) error {
	return executor.journal.SettleTerminalDelivery(ctx, attemptID)
}
