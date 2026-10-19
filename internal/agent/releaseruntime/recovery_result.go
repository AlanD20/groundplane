package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryResult struct {
	Report  *releaseexecution.RecoveryReport
	Failure *releaseexecution.FailureReceipt
}

func (executor *RecoveryExecutor) Execute(
	ctx context.Context,
	assignment *agentpb.ReleaseRecoveryAssignment,
	identity workexecution.Identity,
	exchange RecoveryExchange,
) (RecoveryResult, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return RecoveryResult{}, errs.New(errs.KindInternal, "Release recovery execution is not configured")
	}
	input, err := OpenRecovery(assignment, identity, executor.volumeRoot)
	if err != nil {
		return RecoveryResult{}, err
	}
	pending, err := executor.journal.LoadReleaseFailure(ctx, input.Input.Attempt)
	if err != nil {
		return RecoveryResult{}, err
	}
	if pending != nil {
		if pending.Receipt != nil {
			return RecoveryResult{Failure: pending.Receipt}, nil
		}
		receipt, err := recordReleaseFailure(ctx, executor.journal, input.Input.Attempt, pending.Reason, exchange)
		return RecoveryResult{Failure: receipt}, err
	}
	report, executionErr := executor.execute(ctx, input, exchange)
	if executionErr == nil {
		return RecoveryResult{Report: report}, nil
	}
	// Delivery failure after restoration must replay completion, never convert
	// a completed recovery into a new failure or repeat its host actions.
	inspect, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	_, saved, err := executor.journal.LoadRecovery(inspect, input)
	cancel()
	if err != nil {
		return RecoveryResult{}, err
	}
	if saved != nil && saved.Progress.Phase == releaseexecution.RecoveryComplete {
		return RecoveryResult{Report: saved}, executionErr
	}
	reason := releaseexecution.ExecutionFailed
	if !time.Now().Before(input.Input.Attempt.Deadline) {
		reason = releaseexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = releaseexecution.ExecutionInterrupted
	}
	receipt, err := recordReleaseFailure(ctx, executor.journal, input.Input.Attempt, reason, exchange)
	return RecoveryResult{Failure: receipt}, err
}
