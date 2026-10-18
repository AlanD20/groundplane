package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutionResult separates a recorded operation failure from a transport or
// journal failure. Neither a failure receipt nor worker completion proves cleanup.
type ExecutionResult struct {
	Report  *releaseexecution.CheckpointReport
	Failure *releaseexecution.FailureReceipt
}

func (executor *Executor) Execute(ctx context.Context, assignment *agentpb.ReleaseWorkAssignment, identity workexecution.Identity, exchange Exchange) (ExecutionResult, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return ExecutionResult{}, errs.New(errs.KindInternal, "native Release execution is not configured")
	}
	input, err := Open(assignment, identity, executor.volumeRoot)
	if err != nil {
		return ExecutionResult{}, err
	}
	if input.Attempt.Direction != workexecution.Forward {
		return ExecutionResult{}, errs.New(errs.KindStateConflict, "recovery assignments require the Release recovery executor")
	}
	pending, err := executor.journal.LoadReleaseFailure(ctx, input.Attempt)
	if err != nil {
		return ExecutionResult{}, err
	}
	if pending != nil {
		if pending.Receipt != nil {
			return ExecutionResult{Failure: pending.Receipt}, nil
		}
		return executor.fail(ctx, input.Attempt, pending.Reason, exchange)
	}
	report, err := executor.execute(ctx, input, exchange)
	if err == nil {
		return ExecutionResult{Report: report}, nil
	}
	// A completed operation awaiting its acknowledgement must only replay that
	// acknowledgement. Never convert a failed completion delivery into failure.
	if report != nil && report.Progress.Phase == releaseexecution.Complete {
		return ExecutionResult{Report: report}, err
	}
	inspect, cancelInspect := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	completed, journalErr := executor.journal.LoadReleaseCompletion(inspect, input)
	cancelInspect()
	if journalErr != nil {
		return ExecutionResult{}, journalErr
	}
	if completed != nil {
		return ExecutionResult{Report: &completed.Report}, err
	}
	reason := releaseexecution.ExecutionFailed
	if !time.Now().Before(input.Attempt.Deadline) {
		reason = releaseexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = releaseexecution.ExecutionInterrupted
	}
	return executor.fail(ctx, input.Attempt, reason, exchange)
}

func (executor *Executor) fail(ctx context.Context, attempt workexecution.Attempt, reason releaseexecution.FailureReason, exchange Exchange) (ExecutionResult, error) {
	receipt, err := recordReleaseFailure(ctx, executor.journal, attempt, reason, exchange)
	return ExecutionResult{Failure: receipt}, err
}

type failureExchange interface {
	FailRelease(context.Context, *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error)
}

func recordReleaseFailure(ctx context.Context, journal *agentworkjournal.Journal, attempt workexecution.Attempt, reason releaseexecution.FailureReason, exchange failureExchange) (*releaseexecution.FailureReceipt, error) {
	// Save before transport, including when the session was cancelled. This
	// bounded local write cannot authorize any new host effect.
	save, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	pending, err := journal.PrepareReleaseFailure(save, attempt, reason)
	cancelSave()
	if err != nil {
		return nil, err
	}
	if pending.Receipt != nil {
		return pending.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.FailRelease(bounded, &agentpb.ReleaseWorkFailureRequest{Attempt: identity, Reason: string(pending.Reason)})
	if err != nil {
		return nil, err
	}
	receipt, err := journal.AcknowledgeReleaseFailure(bounded, attempt, ack)
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}
