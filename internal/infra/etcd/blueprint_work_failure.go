package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleFailedBlueprintRelease releases execution capacity, not unresolved host
// ownership. Captured inputs and conflict claims survive for native recovery.
// Desired heads are deliberately not consulted when recording the original failure.
func (ledger *ReleaseLedger) SettleFailedBlueprintRelease(ctx context.Context, view workqueue.ResumeView, input releaseexecution.Input,
	reason releaseexecution.FailureReason, authorization []keyvalue.Condition, at time.Time,
) error {
	if ledger == nil || ledger.store == nil || len(authorization) == 0 {
		return errs.New(errs.KindStateConflict, "Release failure requires authenticated execution authority")
	}
	sources, err := ledger.LoadBlueprintReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	failure, result, err := releasework.PrepareFailure(ctx, ledger.store, view, input, reason, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(failure.Mutations)
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	request, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	request.At, request.Result = at, result
	var execution workqueue.Change
	if result.EffectsSettled {
		execution, err = workqueue.PrepareCompletion(request)
	} else {
		budget := sources.Rollout.Record.Deadline.Sub(sources.Rollout.Record.CreatedAt)
		if budget <= 0 || budget > 24*time.Hour {
			return errs.New(errs.KindStateConflict, "Release cleanup has no valid captured execution budget")
		}
		execution, err = workqueue.PrepareCompletionWithRecovery(request, at.Add(budget))
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	committed, err := queue.Commit(ctx, workqueue.Change{Conditions: authorization}, workqueue.Change{Conditions: sources.Conditions}, failure, execution)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Release failure state changed; reload the same attempt")
	}
	return nil
}

func (ledger *ReleaseLedger) SettleFailedBlueprintRecovery(ctx context.Context, view workqueue.ResumeView, input releaseexecution.RecoveryInput,
	reason releaseexecution.FailureReason, authorization []keyvalue.Condition, at time.Time,
) error {
	if ledger == nil || ledger.store == nil || len(authorization) == 0 {
		return errs.New(errs.KindStateConflict, "Release recovery failure requires authenticated execution authority")
	}
	failure, result, err := releasework.PrepareRecoveryFailure(ctx, ledger.store, view, input, reason, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(failure.Mutations)
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	request, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	request.At, request.Result = at, result
	execution, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	committed, err := queue.Commit(ctx, workqueue.Change{Conditions: authorization}, failure, execution)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Release recovery failure changed; reload the same attempt")
	}
	return nil
}
