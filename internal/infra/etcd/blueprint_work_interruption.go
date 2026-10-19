package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RejectBlueprintReleaseBeforeEffects uses absence of a checkpoint grant, not
// absence of observed containers, as proof that this attempt had no effects.
// Start alone cannot authorize a Release effect. The replaced generation cannot
// obtain another grant after the joined executor/checkpoint comparisons.
func (ledger *ReleaseLedger) RejectBlueprintReleaseBeforeEffects(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
	at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	replacement, attempt := authorization.Identity(), view.Attempt.Attempt
	if ledger == nil || ledger.store == nil || view.Work.Item.Item.Operation != workexecution.Release ||
		replacement.Executor != workexecution.Agent || attempt.Executor.Executor != workexecution.Agent ||
		replacement.HostID != attempt.Executor.HostID || replacement.Generation <= attempt.Executor.Generation {
		return errs.New(errs.KindStateConflict, "interrupted Release rejection requires its replacement host executor")
	}
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	current, err := queue.ReadExecutor(ctx, view.Work.Item.Item.Placement)
	if err != nil {
		return err
	}
	if current.Revision != view.ExecutorRevision || current.Record.Identity != replacement {
		return errs.New(errs.KindStateConflict, "replacement Agent changed during interrupted Release settlement")
	}
	completion, err := queue.ReadReleaseBeforeEffectsAt(ctx, view)
	if err != nil {
		return err
	}
	failure := releaseexecution.Failure{
		AttemptID:  attempt.ID,
		Input:      attempt.Input,
		Reason:     releaseexecution.ExecutionInterrupted,
		RecordedAt: at,
	}
	raw, reference, err := releaseexecution.EncodeFailure(attempt, failure)
	if err != nil {
		return err
	}
	defer clear(raw)
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	completion.At, completion.Result = at, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       reference,
		EffectsSettled: true,
	}
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	evidence := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.ExecutorKey(replacement), ModRevision: current.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID)},
		{Key: releasework.ProgressKey(attempt.ID)},
		{Key: releasework.FailureKey(reference.ID)},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: releasework.FailureKey(reference.ID), Value: raw}}}
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: guards}, evidence, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "interrupted Release changed before settlement")
	}
	return nil
}
