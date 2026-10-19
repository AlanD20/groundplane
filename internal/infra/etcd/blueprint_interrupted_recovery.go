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

// QueueInterruptedBlueprintRecovery fences the old forward attempt and queues
// inspection/restoration. It does not assert termination, undo effects or release
// resource claims. Recovery must prove quiescence before any restorative write.
func (ledger *ReleaseLedger) QueueInterruptedBlueprintRecovery(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.Input, authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	replacement, attempt := authorization.Identity(), view.Attempt.Attempt
	if ledger == nil || ledger.store == nil || replacement.Executor != workexecution.Agent ||
		attempt.Executor.Executor != workexecution.Agent || replacement.HostID != attempt.Executor.HostID ||
		replacement.Generation <= attempt.Executor.Generation {
		return errs.New(errs.KindStateConflict, "Release interruption requires its replacement host executor")
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
		return errs.New(errs.KindStateConflict, "replacement Agent changed during Release interruption")
	}
	completion, err := queue.ReadInterruptedReleaseAt(ctx, view)
	if err != nil {
		return err
	}
	if !workexecution.SameAssignment(input.Attempt, attempt) {
		return errs.New(errs.KindStateConflict, "interrupted Release input changed attempt")
	}
	progress, err := releasework.Load(ctx, ledger.store, input, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if progress.Record == nil {
		return errs.New(errs.KindStateConflict, "interrupted Release progress is unavailable")
	}
	if progress.Record.Progress.Phase == releaseexecution.Complete {
		return ledger.SettleInterruptedBlueprintRelease(ctx, view, input, authorization, at)
	}
	sources, err := ledger.LoadReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	budget := sources.Rollout.Record.Deadline.Sub(sources.Rollout.Record.CreatedAt)
	if budget <= 0 || budget > 24*time.Hour {
		return errs.New(errs.KindStateConflict, "Release recovery budget is invalid")
	}
	failure := releaseexecution.Failure{AttemptID: attempt.ID, Input: attempt.Input,
		Reason: releaseexecution.ExecutionInterrupted, Checkpoint: view.Checkpoint.Record, RecordedAt: at}
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
		EffectsSettled: false,
	}
	change, err := workqueue.PrepareCompletionWithRecovery(completion, at.Add(budget))
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	evidence := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.ExecutorKey(replacement), ModRevision: current.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: releasework.ProgressKey(attempt.ID), ModRevision: progress.Revision},
		{Key: releasework.FailureKey(reference.ID)},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: releasework.FailureKey(reference.ID), Value: raw}}}
	result, err := queue.Commit(
		ctx,
		workqueue.Change{Conditions: guards},
		workqueue.Change{Conditions: sources.Conditions},
		evidence,
		change,
	)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "interrupted Release changed before recovery publication")
	}
	return nil
}
