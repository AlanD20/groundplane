package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyEnvironmentFailure accounts for the worker stopping, not for
// an interrupted Docker call completing. An in-flight boundary keeps claims;
// an acknowledged between-effects boundary can release its execution ownership.
func (repository *HierarchyDeletionRepository) PrepareHierarchyEnvironmentFailure(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, reason hierarchyexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareHierarchyEnvironmentFailure(ctx, view, input, reason, at, false)
}

func (repository *HierarchyDeletionRepository) PrepareInterruptedHierarchyEnvironmentFailure(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareHierarchyEnvironmentFailure(ctx, view, input, hierarchyexecution.ExecutionInterrupted, at, true)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyEnvironmentFailure(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, reason hierarchyexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 || view.Attempt.Attempt.FinishedAt != nil ||
		view.Work.Item.Item.Operation != workexecution.Hierarchy || !workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "Environment removal failure no longer owns its active attempt")
	}
	source, err := repository.LoadHierarchyEnvironmentInput(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	clear(source.Input.ComposeArtifact)
	recovery, err := repository.hierarchyEnvironmentRecoveryConditions(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	checkpoint := view.Checkpoint.Record
	change := workqueue.Change{Conditions: append(source.Conditions,
		keyvalue.Condition{Key: hierarchydeletion.EnvironmentFailureKey(input.Attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(input.Attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(input.Attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision})}
	settled := checkpoint == nil
	change.Conditions = append(change.Conditions, recovery...)
	if checkpoint != nil {
		key := hierarchydeletion.EnvironmentReportKey(input.Attempt.ID, checkpoint.Sequence)
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		if read != nil {
			defer keyvalue.ClearValues(read.Values)
		}
		if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
			return workqueue.Change{}, workexecution.Completion{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		report, err := hierarchyexecution.DecodeEnvironmentReport(input, read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		expected, err := report.Checkpoint(input)
		if err != nil || expected.Evidence != checkpoint.Evidence || expected.Sequence != checkpoint.Sequence ||
			expected.StageID != checkpoint.StageID || !expected.RecordedAt.Equal(checkpoint.RecordedAt) ||
			report.Progress.Phase == hierarchyexecution.EnvironmentRemoved {
			return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "Environment removal failure differs from acknowledged progress; completed removal must report completion")
		}
		settled = !report.Progress.InFlight
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	}
	if input.Attempt.Direction == workexecution.Recovery {
		// Failure of recovery cannot settle the original unknown effect.
		settled = false
	}
	failure := hierarchyexecution.EnvironmentFailure{AttemptID: input.Attempt.ID, Input: input.Attempt.Input,
		Reason: reason, Checkpoint: checkpoint, RecordedAt: at}
	raw, evidence, err := hierarchyexecution.EncodeEnvironmentFailure(input.Attempt, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: hierarchydeletion.EnvironmentFailureKey(input.Attempt.ID), Value: raw}}
	return change, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}, nil
}

func (repository *HierarchyDeletionRepository) LoadHierarchyEnvironmentFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if view.Work.Item.Item.Operation != workexecution.Hierarchy || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, errs.New(errs.KindStateConflict, "Environment removal failure has no retained terminal attempt")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hierarchydeletion.EnvironmentFailureKey(attempt.ID)}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if _, err := hierarchyexecution.DecodeEnvironmentFailure(attempt, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, err
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
