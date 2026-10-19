package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyEnvironmentRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionRemove ||
		item.Operation != workexecution.Hierarchy || item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Settled ||
		item.Result == nil || item.Input == nil || item.ActiveAttemptID != "" || task.CollectionStartedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.RetainUntil) || view.ReadRevision <= 0 {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Environment removal is not eligible for retention",
		)
	}
	if task.Outcome != workexecution.Succeeded {
		return false, workqueue.Change{}, nil
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup ||
		captured.Operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed ||
		captured.Operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionRetained {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"host removal evidence requires its completed retained hierarchy",
		)
	}
	source, err := repository.LoadHierarchyEnvironmentInput(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer clear(source.Input.ComposeArtifact)
	completionKey, err := hierarchydeletion.HierarchyDeletionCompletionKey(
		captured.Action.ParentOperationID,
		captured.Action.Ordinal,
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	summaryKey := hierarchydeletion.WorkSummaryKey(task.ID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(
		string(captured.Operation.Tombstone.TargetKind),
		captured.Operation.Tombstone.TargetID,
	)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{completionKey, summaryKey, lockKey}, Revision: view.ReadRevision},
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 3 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[2] != nil {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	result, err := hierarchydeletion.DecodeEnvironmentResult(read.Values[0].Value)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	input, err := result.Input(source.Input)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	checkpoint, err := result.Report.Checkpoint(input)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	summary, err := hierarchydeletion.DecodeWorkSummary(read.Values[1].Value)
	if err != nil || summary.TaskID != task.ID || summary.OperationID != captured.Action.ParentOperationID ||
		summary.Outcome != task.Outcome || summary.Count != task.ExpectedItems || task.FinishedAt == nil || !summary.CompletedAt.Equal(*task.FinishedAt) ||
		captured.Operation.Tombstone.PlanDigest == nil || summary.PlanDigest != *captured.Operation.Tombstone.PlanDigest ||
		result.Attempt.TaskID != task.ID || result.Attempt.WorkItemID != item.ID || result.Attempt.Input != *item.Input ||
		result.Attempt.Number != item.AttemptNumber || result.Attempt.Executor.HostID != item.Placement.HostID ||
		item.Outcome != workexecution.Succeeded || checkpoint.Evidence != *item.Result ||
		input.Removal.CapturedSource.InputId != ids.DeriveAt(
			ids.KindPlan,
			task.CreatedAt,
			item.ID,
			"environment-removal-source",
		) {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change := workqueue.Change{Conditions: append(source.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: completionKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: summaryKey, ModRevision: read.Values[1].ModRevision}, keyvalue.Condition{Key: lockKey})}
	if final {
		actionKey, err := hierarchydeletion.HierarchyDeletionActionKey(
			captured.Action.ParentOperationID,
			captured.Action.Ordinal,
		)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		for _, key := range []string{actionKey, completionKey, hierarchydeletion.WorkBindingKey(item.ID), hierarchydeletion.WorkStopKey(item.ID),
			hierarchydeletion.EnvironmentInputKey(captured.Action.ParentOperationID, captured.Action.TargetID), hierarchydeletion.EnvironmentRecoveryKey(item.ID)} {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Prefix: true,
			Key: hierarchydeletion.WorkStopEvidencePrefix(item.ID)})
	}
	return true, change, nil
}
