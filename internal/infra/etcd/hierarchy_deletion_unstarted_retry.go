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

func (repository *HierarchyDeletionRepository) PrepareHierarchyUnstartedRetry(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionRemove || task.Stop != nil || task.CollectionStartedAt != nil ||
		(item.Operation != workexecution.Hierarchy && item.Operation != workexecution.RemovalBackupObject) || item.AttemptNumber != 0 || item.Input == nil || item.Result == nil ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"removal Retry requires captured work that failed before execution",
		)
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	var attachCapture workqueue.Change
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		attachCapture, err = repository.prepareHierarchyAttachUnstartedRetry(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	defer func() { keyvalue.ClearMutationValues(attachCapture.Mutations) }()
	budget := time.Duration(captured.Operation.Intent.TimeoutSeconds) * time.Second
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	stopKey := hierarchydeletion.WorkStopKey(item.ID)
	summaryKey := hierarchydeletion.WorkSummaryKey(task.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{stopKey, summaryKey}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil ||
		item.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "hierarchy-stop") ||
		item.Result.SHA256 != hierarchydeletion.HierarchyDeletionBytesDigest(read.Values[0].Value) {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change, err := workqueue.PrepareUnstartedRetry(view.Task, view.Item, at.Add(budget), at)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := prepareHierarchyRetryFence(view.Task, captured.Operation, read.Values[1], at)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: stopKey, ModRevision: read.Values[0].ModRevision},
	)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	// Preserve the prior stop proof under its content identity before clearing
	// the current result slot, which a later queued timeout may populate again.
	evidenceKey := hierarchydeletion.WorkStopEvidenceKey(item.ID, item.Result.SHA256)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: evidenceKey})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   evidenceKey,
			Value: append([]byte(nil), read.Values[0].Value...),
		},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: stopKey},
	)
	change.Conditions = append(change.Conditions, attachCapture.Conditions...)
	change.Mutations = append(change.Mutations, attachCapture.Mutations...)
	attachCapture.Mutations = nil
	return change, nil
}
