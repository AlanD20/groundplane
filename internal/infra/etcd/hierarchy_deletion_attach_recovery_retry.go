package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyAttachRecoveryRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.Action != workexecution.ActionRemove || task.CollectionStartedAt != nil ||
		item.Operation != workexecution.Hierarchy ||
		item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Attention ||
		item.ActiveAttemptID != "" ||
		item.Input == nil ||
		attempt.Result == nil ||
		attempt.FinishedAt == nil ||
		attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"removal has no stopped captured recovery to retry",
		)
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionAttachDeprovision ||
		captured.Action.AgentProcedure == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "captured removal kind has no Attach recovery")
	}
	capture, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Revision <= 0 {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	budget := time.Duration(captured.Action.AgentProcedure.TimeoutSeconds) * time.Second
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	key := attachremoval.WorkRecoveryKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{key, attachremoval.WorkFailureKey(attempt.ID)},
			Revision: view.ReadRevision,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var source attachexecution.RemovalRecovery
	var revision int64
	if read.Values[0] != nil {
		revision = read.Values[0].ModRevision
	}
	if attempt.Direction == workexecution.Forward {
		if read.Values[1] == nil || item.Result == nil || *item.Result != *attempt.Result {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		failure, err := attachexecution.DecodeRemovalFailure(attempt, read.Values[1].Value, *attempt.Result)
		if err != nil {
			return workqueue.Change{}, err
		}
		source = attachexecution.RemovalRecovery{Forward: attempt, Failure: failure, Evidence: *attempt.Result}
	} else {
		if read.Values[0] == nil || item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		source, err = attachexecution.DecodeRemovalRecovery(read.Values[0].Value)
		if err != nil || source.Forward.TaskID != task.ID || source.Forward.WorkItemID != item.ID || source.Forward.Input != *item.Input ||
			source.Forward.Executor.HostID != item.Placement.HostID || source.Forward.Number >= attempt.Number {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	value, err := attachexecution.EncodeRemovalRecovery(source)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	if attempt.Direction == workexecution.Forward {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: attachremoval.WorkFailureKey(attempt.ID), ModRevision: read.Values[1].ModRevision},
		)
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
