package etcd

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyForwardRetry reopens only captured host work whose preceding
// failure has no unresolved effect. It retains prior failure/attempt evidence and
// the hierarchy lock; changing desired state cannot change this retry's source.
func (repository *HierarchyDeletionRepository) PrepareHierarchyForwardRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, time.Duration, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.Action != workexecution.ActionRemove || task.Stop != nil || task.CollectionStartedAt != nil ||
		item.Operation != workexecution.Hierarchy || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed || item.ForwardRevoked || item.Input == nil || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone &&
			item.Cleanup != workexecution.CleanupComplete || attempt.FinishedAt == nil ||
		attempt.Result == nil || attempt.Input != *item.Input || attempt.Number != item.AttemptNumber {
		return workqueue.Change{}, 0, errs.New(
			errs.KindTaskNotRetryable,
			"removal Retry requires a failed attempt with settled effects",
		)
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, 0, err
	}
	basis := attempt
	var recoveryCondition []keyvalue.Condition
	if attempt.Direction == workexecution.Recovery {
		if item.Cleanup != workexecution.CleanupComplete || attempt.Outcome != workexecution.Succeeded ||
			item.CleanupResult == nil ||
			*item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, 0, errs.New(
				errs.KindTaskNotRetryable,
				"removal requires a successful captured recovery before forward Retry",
			)
		}
		if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
			basis, recoveryCondition, err = repository.hierarchyAttachRetryBasis(ctx, view, attempt)
			if err != nil {
				return workqueue.Change{}, 0, err
			}
		} else {
			recovery, err := repository.LoadHierarchyEnvironmentRecovery(ctx, view)
			if err != nil {
				return workqueue.Change{}, 0, err
			}
			basis = recovery.Source.Forward
			recoveryCondition = []keyvalue.Condition{{Key: hierarchydeletion.EnvironmentRecoveryKey(item.ID), ModRevision: recovery.Revision}}
		}
	} else if attempt.Direction != workexecution.Forward || item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, 0, errs.New(errs.KindTaskNotRetryable, "removal Retry has no failed forward execution")
	}
	if basis.Result == nil || *basis.Result != *item.Result {
		return workqueue.Change{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup &&
		captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionAttachDeprovision ||
		captured.Action.AgentProcedure == nil ||
		captured.Operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		captured.Operation.Fence.Generation == math.MaxInt64 {
		return workqueue.Change{}, 0, errs.New(errs.KindTaskNotRetryable, "captured removal does not permit this Retry")
	}
	budget := time.Duration(captured.Action.AgentProcedure.TimeoutSeconds) * time.Second
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
	}
	failureKey := hierarchydeletion.EnvironmentFailureKey(basis.ID)
	var sourceConditions []keyvalue.Condition
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		source, err := repository.LoadHierarchyAttachCapture(ctx, view)
		if err != nil {
			return workqueue.Change{}, 0, err
		}
		defer source.Clear()
		if source.Revision <= 0 {
			return workqueue.Change{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
		}
		sourceConditions, failureKey = source.Conditions, attachremoval.WorkFailureKey(basis.ID)
	} else {
		source, err := repository.LoadHierarchyEnvironmentInput(ctx, view)
		if err != nil {
			return workqueue.Change{}, 0, err
		}
		clear(source.Input.ComposeArtifact)
		sourceConditions = source.Conditions
	}
	summaryKey := hierarchydeletion.WorkSummaryKey(task.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{failureKey, summaryKey}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, 0, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil {
		return workqueue.Change{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var checkpoint *workexecution.Checkpoint
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		failure, err := attachexecution.DecodeRemovalFailure(basis, read.Values[0].Value, *basis.Result)
		if err != nil {
			return workqueue.Change{}, 0, err
		}
		checkpoint = failure.Checkpoint
	} else {
		failure, err := hierarchyexecution.DecodeEnvironmentFailure(basis, read.Values[0].Value, *basis.Result)
		if err != nil {
			return workqueue.Change{}, 0, err
		}
		checkpoint = failure.Checkpoint
	}
	if attempt.Direction == workexecution.Forward && checkpoint != nil && checkpoint.Sequence%2 != 0 {
		return workqueue.Change{}, 0, errs.New(
			errs.KindTaskNotRetryable,
			"interrupted removal still requires effect inspection before Retry",
		)
	}
	change := workqueue.Change{Conditions: append(sourceConditions,
		keyvalue.Condition{Key: failureKey, ModRevision: read.Values[0].ModRevision})}
	change.Conditions = append(change.Conditions, recoveryCondition...)
	fence, err := prepareHierarchyRetryFence(view.Task, captured.Operation, read.Values[1], at)
	if err != nil {
		return workqueue.Change{}, 0, err
	}
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	return change, budget, nil
}
