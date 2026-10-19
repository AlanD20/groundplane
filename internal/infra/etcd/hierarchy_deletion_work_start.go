package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyAgentStart supplies removal-specific authority to the shared
// Start transaction. The shared owner adds authenticated process generation,
// attempt, claims and deadline comparisons; these guards cannot grant Start alone.
func (repository *HierarchyDeletionRepository) PrepareHierarchyAgentStart(
	ctx context.Context,
	view workqueue.ResumeView,
) (workqueue.Change, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || task.Action != workexecution.ActionRemove ||
		item.Operation != workexecution.Hierarchy || item.Placement.Executor != workexecution.Agent ||
		attempt.Direction == workexecution.Forward &&
			(task.Stop != nil || item.ForwardRevoked) || attempt.StartedAt != nil ||
		attempt.Executor.HostID != item.Placement.HostID || workexecution.ValidateDelivery(item, attempt) != nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"removal host action no longer owns Start authority",
		)
	}
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if captured.Action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureAgent ||
		captured.Action.AgentProcedure == nil ||
		captured.Operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen ||
		captured.Operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"captured removal is no longer open for host execution",
		)
	}
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup {
		environment, err := repository.LoadHierarchyEnvironmentInput(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		clear(environment.Input.ComposeArtifact)
		if attempt.Direction == workexecution.Recovery {
			recovery, err := repository.LoadHierarchyEnvironmentRecovery(ctx, view.Work)
			if err != nil {
				return workqueue.Change{}, err
			}
			if err := recovery.Source.ValidateFor(attempt); err != nil {
				return workqueue.Change{}, err
			}
			environment.Conditions = append(environment.Conditions, keyvalue.Condition{
				Key: hierarchydeletion.EnvironmentRecoveryKey(item.ID), ModRevision: recovery.Revision})
		}
		return workqueue.Change{Conditions: environment.Conditions}, nil
	}
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		change, err := repository.HierarchyAttachAdmission(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		if attempt.Direction == workexecution.Recovery {
			recovery, err := repository.LoadHierarchyAttachRecovery(ctx, view.Work)
			if err != nil {
				return workqueue.Change{}, err
			}
			if err := recovery.Source.ValidateFor(attempt); err != nil {
				return workqueue.Change{}, err
			}
			change.Conditions = append(change.Conditions, keyvalue.Condition{
				Key: attachremoval.WorkRecoveryKey(item.ID), ModRevision: recovery.Revision})
		}
		return change, nil
	}
	return workqueue.Change{Conditions: captured.Conditions}, nil
}
