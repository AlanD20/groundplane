package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || item.Operation != workexecution.Hierarchy || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready ||
		item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped ||
		at.IsZero() || at.Before(item.UpdatedAt) {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "removal recovery cannot obtain this assignment")
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	if request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "removal recovery belongs to another host")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	captured, err := owner.records.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		authority, err := owner.records.HierarchyAttachAdmission(ctx, view)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		recovery, err := owner.records.LoadHierarchyAttachRecovery(ctx, view)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		conditions := append(authority.Conditions, keyvalue.Condition{
			Key: attachremoval.WorkRecoveryKey(item.ID), ModRevision: recovery.Revision})
		return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
			Change: workqueue.Change{Conditions: conditions}}, nil
	}
	if captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "captured removal has no recovery handler")
	}
	source, err := owner.records.LoadHierarchyEnvironmentInput(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	clear(source.Input.ComposeArtifact)
	recovery, err := owner.records.LoadHierarchyEnvironmentRecovery(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	conditions := append(source.Conditions, keyvalue.Condition{
		Key: hierarchydeletion.EnvironmentRecoveryKey(item.ID), ModRevision: recovery.Revision})
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}
