package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AgentWork admits captured host actions under their original visible Task.
// Assignment is not execution permission; Start separately compares authority.
type AgentWork struct {
	queue         *workqueue.Repository
	records       *etcd.HierarchyDeletionRepository
	attachPlanner *attachplan.RemovalPlanner
}

func NewAgentWork(queue *workqueue.Repository, records *etcd.HierarchyDeletionRepository, attachPlanner *attachplan.RemovalPlanner) (*AgentWork, error) {
	if queue == nil || records == nil || attachPlanner == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy host work requires captured storage")
	}
	return &AgentWork{queue: queue, records: records, attachPlanner: attachPlanner}, nil
}

func (owner *AgentWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.Hierarchy ||
		item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready || item.ActiveAttemptID != "" || at.IsZero() || at.Before(item.UpdatedAt) {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "removal host action cannot obtain this assignment")
	}
	if task.Stop != nil {
		change, err := owner.Stop(ctx, request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	renewal, err := owner.records.PrepareHierarchyDeadline(ctx, request, at)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if len(renewal.Mutations) != 0 {
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: renewal}, nil
	}
	if !at.Before(item.Deadline) {
		if item.AttemptNumber != 0 {
			_, previous, err := owner.queue.ReadAttempt(ctx, task.ID, item.ID, item.AttemptNumber, request.ReadRevision)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			if previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil {
				return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "expired removal Retry has no saved preceding result")
			}
			change, err := workqueue.PrepareQueuedRetrySettlement(request.Task, request.Item, workexecution.Completion{
				Outcome: workexecution.Failed, Evidence: *previous.Attempt.Result, EffectsSettled: true,
			}, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
		}
		change, err := owner.records.PrepareHierarchyUnstartedStop(ctx, request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "removal assignment belongs to another host")
	}
	input, err := owner.records.LoadHierarchyWork(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision})
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if input.Action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureAgent || input.Action.AgentProcedure == nil ||
		input.Operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen || input.Operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "captured removal no longer permits host execution")
	}
	deadline := at.Add(time.Duration(input.Action.AgentProcedure.TimeoutSeconds) * time.Second)
	if item.Deadline.Before(deadline) {
		deadline = item.Deadline
	}
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	if input.Action.ActionKind == hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup {
		environment, err := owner.records.LoadHierarchyEnvironmentInput(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision})
		if err != nil {
			return workdispatch.Decision{}, err
		}
		clear(environment.Input.ComposeArtifact)
		input.Conditions = environment.Conditions
	}
	if input.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		change, err := owner.records.HierarchyAttachAdmission(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision})
		if err != nil {
			return workdispatch.Decision{}, err
		}
		input.Conditions = change.Conditions
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: workqueue.Change{Conditions: input.Conditions}}, nil
}

func (owner *AgentWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil || task.Task.Action != workexecution.ActionRemove || item.Item.Operation != workexecution.Hierarchy ||
		item.Item.Placement.Executor != workexecution.Agent || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal host work has no matching stop request")
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Cleanup == workexecution.CleanupNone {
		return owner.records.PrepareHierarchyUnstartedStop(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	// Active effects remain owned until the executor accounts for them. Abort
	// never releases a host slot or resource claim on the strength of a timeout.
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	return owner.records.PrepareHierarchyAgentStart(ctx, view)
}
