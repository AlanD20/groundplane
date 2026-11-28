package runnermigrationoperator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const activationTimeout = 5 * time.Minute

type Work struct {
	operator *Operator
	queue    *workqueue.Repository
}

func NewWork(operator *Operator, queue *workqueue.Repository) (*Work, error) {
	if operator == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Runner migration Task dependencies are required")
	}
	return &Work{operator: operator, queue: queue}, nil
}

func (work *Work) AcceptedPreparation() taskdependencies.Registration {
	registration := acceptedaction.Registration(work.operator.submitter, Kind, workexecution.ActionUpdate,
		ids.KindRunner, work.prepareAccepted)
	registration.Cleanup = work.cleanupPreparation
	return registration
}

func (work *Work) prepareAccepted(ctx context.Context, snapshot workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedMigration,
) (taskdependencies.Prepared, error) {
	task := workexecution.CloneTask(snapshot.Task)
	state, exists, err := work.operator.records.Load(ctx, input.RunnerID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if !exists || state.Record.Phase != runnerscoped.Pending || state.Record.ID != task.ID ||
		task.TargetID != input.RunnerID || state.Record.Candidate.Digest() != input.PlanDigest {
		return taskdependencies.Prepared{}, conflict("Runner migration Task has no matching capture")
	}
	ref := reference(task, state.Record)
	task.Intent, task.ExpectedItems, task.UpdatedAt = ref, 1, at
	item := workexecution.Item{ID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, Kind), TaskID: task.ID,
		Operation: workexecution.RunnerScopedMigration, TargetID: input.RunnerID, TargetName: task.TargetName,
		Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims:    []workexecution.Claim{{ScopeID: input.RunnerID}}, Input: &ref,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: acceptedaction.ExecutionDeadline(task, at, activationTimeout)}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: stateGuard(state)}, nil
}

func (work *Work) cleanupPreparation(ctx context.Context, task workqueue.TaskSnapshot, _ time.Time) (bool, error) {
	if task.Task.ExpectedItems != 0 {
		return true, nil
	}
	if task.Task.FinishedAt == nil || task.Task.Outcome == workexecution.Succeeded {
		return false, conflict("Runner migration Task is still preparing")
	}
	state, exists, err := work.operator.records.Load(ctx, task.Task.TargetID)
	if err != nil || !exists || state.Record.ID != task.Task.ID {
		return err == nil, err
	}
	cancel, err := work.operator.records.PrepareCancel(ctx, state)
	if err != nil {
		return false, err
	}
	result, err := work.queue.Commit(ctx, cancel, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
	}})
	return result.Succeeded, err
}

func (work *Work) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	state, err := work.load(ctx, item)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if request.Direction == workexecution.Recovery {
		if state.Record.Phase != runnerscoped.Cutover && state.Record.Phase != runnerscoped.Complete {
			return workdispatch.Decision{}, conflict("Runner migration recovery requires an issued cutover")
		}
		if !at.Before(item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
	} else if state.Record.Phase != runnerscoped.Pending || request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, conflict("Runner migration forward authority has changed")
	} else if request.Task.Task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) {
		change, err := work.cancelUnstarted(ctx, request.Task, request.Item, state, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    item.Deadline,
		Change:      stateGuard(state),
	}, nil
}

func (work *Work) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, conflict("Runner migration has no stop request")
	}
	if item.Item.AttemptNumber != 0 || item.Item.ActiveAttemptID != "" {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	state, err := work.load(ctx, item.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	return work.cancelUnstarted(ctx, task, item, state, at)
}

func (work *Work) cancelUnstarted(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	state keyvalue.Versioned[runnerscoped.Migration], at time.Time,
) (workqueue.Change, error) {
	result := workexecution.Completion{Outcome: failure(task, item), Evidence: *item.Item.Input, EffectsSettled: true}
	settlement, err := workqueue.PrepareUnstartedSettlement(task, item, result, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	cancel, err := work.operator.records.PrepareCancel(ctx, state)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.JoinChanges(settlement, cancel)
}

func (work *Work) load(
	ctx context.Context,
	item workexecution.Item,
) (keyvalue.Versioned[runnerscoped.Migration], error) {
	if item.Validate() != nil || item.Operation != workexecution.RunnerScopedMigration ||
		item.Placement.Executor != workexecution.Controller ||
		item.Input == nil {
		return keyvalue.Versioned[runnerscoped.Migration]{}, conflict("Runner migration work has no captured input")
	}
	state, exists, err := work.operator.records.Load(ctx, item.TargetID)
	if err != nil {
		return keyvalue.Versioned[runnerscoped.Migration]{}, err
	}
	if !exists || state.Record.ID != item.TaskID || state.Record.Candidate.Digest() != item.Input.SHA256 {
		return keyvalue.Versioned[runnerscoped.Migration]{}, conflict(
			"Runner migration work differs from the saved capture",
		)
	}
	return state, nil
}

func reference(task workexecution.Task, record runnerscoped.Migration) workexecution.Input {
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, Kind),
		SHA256: record.Candidate.Digest(),
	}
}

func stateGuard(state keyvalue.Versioned[runnerscoped.Migration]) workqueue.Change {
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: runnerscoped.Key(state.Record.Candidate.RunnerID), ModRevision: state.Revision},
	}}
}

func failure(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot) workexecution.Outcome {
	if task.Task.Stop != nil {
		return task.Task.Stop.Outcome
	}
	if item.Item.ForwardRevoked {
		return workexecution.Superseded
	}
	return workexecution.Failed
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
