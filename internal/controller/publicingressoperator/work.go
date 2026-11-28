package publicingressoperator

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const activationTimeout = 10 * time.Minute

// Work owns one native Agent operation for a reviewed ingress publication.
type Work struct {
	operator *Operator
	queue    *workqueue.Repository
}

func NewWork(operator *Operator, queue *workqueue.Repository) (*Work, error) {
	if operator == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "public ingress work requires native Task storage and authority")
	}
	return &Work{operator: operator, queue: queue}, nil
}

func (work *Work) AcceptedPreparation() taskdependencies.Registration {
	registration := acceptedaction.Registration(
		work.operator.submitter, ApplyKind, TaskAction, ids.KindHost, work.prepareAccepted,
	)
	registration.Cleanup = work.cleanupPreparation
	return registration
}

func (work *Work) prepareAccepted(
	ctx context.Context, snapshot workqueue.TaskSnapshot, _ int64, at time.Time, input AcceptedApply,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	if task.Action != TaskAction || task.TargetID != input.HostID {
		return taskdependencies.Prepared{}, conflict("public ingress Task does not match its captured Host")
	}
	state, err := work.operator.ingress.Read(ctx, input.HostID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if state.Pending == nil || state.Pending.Revision != input.PublicationRevision ||
		state.Pending.Digest != input.Digest || state.Pending.TaskID != task.ID || state.Pending.Authorized {
		return taskdependencies.Prepared{}, conflict("public ingress Task no longer has a matching pending publication")
	}
	plan := planFrom(state)
	ref, err := planReference(plan, task.CreatedAt)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared := workexecution.CloneTask(task)
	prepared.Intent, prepared.ExpectedItems, prepared.UpdatedAt = ref, 1, at
	item := workexecution.Item{
		ID:     ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, ApplyKind),
		TaskID: task.ID, Operation: workexecution.PublicIngress, TargetID: input.HostID, TargetName: task.TargetName,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID},
		Claims:    []workexecution.Claim{{HostID: input.HostID, ScopeID: input.HostID}},
		Input:     &ref, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: acceptedaction.ExecutionDeadline(prepared, at, activationTimeout),
	}
	return taskdependencies.Prepared{
		Task: prepared, Graph: []workexecution.Item{item}, Authority: stateGuard(state),
	}, nil
}

func (work *Work) cleanupPreparation(
	ctx context.Context, task workqueue.TaskSnapshot, _ time.Time,
) (bool, error) {
	if task.Task.ExpectedItems != 0 {
		return true, nil
	}
	if task.Task.FinishedAt == nil || task.Task.Outcome == workexecution.Succeeded {
		return false, conflict("public ingress Task is still preparing")
	}
	change, err := work.operator.ingress.PrepareCancel(ctx, task.Task.TargetID, task.Task.ID)
	if err != nil || len(change.Mutations) == 0 {
		return err == nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := work.queue.Commit(ctx, change, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
	}})
	return result.Succeeded, err
}

func planFrom(state storage.State) ingressexecution.Plan {
	pending := state.Pending
	plan := ingressexecution.Plan{
		HostID: state.HostID, TaskID: pending.TaskID, Revision: pending.Revision, Digest: pending.Digest,
		Addresses: slices.Clone(pending.Desired.Addresses), Configuration: slices.Clone(pending.Configuration),
	}
	if state.Active != nil {
		receipt := ingressexecution.Receipt(*state.Active)
		plan.Predecessor = &receipt
	}
	return plan
}

func planReference(plan ingressexecution.Plan, created time.Time) (workexecution.Input, error) {
	digest, err := plan.SHA256()
	if err != nil {
		return workexecution.Input{}, err
	}
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, created, plan.TaskID, ApplyKind),
		SHA256: digest,
	}, nil
}

func stateGuard(state storage.State) workqueue.Change {
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: storage.Key(state.HostID), ModRevision: state.Revision}},
	}
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
