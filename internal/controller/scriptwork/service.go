// Package scriptwork owns standalone Script execution in the native work queue.
// Script authoring and submission remain in the scripts package.
package scriptwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Service struct {
	queue      *workqueue.Repository
	repository *etcd.ScriptRepository
	artifacts  *taskplanning.ScriptArtifactService
}

func New(
	queue *workqueue.Repository,
	repository *etcd.ScriptRepository,
	artifacts *taskplanning.ScriptArtifactService,
) (*Service, error) {
	if queue == nil || repository == nil || artifacts == nil {
		return nil, errs.New(errs.KindInternal, "Script execution requires queue and captured input storage")
	}
	return &Service{queue: queue, repository: repository, artifacts: artifacts}, nil
}

func (service *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	if err := ctx.Err(); err != nil {
		return workdispatch.Decision{}, err
	}
	item, task := request.Item.Item, request.Task.Task
	if request.Direction == workexecution.Recovery {
		return service.admitRecovery(ctx, request, at)
	}
	if request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.Script ||
		item.Placement.Executor != workexecution.Agent || request.Executor.Record.Identity.HostID != item.Placement.HostID ||
		at.IsZero() {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Script cannot obtain this assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	input, err := service.repository.LoadRetainedScriptWork(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	defer input.Execution.Clear()
	if task.Stop != nil || item.ForwardRevoked || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		change, err := scriptexecutions.PrepareUnstartedStop(request.Task, request.Item, input.Execution, at)
		if err == nil {
			change.Conditions = append(change.Conditions, input.Conditions...)
		}
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	execution := input.Execution
	if execution.State != scriptexecutions.ScriptExecutionNotStarted || execution.StartAuthorized ||
		execution.AttemptID != "" || execution.Executor != nil || execution.ReconciliationRequired {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Script has prior execution evidence requiring settlement",
		)
	}
	deadline := item.Deadline
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline,
		Change: workqueue.Change{Conditions: input.Conditions}}, nil
}

func (service *Service) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script has no stop request")
	}
	view, err := service.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script changed before queued cancellation")
	}
	input, err := service.repository.LoadRetainedScriptWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Execution.Clear()
	change, err := scriptexecutions.PrepareUnstartedStop(task, item, input.Execution, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, input.Conditions...)
	return change, nil
}

func (service *Service) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		if _, err := service.repository.LoadScriptRecoveryInput(ctx, view); err != nil {
			return workqueue.Change{}, err
		}
		sources, err := service.repository.LoadScriptRecoverySources(ctx, view.Work)
		return workqueue.Change{Conditions: sources.Conditions}, err
	}
	return service.repository.PrepareScriptWorkStart(ctx, view)
}
