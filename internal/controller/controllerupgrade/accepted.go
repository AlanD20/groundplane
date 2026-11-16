package controllerupgrade

import (
	"context"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type controllerUpdateRequest struct {
	Release upgrade.Digest `json:"release"`
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.submitter, "controller-update",
		workexecution.ActionUpdate, ids.KindComponent, service.prepareAcceptedUpdate)}
}

func (service *Service) prepareAcceptedUpdate(ctx context.Context, accepted workqueue.TaskSnapshot, _ int64,
	at time.Time, request controllerUpdateRequest,
) (taskdependencies.Prepared, error) {
	input, err := service.freeze(ctx, request.Release)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task := workexecution.CloneTask(accepted.Task)
	capture := controllersoftware.Capture{TaskID: task.ID, Input: input, CreatedAt: at,
		WorkItemID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "controller-update"),
		PlanID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "controller-update")}
	captured, item, err := capture.Work()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, 1, at
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, upgrade.TaskTimeoutSeconds*time.Second)
	authority, err := service.tasks.PrepareControllerSoftwareAuthority(ctx, capture)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
