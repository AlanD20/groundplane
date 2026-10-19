package tasks

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskAbortService struct {
	queue *workqueue.Repository
}

func NewAbortService(queue *workqueue.Repository) (*taskAbortService, error) {
	if queue == nil {
		return nil, errs.New(errs.KindInternal, "task abort requires execution storage")
	}
	return &taskAbortService{queue: queue}, nil
}

// AbortTask records intent, not executor termination. The stop cursor resolves
// each work item through its operation owner; claims and active attempts remain
// protected until their effects have been reconciled. Repeated cancellation is
// naturally idempotent on this Task and never creates another execution.
func (service *taskAbortService) AbortTask(
	ctx context.Context,
	taskID, _ string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "task abort context is required")
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "task id is invalid")
	}
	for attempt := 0; attempt < 3; attempt++ {
		current, _, err := service.queue.ReadTask(ctx, taskID, 0)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		task := current.Task
		if task.CollectionStartedAt != nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindTaskNotFound, "task history has expired")
		}
		if task.FinishedAt != nil {
			if task.Outcome == workexecution.Cancelled {
				return taskAbortResponse(taskID)
			}
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindTaskNotAbortable,
				"task has already finished",
			)
		}
		if err := workexecution.CancellationFailure(task); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if task.Stop != nil {
			if task.Stop.Outcome == workexecution.Cancelled {
				return taskAbortResponse(taskID)
			}
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindTaskNotAbortable,
				"task is already superseded",
			)
		}
		at := time.Now().UTC()
		if at.Before(task.UpdatedAt) {
			at = task.UpdatedAt
		}
		change, err := workqueue.PrepareStop(current, workexecution.Cancelled, at)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		result, err := service.queue.Commit(ctx, change)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if result.Succeeded {
			return taskAbortResponse(taskID)
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindStateConflict,
		"task changed repeatedly while requesting cancellation; retry the request",
	)
}

func taskAbortResponse(taskID string) (idempotencyrecord.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}, nil
}
