package imagedelivery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareRemoval(
	ctx context.Context,
	imageID, key string,
	now time.Time,
) (etcd.TaskRecord, []byte, error) {
	if err := service.checkRemoval(ctx, imageID, ""); err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	hash, err := imagefetch.RemovalHash(imageID)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		IdempotencyKey: key, Type: taskjournal.TaskRemove, Target: imageID, PlanHash: hash, RenderGeneration: 1,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam:     taskjournal.TaskResourceImage,
			taskjournal.TaskImageRemoveInputParam: imageID,
		},
		TimeoutSeconds: imagefetch.TimeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := etcd.ValidateTaskRecord(task); err != nil {
		return task, nil, err
	}
	raw, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	return task, raw, err
}

func (service *Service) checkRemoval(ctx context.Context, imageID, operationID string) error {
	images, err := service.registry.List(ctx)
	if err != nil {
		return err
	}
	for _, image := range images {
		if image.ID != imageID {
			continue
		}
		retention, err := service.retainedImages(ctx, operationID)
		if err != nil {
			return err
		}
		if reason := removalReason(image, retention); reason != "" {
			return errs.New(errs.KindResourceInUse, reason)
		}
		return nil
	}
	// Absence is already the requested outcome. An identical content-addressed
	// ID cannot select some later, different image on retry.
	return nil
}

func (service *Service) executeRemoval(ctx context.Context, task etcd.TaskRecord) (result error) {
	if err := acquireRemoval(ctx, service.store, task.OperationID); err != nil {
		return err
	}
	release := true
	defer func() {
		if !release {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		result = errors.Join(result, releaseRemoval(cleanup, service.store, task.OperationID))
	}()
	if err := service.checkRemoval(ctx, task.Target, task.OperationID); err != nil {
		return err
	}
	result = service.registry.Remove(ctx, task.Target)
	// An interrupted Docker response is not proof that deletion stopped. Keep
	// the durable fence until this same removal operation is retried/settled.
	if result != nil && !errors.Is(result, errs.New(errs.KindResourceInUse, "")) {
		release = false
	}
	return result
}
