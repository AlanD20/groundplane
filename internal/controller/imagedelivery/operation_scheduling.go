package imagedelivery

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.Image || item.Placement.Executor != workexecution.Controller ||
		item.Input == nil || request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"image work requires its captured forward execution",
		)
	}
	loaded, err := service.operations.Load(ctx, item.TaskID)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	record := loaded.Record
	input, err := record.Input.Reference()
	if err != nil || record.Phase != imageoperations.Ready || record.Input.WorkItemID != item.ID ||
		input != *item.Input || record.Input.Target() != item.TargetID || record.Input.Placement != item.Placement {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "image work differs from its captured input")
	}
	if !item.Deadline.After(at) {
		change, err := imageoperations.PrepareUnstarted(loaded, request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: imageoperations.Key(item.TaskID), ModRevision: loaded.Revision}},
		}}, nil
}

func (service *Service) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "image Task has no stop request")
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Phase != workexecution.Settled {
		loaded, err := service.operations.Load(ctx, task.Task.ID)
		if err != nil {
			return workqueue.Change{}, err
		}
		return imageoperations.PrepareUnstarted(loaded, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}
