package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software history collection requires expired, settled work",
		)
	}
	snapshot, err := service.records.LoadWork(ctx, item, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := snapshot.Guard()
	if final {
		// The artifact projection is independent. Activation captures its own
		// outputs; neither history nor collection owns installed software.
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: preparationrecord.InputKey(item.ID)},
		)
	}
	return true, change, nil
}

func (service *Service) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.SoftwarePreparation || view.Item.Item.Input == nil ||
		attempt.Input != *view.Item.Item.Input || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software attempt history differs from its captured build",
		)
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: preparationrecord.ResultKey(attempt.ID)},
	}}, nil
}
