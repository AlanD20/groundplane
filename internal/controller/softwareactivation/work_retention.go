package softwareactivation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil || item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation history requires expired settled work")
	}
	record, err := service.records.At(ctx, task.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if err := record.Record.MatchStage(item); err != nil {
		return false, workqueue.Change{}, err
	}
	if record.Record.Progress.Phase != activation.PhaseCompleted && record.Record.Progress.Phase != activation.PhaseFailed {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation has no final component outcome")
	}
	change := activationGuard(record)
	if final {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: activation.Key(task.ID)})
	}
	return true, change, nil
}

func (service *Service) PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.SoftwareStaging || view.Item.Item.Input == nil || attempt.Input != *view.Item.Item.Input || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software staging history differs from captured input")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: activation.StagingResultKey(attempt.ID)}}}, nil
}
