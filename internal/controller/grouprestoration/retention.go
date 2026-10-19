package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireWork retains its own receipt until all attempts are collected. Source
// member artifacts can already be gone: completed Task collection grants no
// further restoration, and does not need to reconstruct any executable input.
func (service *Service) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || at.Before(*task.CollectionStartedAt) {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group restoration still has execution effects to settle",
		)
	}
	stored, err := service.groups.LoadRestorationInput(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: stored.Conditions}
	if final {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
			Key: releasegroups.RestorationInputKey(item.ID)})
	}
	return true, change, nil
}

func (service *Service) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	return releasework.PrepareAttemptRetirement(view, attempt)
}
