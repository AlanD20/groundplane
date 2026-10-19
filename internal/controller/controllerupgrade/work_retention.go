package controllerupgrade

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) RetireWork(
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
			"Controller software history requires expired, settled work",
		)
	}
	capture, err := owner.inputs.Load(ctx, item, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := captureGuard(capture)
	if final {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: controllersoftware.InputKey(item.ID)},
		)
	}
	// Installed releases, the local recovery journal and the independent latest
	// update projection are not diagnostic history and are never deleted here.
	return true, change, nil
}

func (owner *Work) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.ControllerSoftware || view.Item.Item.Input == nil ||
		attempt.Input != *view.Item.Item.Input || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Controller activation history differs from captured input",
		)
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: prepared.Conditions,
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: controllersoftware.ResultKey(attempt.ID)}},
	}, nil
}
