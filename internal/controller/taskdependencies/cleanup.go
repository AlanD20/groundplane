package taskdependencies

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
)

// CollectAcceptedPreparation dispatches bounded typed draft cleanup using small
// immutable routing metadata. The encrypted request has already been retired.
func (preparations *Preparations) CollectAcceptedPreparation(
	ctx context.Context,
	taskID string,
	at time.Time,
) (bool, error) {
	task, revision, err := preparations.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	if task.Task.AcceptedIntent == nil || task.Task.PendingPreparation() {
		return false, nil
	}
	kind, guard, err := preparations.queue.ReadAcceptedInputKind(ctx, task, revision)
	if err != nil {
		return false, err
	}
	target, _, _ := strings.Cut(task.Task.TargetID, "_")
	if workloadimage.LocalIDValid(task.Task.TargetID) {
		target = string(ImageTarget)
	}
	key := ownerKey{action: task.Task.Action, target: ids.Kind(target), kind: kind}
	if preparations.owners[key] == nil {
		return false, errs.New(errs.KindStateConflict, "Accepted Task draft cleanup owner is unavailable")
	}
	if cleanup := preparations.cleanups[key]; cleanup != nil {
		complete, err := cleanup(ctx, task, at)
		if err != nil || !complete {
			return false, err
		}
	}
	complete, err := workqueue.PrepareAcceptedPreparationCleanupComplete(task)
	if err != nil {
		return false, err
	}
	result, err := preparations.queue.Commit(ctx, guard, complete)
	return result.Succeeded, err
}
