package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// CleanupAcceptedPreparation retires only custody after successful promotion,
// or abandons inert captures of a terminal uncompiled Task. Shared cleanup
// scheduling survives encrypted request expiry.
func (service *zoneDeletionService) CleanupAcceptedPreparation(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	at time.Time,
) (bool, error) {
	claim, done, err := service.repository.CleanupNativePreparationDraft(ctx, task, at)
	if err != nil || !done {
		return done, err
	}
	if claim.TaskID == "" {
		return true, nil
	}
	err = service.repository.AbandonEnvironmentBlueprintStage(ctx, claim)
	if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
		err = nil
	}
	if err != nil {
		return false, err
	}
	return service.repository.FinishNativePreparationDraft(ctx, task.Task.ID, false)
}
