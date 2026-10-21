package blueprintcoordinator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InputPreparation closes obsolete captured work before invoking its typed
// planner. Every Apply input owner uses this boundary; it never publishes new
// executable input or pretends that assigned work has no effects.
type InputPreparation struct {
	queue *workqueue.Repository
	units *blueprintunits.Repository
	owner workdispatch.InputOwner
}

func NewInputPreparations(queue *workqueue.Repository, units *blueprintunits.Repository,
	registrations []workdispatch.InputRegistration,
) ([]workdispatch.InputRegistration, error) {
	if queue == nil || units == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "Blueprint input preparation requires ownership and typed planners")
	}
	result := make([]workdispatch.InputRegistration, 0, len(registrations))
	for _, registration := range registrations {
		if registration.Action != workexecution.ActionApply || registration.Owner == nil ||
			!workexecution.ValidOperation(registration.Operation) {
			return nil, errs.New(errs.KindInternal, "Blueprint input planner registration is invalid")
		}
		registration.Owner = &InputPreparation{queue: queue, units: units, owner: registration.Owner}
		result = append(result, registration)
	}
	return result, nil
}

func (owner *InputPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	current, guards, err := owner.units.ReadWorkIntentMatch(ctx, view)
	if err != nil {
		return err
	}
	if current {
		// The planner still compares its own fixed snapshot when publishing.
		return owner.owner.Prepare(ctx, taskID, workID)
	}
	change, err := workqueue.PrepareInputSupersession(view, guards, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint input ownership changed; reload the captured work")
	}
	return nil
}
