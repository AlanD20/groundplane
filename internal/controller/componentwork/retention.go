package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DefinitionRetention interface {
	RetireWork(context.Context, workqueue.View, time.Time, bool) (bool, workqueue.Change, error)
}

type PreparationRetirement interface {
	DiscardProxyPreparation(context.Context, workqueue.View) (bool, error)
}

// Retention routes the two native Component operations: Controller-only
// definition settlement and host execution. Neither uses the retired Task engine.
type Retention struct {
	inputs       *componentrecord.InputRepository
	definitions  DefinitionRetention
	preparations PreparationRetirement
}

func NewRetention(inputs *componentrecord.InputRepository, definitions DefinitionRetention, preparations PreparationRetirement) (*Retention, error) {
	if inputs == nil || definitions == nil || preparations == nil {
		return nil, errs.New(errs.KindInternal, "Component retention requires execution, definition and source owners")
	}
	return &Retention{inputs: inputs, definitions: definitions, preparations: preparations}, nil
}

func (owner *Retention) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if view.Item.Item.Operation != workexecution.Component {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Component retention cannot collect another operation")
	}
	if view.Item.Item.Placement.Executor == workexecution.Controller {
		if view.Task.Task.Action != workexecution.ActionApply {
			return owner.inputs.RetireRouteDefinition(ctx, view, at, final)
		}
		return owner.definitions.RetireWork(ctx, view, at, final)
	}
	ready, change, err := owner.inputs.PrepareRetention(ctx, view, at, final)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	if view.Task.Task.Action == workexecution.ActionApply {
		done, err := owner.preparations.DiscardProxyPreparation(ctx, view)
		if err != nil || !done {
			return false, workqueue.Change{}, err
		}
	}
	return true, change, nil
}

func (owner *Retention) PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return componentrecord.PrepareAttemptRetirement(view, snapshot)
}
