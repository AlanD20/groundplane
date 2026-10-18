package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareDefinitionSettlement(ctx context.Context, work workqueue.View,
	unit blueprintunits.Unit, snapshot blueprintunits.Snapshot, at time.Time,
) (workqueue.Change, error) {
	if service == nil || ctx == nil || snapshot.ReadRevision != work.ReadRevision ||
		snapshot.HeadTaskID != work.Task.Task.ID || snapshot.EnvironmentID != work.Task.Task.Scope.EnvironmentID ||
		unit.Target.ID != work.Item.Item.TargetID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint definition work authority changed")
	}
	switch unit.Target.Kind {
	case ids.KindScript:
		return service.prepareScriptSettlement(ctx, work, unit, at)
	case ids.KindRoute:
		return service.prepareRouteSettlement(ctx, work, unit, at)
	default:
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint definition kind is unsupported")
	}
}
