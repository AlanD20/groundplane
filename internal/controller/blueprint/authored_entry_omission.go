package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredEntryOmissionRepository interface {
	PrepareBlueprintEntryOmission(context.Context, workqueue.View, blueprintunits.Unit, time.Time) (workqueue.Change, bool, error)
}

func (service *Service) prepareEntryOmission(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit,
) (workqueue.Change, error) {
	if service == nil || ctx == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Entry omission settler is not configured")
	}
	settler, ok := service.repository.(authoredEntryOmissionRepository)
	if !ok {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Entry omission repository is not configured")
	}
	change, ready, err := settler.PrepareBlueprintEntryOmission(ctx, work, unit, service.now().UTC())
	if err != nil {
		return change, err
	}
	if !ready {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry removal requires managed-file cleanup before its definition can be retired")
	}
	return change, nil
}
