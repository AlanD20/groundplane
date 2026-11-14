package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareEntryOmission(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit,
) (workqueue.Change, error) {
	if service == nil || ctx == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Entry omission settler is not configured")
	}
	change, ready, err := service.repository.PrepareBlueprintEntryOmission(ctx, work, unit, service.now().UTC())
	if err != nil {
		return change, err
	}
	if !ready {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Entry removal requires managed-file cleanup before its definition can be retired",
		)
	}
	return change, nil
}
