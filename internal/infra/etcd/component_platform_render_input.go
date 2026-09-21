package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ComponentRepository) GetPlatformComponentTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcdstore.Versioned[platformcomponents.PlatformComponentTaskRenderInput], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[platformcomponents.PlatformComponentTaskRenderInput]{}, err
	}
	if ids.Validate(ids.KindPlan, planID) != nil {
		return etcdstore.Versioned[platformcomponents.PlatformComponentTaskRenderInput]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component render-input Plan id is invalid",
		)
	}
	return recordquery.Get(
		ctx,
		repository.store,
		platformcomponents.PlatformComponentTaskRenderInputKey(planID),
		planID,
		errs.KindTaskNotFound,
		platformcomponents.DecodePlatformComponentTaskRenderInput,
		func(record platformcomponents.PlatformComponentTaskRenderInput) string { return record.PlanID },
	)
}
