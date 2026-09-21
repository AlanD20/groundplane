package platformcomponents

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Persistence) GetPlatformComponentTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcdstore.Versioned[PlatformComponentTaskRenderInput], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[PlatformComponentTaskRenderInput]{}, err
	}
	if ids.Validate(ids.KindPlan, planID) != nil {
		return etcdstore.Versioned[PlatformComponentTaskRenderInput]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component render-input Plan id is invalid",
		)
	}
	return recordquery.Get(
		ctx,
		repository.store,
		PlatformComponentTaskRenderInputKey(planID),
		planID,
		errs.KindTaskNotFound,
		DecodePlatformComponentTaskRenderInput,
		func(record PlatformComponentTaskRenderInput) string { return record.PlanID },
	)
}
