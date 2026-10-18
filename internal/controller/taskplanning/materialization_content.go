package taskplanning

import (
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentMaterializationContentRepository interface {
	Stage(context.Context, materializationrecord.Record, uint64, []byte) error
	Load(context.Context, materializationrecord.Record, uint64) ([]byte, error)
}

func (resolver *TaskPlanResolver) EnableComponentMaterializationContent(
	repository componentMaterializationContentRepository,
) error {
	if resolver == nil || repository == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is required")
	}
	resolver.materializations = repository
	return nil
}

func (resolver *TaskPlanResolver) retainComponentMaterialization(
	ctx context.Context,
	record materializationrecord.Record,
	generation uint64,
	content []byte,
) error {
	if resolver == nil || resolver.materializations == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is unavailable")
	}
	return resolver.materializations.Stage(ctx, record, generation, content)
}

func (resolver *TaskPlanResolver) loadComponentMaterialization(
	ctx context.Context,
	record materializationrecord.Record,
	generation uint64,
) ([]byte, error) {
	if resolver == nil || resolver.materializations == nil {
		return nil, errs.New(errs.KindInternal, "Component materialization content repository is unavailable")
	}
	return resolver.materializations.Load(ctx, record, generation)
}
