package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ComponentTaskPlanResolver interface {
	ResolveComponentExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
}

func (resolver *TaskPlanResolver) EnableComponentPlans(componentPlans ComponentTaskPlanResolver) error {
	if resolver == nil || componentPlans == nil {
		return errs.New(errs.KindInternal, "Component Task plan resolver is required")
	}
	resolver.componentPlans = componentPlans
	return nil
}

type componentMaterializationContentRepository interface {
	Stage(context.Context, etcd.TaskMaterializationRecord, uint64, []byte) error
	Load(context.Context, etcd.TaskMaterializationRecord, uint64) ([]byte, error)
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

func (resolver *TaskMaterializationResolver) EnableComponentMaterializationContent(
	repository componentMaterializationContentRepository,
) error {
	if resolver == nil || repository == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is required")
	}
	resolver.materializations = repository
	return nil
}

// RetainComponentFile persists only exact generated Component plain-file
// bytes. Blueprint and secret-derived sources remain in their owning stores.
func (resolver *TaskMaterializationResolver) RetainComponentFile(
	ctx context.Context,
	record etcd.TaskMaterializationRecord,
	generation uint64,
	content []byte,
) error {
	if resolver == nil || resolver.materializations == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is unavailable")
	}
	return resolver.materializations.Stage(ctx, record, generation, content)
}

func (resolver *TaskPlanResolver) retainComponentMaterialization(
	ctx context.Context,
	record etcd.TaskMaterializationRecord,
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
	record etcd.TaskMaterializationRecord,
	generation uint64,
) ([]byte, error) {
	if resolver == nil || resolver.materializations == nil {
		return nil, errs.New(errs.KindInternal, "Component materialization content repository is unavailable")
	}
	return resolver.materializations.Load(ctx, record, generation)
}
