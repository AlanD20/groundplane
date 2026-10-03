package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyDeletionAttachPlanBuilder interface {
	BuildHierarchyDeletionAttachPlan(
		context.Context,
		TaskRecord,
		hierarchydeletionattach.Input,
	) (HierarchyDeletionAttachPlan, error)
}

type HierarchyDeletionAttachPlan struct {
	PlanHash      string
	Steps         []taskjournal.TaskStepRecord
	Configuration *taskconfiguration.TaskConfiguration
	HookInputs    *taskconfiguration.BackingHookEncryptedInputs
}

func (repository *HierarchyDeletionRepository) EnableAttachPlanBuilder(
	builder HierarchyDeletionAttachPlanBuilder,
) error {
	if repository == nil || builder == nil || repository.attachPlanBuilder != nil {
		return errs.New(errs.KindInternal, "hierarchy Attach plan builder is invalid or already configured")
	}
	repository.attachPlanBuilder = builder
	return nil
}

func (repository *HierarchyDeletionRepository) ReadHierarchyDeletionAttachInput(
	ctx context.Context,
	planID string,
) (hierarchydeletionattach.Input, error) {
	return hierarchydeletionattach.ReadByPlan(ctx, repository.store, planID)
}
