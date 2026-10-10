package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintComponentConfiguration retains candidate file sources and
// the acknowledged predecessor before the immutable execution recipe is sealed.
// Secret-pin activation and the predecessor compare belong to child publication.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintComponentConfiguration(ctx context.Context, child TaskRecord, revision int64) (TaskRecord, error) {
	if repository == nil || repository.HierarchyRepository == nil ||
		child.Executor != taskjournal.TaskExecutorAgent || child.Actor != taskjournal.TaskActorSystem ||
		child.Status != taskjournal.TaskStatusPending ||
		ids.Validate(ids.KindComponent, child.Params[taskjournal.TaskBlueprintComponentUnitParam]) != nil ||
		ids.Validate(ids.KindTask, child.Params[taskjournal.TaskBlueprintParentParam]) != nil ||
		child.Params[taskjournal.TaskBlueprintParentParam] != child.Params[blueprints.EnvironmentDesiredRevisionParam] {
		return TaskRecord{}, errs.New(errs.KindValidationFailed, "Blueprint Component configuration identity is invalid")
	}
	return prepareRuntimeConfigurationTask(ctx, repository.store, child, revision)
}
