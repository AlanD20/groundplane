package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintEntryConfiguration captures the acknowledged predecessor
// before an authored omission plan is sealed.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintEntryConfiguration(
	ctx context.Context, child TaskRecord, revision int64,
) (TaskRecord, error) {
	if repository == nil || repository.HierarchyRepository == nil ||
		!authoredBlueprintEntryRemovalTask(child) ||
		child.Params[taskjournal.TaskBlueprintParentParam] != child.Params[blueprints.EnvironmentDesiredRevisionParam] ||
		ids.Validate(ids.KindEnvEntry, child.Target) != nil {
		return TaskRecord{}, errs.New(
			errs.KindValidationFailed, "Blueprint Entry configuration identity is invalid",
		)
	}
	return prepareRuntimeConfigurationTask(ctx, repository.store, child, revision)
}
