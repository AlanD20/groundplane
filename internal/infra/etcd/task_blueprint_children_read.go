package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListBlueprintTaskChildren reads retained executions, not the mutable current
// unit ledger. Its cursor binds the visible parent and fixed Environment snapshot.
func (repository *TaskRepository) ListBlueprintTaskChildren(
	ctx context.Context, parent TaskRecord, request keyvalue.PageRequest,
) (keyvalue.Page[TaskRecord], error) {
	if parent.Executor != taskjournal.TaskExecutorBlueprint || ids.Validate(ids.KindTask, parent.ID) != nil ||
		ids.Validate(ids.KindEnvironment, parent.Owner.EnvironmentID) != nil {
		return keyvalue.Page[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint execution history requires its visible parent",
		)
	}
	page, err := recordquery.ListVisibleIndex(ctx, repository.store, "blueprint-executions", "parent", parent.ID,
		taskjournal.TaskEnvironmentIndexPrefix+parent.Owner.EnvironmentID+"/", taskjournal.TaskStorageKey, ids.KindTask,
		request.NewestFirst(), DecodeTaskRecord, func(task TaskRecord) string { return task.ID },
		func(task TaskRecord) bool { return task.Owner.EnvironmentID == parent.Owner.EnvironmentID },
		func(task TaskRecord) bool {
			return taskjournal.IsBlueprintChild(task.Params) &&
				task.Params[taskjournal.TaskBlueprintParentParam] == parent.ID &&
				task.Owner == parent.Owner
		})
	return repository.verifyTaskOwnerPage(ctx, page, err)
}
