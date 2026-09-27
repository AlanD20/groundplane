package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Image fetch is a platform-native operation with no application mutation.
// Retry clones this same pinned input; execution never resolves the tag again.
func validateImageFetchTask(record TaskRecord) error {
	if record.Type != taskjournal.TaskFetch &&
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceImage {
		return nil
	}
	if record.Type != taskjournal.TaskFetch || record.Executor != taskjournal.TaskExecutorController ||
		record.Owner != taskjournal.PlatformTaskOwner() || record.Actor != taskjournal.TaskActorOperator ||
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceImage || len(record.Params) != 2 ||
		record.TimeoutSeconds != imagefetch.TimeoutSeconds || record.RenderGeneration != 1 ||
		len(record.Steps) != 0 || len(record.Materializations) != 0 || record.Configuration != nil ||
		record.EntryRuntime != nil || record.Result != nil || len(record.ComponentActionStepIDs) != 0 ||
		len(record.ManagedComponentTeardownSources) != 0 {
		return errs.New(errs.KindValidationFailed, "image fetch Task authority is invalid")
	}
	plan, err := imagefetch.Decode(record.Params[taskjournal.TaskImageFetchInputParam], record.PlanHash)
	if err != nil {
		return err
	}
	if record.Target != plan.Reference() {
		return errs.New(errs.KindValidationFailed, "image fetch Task target differs from its pinned input")
	}
	return nil
}
