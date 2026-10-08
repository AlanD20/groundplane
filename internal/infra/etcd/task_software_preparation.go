package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const SoftwarePreparationTarget = "software_preparation"

func validateSoftwarePreparationTask(record TaskRecord) error {
	if record.Type != taskjournal.TaskPrepare &&
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftwarePreparation {
		return nil
	}
	if record.Type != taskjournal.TaskPrepare || record.Executor != taskjournal.TaskExecutorController ||
		record.Owner != taskjournal.PlatformTaskOwner() || record.Actor != taskjournal.TaskActorOperator ||
		record.Target != SoftwarePreparationTarget || len(record.Params) != 2 ||
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftwarePreparation ||
		record.TimeoutSeconds != preparation.TaskTimeoutSeconds || record.RenderGeneration != 1 ||
		record.RetryOf != "" || len(record.Steps) != 0 || len(record.Materializations) != 0 ||
		record.Configuration != nil || record.EntryRuntime != nil || record.Result != nil ||
		len(record.ComponentActionStepIDs) != 0 || len(record.ManagedComponentTeardownSources) != 0 {
		return errs.New(errs.KindValidationFailed, "software preparation Task authority is invalid")
	}
	input, err := preparation.DecodeInput(
		record.Params[taskjournal.TaskSoftwarePreparationInputParam], record.PlanHash,
	)
	if err != nil {
		return err
	}
	if input.OperationID != record.OperationID || ids.Validate(ids.KindPlan, record.PlanID) != nil {
		return errs.New(errs.KindValidationFailed, "software preparation Task identity differs from its input")
	}
	return nil
}
