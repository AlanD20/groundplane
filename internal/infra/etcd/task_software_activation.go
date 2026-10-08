package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const SoftwareActivationTarget = "software"

func validateSoftwareActivationTask(record TaskRecord) error {
	if record.Type != taskjournal.TaskApply &&
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftware {
		return nil
	}
	if record.Type != taskjournal.TaskApply || record.Executor != taskjournal.TaskExecutorSoftware ||
		record.Owner != taskjournal.PlatformTaskOwner() || record.Actor != taskjournal.TaskActorOperator ||
		record.Target != SoftwareActivationTarget || len(record.Params) != 2 ||
		record.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftware ||
		record.TimeoutSeconds != activationrecord.TaskTimeoutSeconds || record.RenderGeneration != 1 ||
		record.RetryOf != "" || len(record.Steps) != 0 || len(record.Materializations) != 0 ||
		record.Configuration != nil || record.EntryRuntime != nil || record.Result != nil ||
		len(record.ComponentActionStepIDs) != 0 || len(record.ManagedComponentTeardownSources) != 0 {
		return errs.New(errs.KindValidationFailed, "software activation Task authority is invalid")
	}
	input, err := activationrecord.DecodeInput(
		record.Params[taskjournal.TaskSoftwareActivationInputParam], record.PlanHash,
	)
	if err != nil {
		return err
	}
	if input.OperationID != record.OperationID || ids.Validate(ids.KindPlan, record.PlanID) != nil {
		return errs.New(errs.KindValidationFailed, "software activation Task identity differs from its input")
	}
	return nil
}
