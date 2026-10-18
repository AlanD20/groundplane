package etcd

import (
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeAcknowledgedServiceRuntime(
	value []byte,
	environmentID, serviceID string,
) (serviceruntimerecord.Record, error) {
	record, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](value, "service-acknowledged-runtime")
	if err != nil {
		return serviceruntimerecord.Record{}, err
	}
	if record.EnvironmentID != environmentID || record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(record) != nil {
		return serviceruntimerecord.Record{}, errs.New(errs.KindInternal, "acknowledged Service runtime is corrupt")
	}
	return record, nil
}

func validateAttachRuntimePreparation(input attachrender.AttachTaskRenderInput, task TaskRecord) error {
	prepared := input.RuntimePreparation
	if prepared == nil || prepared.EnvironmentID != input.EnvironmentID || prepared.PlanID != task.PlanID ||
		prepared.PlanHash != task.PlanHash || prepared.RenderGeneration != input.RenderGeneration {
		return errs.New(errs.KindValidationFailed, "Attach runtime preparation does not match its Task")
	}
	if err := serviceruntimerecord.ValidateAttachPreparation(*prepared); err != nil {
		return err
	}
	stepFound := false
	for _, step := range task.Steps {
		if step.ID == prepared.StepID {
			stepFound = true
		}
	}
	if !stepFound {
		return errs.New(errs.KindValidationFailed, "Attach runtime preparation step is absent")
	}
	var selected []string
	for _, serviceID := range input.ConsumerServiceIDs {
		if slices.Contains(input.RunningServiceIDs, serviceID) {
			selected = append(selected, serviceID)
		}
	}
	if len(selected) != len(prepared.Updates) {
		return errs.New(errs.KindValidationFailed, "Attach runtime preparation selection differs")
	}
	for index, update := range prepared.Updates {
		if update.Runtime.ServiceID != selected[index] {
			return errs.New(errs.KindValidationFailed, "Attach runtime preparation consumer differs")
		}
	}
	return nil
}

func attachRuntimeSourceConditions(input attachrender.AttachTaskRenderInput) []etcdstore.Condition {
	conditions := make([]etcdstore.Condition, len(input.RuntimePreparation.Updates))
	for index, update := range input.RuntimePreparation.Updates {
		conditions[index] = etcdstore.Condition{
			Key:         serviceruntimerecord.Key(update.Runtime.ServiceID),
			ModRevision: update.PreviousRevision,
		}
	}
	return conditions
}
