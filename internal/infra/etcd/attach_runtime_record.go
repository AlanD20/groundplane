package etcd

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareAttachRuntime reads acknowledged sources in one snapshot. Publication
// compares these exact revisions together with the captured Environment epoch.
func (repository *AttachRepository) PrepareAttachRuntime(
	ctx context.Context, plan *agentpb.ExecutionPlan,
) (serviceruntimerecord.AttachPreparation, error) {
	var empty serviceruntimerecord.AttachPreparation
	if err := validateContext(ctx); err != nil {
		return empty, err
	}
	sealed, err := executionplan.Validate(plan)
	if err != nil {
		return empty, err
	}
	if (sealed.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
		sealed.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DETACH) || len(sealed.GetArtifacts()) != 1 {
		return empty, errs.New(errs.KindValidationFailed, "Attach runtime preparation requires a standalone plan")
	}
	prepared := serviceruntimerecord.AttachPreparation{
		EnvironmentID: sealed.GetArtifacts()[0].GetOwnerId(), PlanID: sealed.GetPlanId(),
		PlanHash: hex.EncodeToString(sealed.GetPlanHash()), RenderGeneration: sealed.GetRenderGeneration(),
	}
	var selected []string
	for _, step := range sealed.GetSteps() {
		if step.GetComposeApply() == nil {
			continue
		}
		if prepared.StepID != "" {
			return empty, errs.New(errs.KindValidationFailed, "Attach runtime has multiple Compose steps")
		}
		prepared.StepID = step.GetStepId()
		if _, _, err := executionplan.AttachMutationServices(sealed, prepared.StepID); err != nil {
			return empty, err
		}
		selected = step.GetComposeApply().GetServiceIds()
	}
	if len(selected) > 1 {
		return empty, errs.New(errs.KindValidationFailed, "Attach runtime has multiple consumers")
	}
	var previous []executionplan.CandidateRuntime
	var sourceRevision int64
	if len(selected) == 1 {
		result, readErr := repository.store.GetMany(
			ctx,
			GetManyRequest{Keys: []string{serviceruntimerecord.Key(selected[0])}},
		)
		if readErr != nil {
			return empty, readErr
		}
		if result == nil || result.ReadRevision <= 0 || len(result.Values) != 1 || result.Values[0] == nil {
			return empty, errs.New(errs.KindStateConflict, "running Attach consumer has no acknowledged runtime")
		}
		defer clearKeyValues(result.Values)
		record, decodeErr := decodeAcknowledgedServiceRuntime(
			result.Values[0].Value,
			prepared.EnvironmentID,
			selected[0],
		)
		if decodeErr != nil {
			return empty, decodeErr
		}
		previous = []executionplan.CandidateRuntime{record.Runtime}
		sourceRevision = result.Values[0].ModRevision
	}
	runtimes, err := executionplan.PrepareAttachRuntimes(sealed, previous)
	if err != nil {
		return empty, err
	}
	for _, runtime := range runtimes {
		prepared.Updates = append(
			prepared.Updates,
			serviceruntimerecord.AttachUpdate{PreviousRevision: sourceRevision, Runtime: runtime},
		)
	}
	return prepared, serviceruntimerecord.ValidateAttachPreparation(prepared)
}

func decodeAcknowledgedServiceRuntime(
	value []byte,
	environmentID, serviceID string,
) (serviceruntimerecord.Record, error) {
	record, err := decodeReleaseRecord[serviceruntimerecord.Record](value, "service-acknowledged-runtime")
	if err != nil {
		return serviceruntimerecord.Record{}, err
	}
	if record.EnvironmentID != environmentID || record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(record) != nil {
		return serviceruntimerecord.Record{}, errs.New(errs.KindInternal, "acknowledged Service runtime is corrupt")
	}
	return record, nil
}

func validateAttachRuntimePreparation(input AttachTaskRenderInput, task TaskRecord) error {
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

func attachRuntimeSourceConditions(input AttachTaskRenderInput) []Condition {
	conditions := make([]Condition, len(input.RuntimePreparation.Updates))
	for index, update := range input.RuntimePreparation.Updates {
		conditions[index] = Condition{
			Key:         serviceruntimerecord.Key(update.Runtime.ServiceID),
			ModRevision: update.PreviousRevision,
		}
	}
	return conditions
}
