package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskMaterializations       = 512
	generatedEnvironmentFormatVersion = 1
)

func validateTaskMaterializationReferences(
	references []taskmaterialization.Record,
	steps []taskjournal.TaskStepRecord,
	environmentID string,
	hasEnvironment bool,
	renderGeneration uint64,
) error {
	if len(references) == 0 {
		return nil
	}
	if !hasEnvironment || len(references) > maximumTaskMaterializations || len(references) > len(steps) {
		return errs.New(errs.KindValidationFailed, "task materialization references are invalid")
	}
	availableSteps := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		availableSteps[step.ID] = struct{}{}
	}
	previousStepID := ""
	materializationIDs := make(map[string]struct{}, len(references))
	for _, reference := range references {
		if reference.StepID <= previousStepID || reference.EnvironmentID != environmentID {
			return errs.New(errs.KindValidationFailed, "task materialization references are not uniquely sorted")
		}
		if _, exists := availableSteps[reference.StepID]; !exists {
			return errs.New(errs.KindValidationFailed, "task materialization reference step is unknown")
		}
		if _, duplicate := materializationIDs[reference.MaterializationID]; duplicate {
			return errs.New(errs.KindValidationFailed, "task materialization id is duplicated")
		}
		if err := taskmaterialization.ValidateRecord(reference, renderGeneration); err != nil {
			return err
		}
		materializationIDs[reference.MaterializationID] = struct{}{}
		previousStepID = reference.StepID
	}
	return nil
}
