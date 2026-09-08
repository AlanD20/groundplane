package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ComponentActionStepIDs projects mutation authority from a fully sealed plan.
func ComponentActionStepIDs(plan *agentpb.ExecutionPlan) ([]string, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	return componentActionStepIDs(sealed), nil
}

func componentActionStepIDs(plan *agentpb.ExecutionPlan) []string {
	var result []string
	for _, step := range plan.GetSteps() {
		if step.GetComponentApply() != nil {
			result = append(result, step.GetStepId())
		}
	}
	return result
}

// ValidateComponentActionStepIDs checks the bounded ordered durable projection.
func ValidateComponentActionStepIDs(stepIDs []string) error {
	if len(stepIDs) > MaximumPlanBytes/len("step_01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		return errs.New(errs.KindValidationFailed, "Component action step bound exceeded")
	}
	seen := make(map[string]bool, len(stepIDs))
	for _, id := range stepIDs {
		if ids.Validate(ids.KindStep, id) != nil || seen[id] {
			return errs.New(errs.KindValidationFailed, "Component action step identity is invalid or duplicate")
		}
		seen[id] = true
	}
	return nil
}
