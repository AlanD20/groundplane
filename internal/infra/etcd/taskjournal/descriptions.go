package taskjournal

import (
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/taskdescription"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureStepDescriptions records public-safe presentation from the sealed plan
// before publication. It never changes execution identity or authority.
func CaptureStepDescriptions(records []TaskStepRecord, plan *agentpb.ExecutionPlan) []TaskStepRecord {
	result := CloneTaskSteps(records)
	descriptions := make(map[string]taskdescription.Description, len(plan.GetSteps()))
	healthchecks := make(map[string][]taskdescription.ConfiguredHealthcheck)
	for _, step := range plan.GetSteps() {
		if step != nil {
			descriptions[step.StepId] = taskdescription.Describe(step)
			healthchecks[step.StepId] = taskdescription.CaptureHealthchecks(plan, step)
		}
	}
	for index := range result {
		if description, ok := descriptions[result[index].ID]; ok {
			result[index].Action = description.Action
			result[index].Description = description.Description
			result[index].Target = description.Target
			result[index].TimeoutSeconds = description.TimeoutSeconds
			result[index].Healthchecks = healthchecks[result[index].ID]
		}
	}
	return result
}

func validateStepDescription(step TaskStepRecord) error {
	if !validPresentationText(step.Action, 160) || !validPresentationText(step.Description, 600) ||
		!validPresentationText(step.Target, 160) || (step.Action == "" &&
		(step.Description != "" || step.Target != "" || step.TimeoutSeconds != 0 || len(step.Healthchecks) != 0)) {
		return errs.New(errs.KindValidationFailed, "task step description is invalid")
	}
	return taskdescription.ValidateHealthchecks(step.Healthchecks)
}

func validPresentationText(value string, limit int) bool {
	if len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func ValidateTargetName(value string) error {
	if !validPresentationText(value, 160) {
		return errs.New(errs.KindValidationFailed, "task target name is invalid")
	}
	return nil
}
