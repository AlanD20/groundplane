package apiclient

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func taskFailureFromGenerated(value *generated.TaskFailure) *apiTypes.TaskFailure {
	if value == nil {
		return nil
	}
	return &apiTypes.TaskFailure{
		Status: string(value.Status), StepID: valueOrEmpty(value.StepId), Code: valueOrEmpty(value.Code),
		Summary: value.Summary, ExitCode: value.ExitCode,
	}
}

func taskRecoveryFromGenerated(value *generated.TaskRecovery) *apiTypes.TaskRecovery {
	if value == nil {
		return nil
	}
	result := &apiTypes.TaskRecovery{
		State: string(value.State), Summary: value.Summary,
		CompletedSteps: value.CompletedSteps,
		TotalSteps:     value.TotalSteps, Deadline: value.Deadline,
		LastFailure:       taskFailureFromGenerated(value.LastFailure),
		AutomaticFailures: uint32(value.AutomaticFailures),
		RetryAfter:        value.RetryAfter, RetryAvailable: value.RetryAvailable,
	}
	if value.StopReason != nil {
		result.StopReason = string(*value.StopReason)
	}
	if value.StepIds != nil {
		result.StepIDs = slices.Clone(*value.StepIds)
	}
	return result
}

func taskPreparationFromGenerated(value *generated.TaskPreparation) *apiTypes.TaskPreparation {
	if value == nil {
		return nil
	}
	return &apiTypes.TaskPreparation{
		State: string(value.State), Summary: value.Summary, NetworkCount: int(value.NetworkCount),
		Deadline: value.Deadline, Failure: taskFailureFromGenerated(value.Failure),
	}
}
