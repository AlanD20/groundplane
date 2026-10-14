package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskFromGenerated(task generated.Task) (apiTypes.Task, error) {
	if !task.Type.Valid() {
		return apiTypes.Task{}, errs.New(errs.KindInternal, "apiclient: Task response has unknown type")
	}
	result := apiTypes.Task{
		ID: task.Id, OperationID: task.OperationId, Type: string(task.Type), Target: task.Target,
		Status: apiTypes.TaskStatus(task.Status), WorkspaceType: apiTypes.TaskWorkspaceType(task.WorkspaceType),
		Actor: apiTypes.TaskActor(task.Actor), CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		StartedAt: task.StartedAt, FinishedAt: task.FinishedAt,
		ReconciliationRequired: task.ReconciliationRequired,
		RecoveryRetryAvailable: task.RecoveryRetryAvailable,
		DiagnosticsExpiredAt:   task.DiagnosticsExpiredAt,
		TargetName:             valueOrEmpty(task.TargetName), ResourceKind: valueOrEmpty(task.ResourceKind),
		Executor: string(task.Executor), TimeoutSeconds: task.TimeoutSeconds,
		Failure: taskFailureFromGenerated(task.Failure), Recovery: taskRecoveryFromGenerated(task.Recovery),
		ResultSummary: valueOrEmpty(task.ResultSummary),
	}
	result.Preparation = taskPreparationFromGenerated(task.Preparation)
	if retry := task.InterruptedDetachRetry; retry != nil {
		result.InterruptedDetachRetry = &apiTypes.TaskDetachRetry{AttachID: retry.AttachId, StepID: retry.StepId}
	}
	history, err := taskAttemptHistoryFromGenerated(task.AttemptHistory)
	if err != nil {
		return apiTypes.Task{}, err
	}
	result.AttemptHistory = history
	result.Executions, err = taskExecutionsFromGenerated(task.Executions)
	if err != nil {
		return apiTypes.Task{}, err
	}
	if task.RetryOf != nil {
		result.RetryOf = *task.RetryOf
	}
	if task.PlanHash != nil {
		result.PlanHash = *task.PlanHash
	}
	if task.TenantId != nil {
		result.TenantID = *task.TenantId
	}
	if task.ProjectId != nil {
		result.ProjectID = *task.ProjectId
	}
	if task.EnvironmentId != nil {
		result.EnvironmentID = *task.EnvironmentId
	}
	if fetch := task.ImageFetch; fetch != nil {
		result.ImageFetch = &apiTypes.TaskImageFetch{
			Requested: fetch.Requested, Image: fetch.Image, Platform: fetch.Platform,
			Progress: imagefetch.Progress{
				Phase: valueOrEmpty(fetch.Progress.Phase), ErrorCode: valueOrEmpty(fetch.Progress.ErrorCode),
				ErrorDetail: valueOrEmpty(fetch.Progress.ErrorDetail),
			},
		}
		if fetch.Progress.DownloadedBytes != nil {
			result.ImageFetch.Progress.DownloadedBytes = *fetch.Progress.DownloadedBytes
		}
		if fetch.Progress.TotalBytes != nil {
			result.ImageFetch.Progress.TotalBytes = *fetch.Progress.TotalBytes
		}
	}
	if task.Steps != nil {
		result.Steps = make([]apiTypes.TaskStep, len(*task.Steps))
		for index, step := range *task.Steps {
			converted, err := taskStepFromGenerated(step)
			if err != nil {
				return apiTypes.Task{}, err
			}
			result.Steps[index] = converted
		}
	}
	return result, nil
}
