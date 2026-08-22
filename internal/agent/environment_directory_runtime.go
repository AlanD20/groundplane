package agent

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const environmentDirectoryHelperSchema = 1

type EnvironmentDirectoryHelper interface {
	Execute(
		context.Context,
		*agentpb.EnvironmentDirectoryHelperRequest,
	) (*agentpb.EnvironmentDirectoryHelperResponse, error)
}

type EnvironmentDirectoryRuntime struct {
	helper EnvironmentDirectoryHelper
}

type environmentDirectoryStepResult struct {
	ExitCode     int32
	FailedStepID string
}

func NewEnvironmentDirectoryRuntime(
	helper EnvironmentDirectoryHelper,
) (*EnvironmentDirectoryRuntime, error) {
	if helper == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: Environment directory helper is required")
	}
	return &EnvironmentDirectoryRuntime{helper: helper}, nil
}

func (runtime *EnvironmentDirectoryRuntime) executeStep(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (environmentDirectoryStepResult, error) {
	if runtime == nil || runtime.helper == nil {
		return environmentDirectoryStepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory runtime is not configured",
		)
	}
	if err := ctx.Err(); err != nil {
		return environmentDirectoryStepResult{}, err
	}
	if step.GetEnvironmentDirectoryCreate() == nil && step.GetEnvironmentDirectoryRemove() == nil &&
		step.GetManagedVolumeDirectoriesEnsure() == nil {
		return environmentDirectoryStepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory runtime received an unsupported step",
		)
	}
	response, err := runtime.helper.Execute(ctx, &agentpb.EnvironmentDirectoryHelperRequest{
		Schema: environmentDirectoryHelperSchema,
		TaskId: assignment.TaskID, OperationId: assignment.OperationID,
		Plan: assignment.Plan, StepId: step.StepId,
		TimeoutSeconds: remainingSeconds(ctx, step.TimeoutSeconds),
	})
	if err != nil {
		return environmentDirectoryStepResult{}, err
	}
	if response == nil || response.Schema != environmentDirectoryHelperSchema || response.ExitCode < 0 {
		return environmentDirectoryStepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory helper returned an invalid response",
		)
	}
	result := environmentDirectoryStepResult{
		ExitCode: response.ExitCode, FailedStepID: response.FailedStepId,
	}
	if response.ExitCode == 0 {
		if response.FailedStepId != "" {
			return result, errs.New(
				errs.KindInternal,
				"agent: Environment directory helper returned an inconsistent success response",
			)
		}
		return result, nil
	}
	if response.FailedStepId != step.StepId {
		return result, errs.New(
			errs.KindInternal,
			"agent: Environment directory helper returned an inconsistent failure response",
		)
	}
	return result, errs.New(errs.KindRequestFailed, "agent: Environment directory helper failed")
}
