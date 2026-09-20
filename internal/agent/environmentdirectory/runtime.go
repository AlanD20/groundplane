package environmentdirectory

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const environmentDirectoryHelperSchema = 1

type Helper interface {
	Execute(
		context.Context,
		*agentpb.EnvironmentDirectoryHelperRequest,
	) (*agentpb.EnvironmentDirectoryHelperResponse, error)
}

type Runtime struct {
	helper Helper
}

type StepResult struct {
	ExitCode       int32
	FailedStepID   string
	NextCursor     []byte
	MutationCount  uint32
	Complete       bool
	ResponseSHA256 []byte
}

func New(
	helper Helper,
) (*Runtime, error) {
	if helper == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: Environment directory helper is required")
	}
	return &Runtime{helper: helper}, nil
}

func (runtime *Runtime) ExecuteStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	checkpoint volumeRemovalCheckpoint,
) (StepResult, error) {
	if runtime == nil || runtime.helper == nil {
		return StepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory runtime is not configured",
		)
	}
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	if step.GetManagedVolumeDirectoryRemove() != nil {
		return runtime.executeVolumeRemoval(ctx, assignment, step, checkpoint)
	}
	if step.GetEnvironmentDirectoryCreate() == nil && step.GetEnvironmentDirectoryRemove() == nil &&
		step.GetManagedVolumeDirectoriesEnsure() == nil && step.GetManagedVolumeDirectoryRemove() == nil {
		return StepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory runtime received an unsupported step",
		)
	}
	response, err := runtime.helper.Execute(ctx, &agentpb.EnvironmentDirectoryHelperRequest{
		Schema:       environmentDirectoryHelperSchema,
		AssignmentId: assignment.AssignmentID,
		TaskId:       assignment.TaskID, OperationId: assignment.OperationID,
		Plan: assignment.Plan, StepId: step.StepId,
		TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.TimeoutSeconds),
	})
	if err != nil {
		return StepResult{}, err
	}
	if response == nil || response.Schema != environmentDirectoryHelperSchema || response.ExitCode < 0 {
		return StepResult{}, errs.New(
			errs.KindInternal,
			"agent: Environment directory helper returned an invalid response",
		)
	}
	result := StepResult{
		ExitCode: response.ExitCode, FailedStepID: response.FailedStepId,
		NextCursor: append([]byte(nil), response.NextCursor...), MutationCount: response.MutationCount,
		Complete: response.Complete, ResponseSHA256: append([]byte(nil), response.ResponseSha256...),
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
