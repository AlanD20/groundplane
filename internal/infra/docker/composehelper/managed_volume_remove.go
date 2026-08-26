package composehelper

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeManagedRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, bool, error) {
	if remove := step.GetManagedNetworkRemove(); remove != nil {
		response, err := executeManagedNetworkRemove(ctx, taskRunner, timeoutSeconds, remove)
		return response, true, err
	}
	if remove := step.GetManagedVolumeRemove(); remove != nil {
		response, err := executeManagedVolumeRemove(ctx, taskRunner, timeoutSeconds, remove)
		return response, true, err
	}
	return nil, false, nil
}

func executeManagedVolumeRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	remove *agentpb.ManagedVolumeRemove,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"volume", "rm", remove.GetDockerName()},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if contextErr := executionCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
		return nil, errs.New(errs.KindInternal, "managed Volume remove returned an invalid exit code")
	}
	if runErr == nil && result.ExitCode == 0 {
		return completedResponse(), nil
	}
	inspection, inspectErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"volume", "inspect", remove.GetDockerName()},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if contextErr := executionCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if inspection.ExitCode == 1 && inspectErr != nil {
		return completedResponse(), nil
	}
	if inspection.ExitCode < 0 || inspection.ExitCode > math.MaxInt32 {
		return nil, errs.New(errs.KindInternal, "managed Volume inspection returned an invalid exit code")
	}
	return failedResponse(int32(max(1, result.ExitCode))), nil
}
