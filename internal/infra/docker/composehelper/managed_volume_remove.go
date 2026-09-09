package composehelper

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeManagedRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, bool, error) {
	if remove := step.GetManagedNetworkRemove(); remove != nil {
		response, err := executeManagedNetworkRemove(ctx, taskRunner, request.TimeoutSeconds, remove)
		return response, true, err
	}
	if remove := step.GetManagedVolumeRemove(); remove != nil {
		response, err := executeManagedVolumeRemove(ctx, taskRunner, request.TimeoutSeconds, remove, request.Plan)
		return response, true, err
	}
	return nil, false, nil
}

func executeManagedVolumeRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	remove *agentpb.ManagedVolumeRemove,
	plan *agentpb.ExecutionPlan,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	artifact, volume, err := volumeRemovalArtifact(plan, remove)
	if err != nil {
		return nil, err
	}
	if err := proveVolumeConsumersDetached(executionCtx, taskRunner, artifact, volume); err != nil {
		return nil, err
	}
	listed, listErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"volume", "ls", "--quiet", "--filter", "name=^" + remove.DockerName + "$"},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
	})
	if listErr != nil || listed.ExitCode != 0 {
		return nil, errs.New(errs.KindRequestFailed, "managed Volume absence could not be inspected")
	}
	exists := false
	for _, name := range strings.Fields(string(listed.Stdout)) {
		if name != remove.DockerName || exists {
			return nil, errs.New(errs.KindStateConflict, "managed Volume listing changed")
		}
		exists = true
	}
	if !exists {
		return completedResponse(), nil
	}
	inspection, present, failure, err := inspectManagedComposeVolume(executionCtx, taskRunner, remove.DockerName)
	if err != nil || failure != nil || !present || !managedVolumeMatches(inspection, volume,
		artifact.AuthorizedVolumeDir+"/"+volume.ComposeName, artifact.ProjectName) {
		return nil, errs.New(errs.KindStateConflict, "managed Volume ownership could not be proved")
	}
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
	return failedResponse(int32(max(1, result.ExitCode))), nil
}
