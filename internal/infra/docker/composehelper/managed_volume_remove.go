package composehelper

import (
	"context"
	"math"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func removeCapturedVolume(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact,
	volume *agentpb.ComposeVolume,
) (*agentpb.ComposeHelperResponse, error) {
	if err := proveVolumeConsumersDetached(ctx, taskRunner, artifact, volume); err != nil {
		return nil, err
	}
	exists, err := managedVolumeExists(ctx, taskRunner, volume.DockerName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return completedResponse(), nil
	}
	inspection, present, failure, err := inspectManagedComposeVolume(ctx, taskRunner, volume.DockerName)
	if err != nil || failure != nil || !present || !managedVolumeMatches(inspection, volume,
		artifact.AuthorizedVolumeDir+"/"+volume.ComposeName, artifact.ProjectName) {
		return nil, errs.New(errs.KindStateConflict, "managed Volume ownership could not be proved")
	}
	result, runErr := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"volume", "rm", volume.DockerName},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if contextErr := ctx.Err(); contextErr != nil {
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

func managedVolumeExists(ctx context.Context, taskRunner runner.Runner, name string) (bool, error) {
	listed, listErr := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"volume", "ls", "--quiet", "--filter", "name=^" + name + "$"},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
	})
	if listErr != nil || listed.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "managed Volume absence could not be inspected")
	}
	exists := false
	for _, found := range strings.Fields(string(listed.Stdout)) {
		if found != name || exists {
			return false, errs.New(errs.KindStateConflict, "managed Volume listing changed")
		}
		exists = true
	}
	return exists, nil
}
