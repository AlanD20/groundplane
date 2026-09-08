package composehelper

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type managedVolumeInspection struct {
	Name    string            `json:"Name"`
	Driver  string            `json:"Driver"`
	Labels  map[string]string `json:"Labels"`
	Options map[string]string `json:"Options"`
}

func needsManagedVolumeEnsure(step *agentpb.ExecutionStep, artifact *agentpb.ComposeArtifact) bool {
	apply := step.GetComposeApply()
	if apply == nil || !apply.FullReconcile || artifact.GetAuthorizedVolumeDir() == "" ||
		len(artifact.GetVolumes()) == 0 {
		return false
	}
	for _, service := range artifact.GetServices() {
		if service.GetExpectedReplicas() != 0 {
			return false
		}
	}
	return true
}

func ensureManagedComposeVolumes(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ComposeHelperResponse, error) {
	volumes := append([]*agentpb.ComposeVolume(nil), artifact.GetVolumes()...)
	sort.Slice(volumes, func(left, right int) bool {
		return volumes[left].GetDockerName() < volumes[right].GetDockerName()
	})
	for _, volume := range volumes {
		inspection, exists, failure, err := inspectManagedComposeVolume(ctx, taskRunner, volume.GetDockerName())
		if err != nil || failure != nil {
			return failure, err
		}
		device := filepath.Join(artifact.GetAuthorizedVolumeDir(), volume.GetComposeName())
		if exists {
			if !managedVolumeMatches(inspection, volume, device, artifact.GetProjectName()) {
				return failedResponse(1), nil
			}
			continue
		}
		result, runErr, err := runManagedVolumeCommand(
			ctx,
			taskRunner,
			managedVolumeCreateArgs(volume, device, artifact.GetProjectName()),
		)
		if err != nil {
			return nil, err
		}
		if runErr != nil || result.ExitCode != 0 {
			return managedVolumeFailure(result.ExitCode), nil
		}
		inspection, exists, failure, err = inspectManagedComposeVolume(ctx, taskRunner, volume.GetDockerName())
		if err != nil || failure != nil {
			return failure, err
		}
		if !exists || !managedVolumeMatches(inspection, volume, device, artifact.GetProjectName()) {
			return failedResponse(1), nil
		}
	}
	return nil, nil
}

func inspectManagedComposeVolume(
	ctx context.Context,
	taskRunner runner.Runner,
	name string,
) (managedVolumeInspection, bool, *agentpb.ComposeHelperResponse, error) {
	result, runErr, err := runManagedVolumeCommand(
		ctx,
		taskRunner,
		[]string{"volume", "inspect", "--format", "{{json .}}", name},
	)
	if err != nil {
		return managedVolumeInspection{}, false, nil, err
	}
	if result.ExitCode == 1 && runErr != nil {
		return managedVolumeInspection{}, false, nil, nil
	}
	if runErr != nil || result.ExitCode != 0 {
		return managedVolumeInspection{}, false, managedVolumeFailure(result.ExitCode), nil
	}
	var inspection managedVolumeInspection
	if json.Unmarshal(result.Stdout, &inspection) != nil {
		return managedVolumeInspection{}, false, failedResponse(1), nil
	}
	return inspection, true, nil, nil
}

func runManagedVolumeCommand(
	ctx context.Context,
	taskRunner runner.Runner,
	args []string,
) (runner.Result, error, error) {
	result, runErr := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: args, Dir: WorkDirectory,
		Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		return runner.Result{}, nil, contextErr
	}
	if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
		return runner.Result{}, nil, errs.New(errs.KindInternal, "managed Volume command returned an invalid exit code")
	}
	if runErr != nil && result.ExitCode == 0 {
		return runner.Result{}, nil, errs.Wrap(errs.KindInternal, runErr)
	}
	return result, runErr, nil
}

func managedVolumeCreateArgs(volume *agentpb.ComposeVolume, device, projectName string) []string {
	args := []string{
		"volume", "create", "--driver", "local",
		"--opt", "type=none", "--opt", "o=bind", "--opt", "device=" + device,
		"--label", "com.docker.compose.project=" + projectName,
		"--label", "com.docker.compose.volume=" + volume.GetComposeName(),
	}
	labels := append([]*agentpb.LabelPair(nil), volume.GetExpectedLabels()...)
	sort.Slice(labels, func(left, right int) bool { return labels[left].GetKey() < labels[right].GetKey() })
	for _, label := range labels {
		args = append(args, "--label", label.GetKey()+"="+label.GetValue())
	}
	return append(args, volume.GetDockerName())
}

func managedVolumeMatches(
	inspection managedVolumeInspection,
	volume *agentpb.ComposeVolume,
	device, projectName string,
) bool {
	if inspection.Name != volume.GetDockerName() || inspection.Driver != "local" || len(inspection.Options) != 3 ||
		inspection.Labels["com.docker.compose.project"] != projectName ||
		inspection.Labels["com.docker.compose.volume"] != volume.GetComposeName() ||
		inspection.Options["type"] != "none" || inspection.Options["o"] != "bind" || inspection.Options["device"] != device {
		return false
	}
	for _, label := range volume.GetExpectedLabels() {
		if label == nil || inspection.Labels[label.GetKey()] != label.GetValue() {
			return false
		}
	}
	return true
}

func managedVolumeFailure(exitCode int) *agentpb.ComposeHelperResponse {
	if exitCode <= 0 || exitCode > math.MaxInt32 {
		exitCode = 1
	}
	return failedResponse(int32(exitCode))
}
