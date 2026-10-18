package composehelper

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type removalResource struct {
	kind    string
	id      string
	name    string
	labels  []*agentpb.LabelPair
	service *agentpb.ComposeService
	volume  *agentpb.ComposeVolume
}

// removeEnvironmentRuntime addresses only individually inspected resource IDs.
// Shared networks or foreign containers cause a failure before any deletion.
// Docker's non-force removal also refuses attached networks or used Volumes.
func removeEnvironmentRuntime(ctx context.Context, taskRunner runner.Runner, input hierarchyexecution.EnvironmentInput, inspectOnly bool) error {
	resources, err := environmentRemovalInventory(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if inspectOnly && len(resources) > 0 {
		return errs.New(errs.KindStateConflict, "Environment runtime resources remain; interrupted removal needs reconciliation")
	}
	for _, selected := range resources {
		if err := inspectRemovalResource(ctx, taskRunner, input, selected); err != nil {
			return err
		}
		if selected.kind == "container" {
			if err := stopRecoveryContainer(ctx, taskRunner, selected.id); err != nil {
				return err
			}
		} else {
			if _, err := environmentDocker(ctx, taskRunner, selected.kind, "rm", "--", selected.id); err != nil {
				return err
			}
		}
	}
	remaining, err := environmentRemovalInventory(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return errs.New(errs.KindStateConflict, "Environment runtime is still present after removal")
	}
	return nil
}

func environmentRemovalInventory(ctx context.Context, taskRunner runner.Runner, input hierarchyexecution.EnvironmentInput) ([]removalResource, error) {
	resources := make([]removalResource, 0)
	for _, kind := range []string{"container", "network", "volume"} {
		filters := []string{"label=com.groundplane.environment-id=" + input.Removal.EnvironmentId}
		if input.Removal.Compose != nil {
			filters = append(filters, "label=com.docker.compose.project="+input.Removal.Compose.ProjectName)
		}
		seen := make(map[string]bool)
		for _, filter := range filters {
			args := []string{kind, "ls"}
			if kind == "container" {
				args = append(args, "--all", "--no-trunc")
			}
			format := "{{.ID}}"
			if kind == "network" {
				args = append(args, "--no-trunc")
			}
			if kind == "volume" {
				format = "{{.Name}}"
			}
			args = append(args, "--filter", filter, "--format", format)
			raw, err := environmentDocker(ctx, taskRunner, args...)
			if err != nil {
				return nil, err
			}
			for _, id := range strings.Fields(string(raw)) {
				if seen[id] {
					continue
				}
				if len(resources) >= 4096 || kind != "volume" && !validComponentContainerID(id) ||
					len(id) > 256 || strings.ContainsAny(id, "\x00/\\") || strings.HasPrefix(id, "-") {
					return nil, foreignEnvironmentRemoval()
				}
				seen[id] = true
				selected, err := captureRemovalResource(ctx, taskRunner, input, kind, id)
				if err != nil {
					return nil, err
				}
				resources = append(resources, selected)
			}
		}
	}
	return resources, nil
}

type removalInspection struct {
	ID     string            `json:"Id"`
	Name   string            `json:"Name"`
	Labels map[string]string `json:"Labels"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func captureRemovalResource(ctx context.Context, taskRunner runner.Runner, input hierarchyexecution.EnvironmentInput, kind, id string) (removalResource, error) {
	inspected, err := removalResourceInspection(ctx, taskRunner, kind, id)
	if err != nil {
		return removalResource{}, err
	}
	artifact := input.Removal.Compose
	if artifact == nil || inspected.Labels["com.groundplane.managed"] != "true" ||
		inspected.Labels["com.groundplane.environment-id"] != input.Removal.EnvironmentId {
		return removalResource{}, foreignEnvironmentRemoval()
	}
	selected := removalResource{kind: kind, id: id, name: inspected.Name}
	switch kind {
	case "container":
		for _, service := range artifact.Services {
			if inspected.Labels["com.docker.compose.project"] == artifact.ProjectName &&
				inspected.Labels["com.docker.compose.service"] == service.ComposeName && hasAllExpectedLabels(inspected.Labels, service.ExpectedLabels) {
				selected.service, selected.labels = service, service.ExpectedLabels
				break
			}
		}
	case "network":
		for _, network := range artifact.Networks {
			if inspected.Name == network.DockerName && hasAllExpectedLabels(inspected.Labels, network.ExpectedLabels) {
				selected.labels = network.ExpectedLabels
				break
			}
		}
	case "volume":
		for _, volume := range artifact.Volumes {
			if inspected.Name == volume.DockerName && hasAllExpectedLabels(inspected.Labels, volume.ExpectedLabels) {
				selected.volume, selected.labels = volume, volume.ExpectedLabels
				break
			}
		}
	}
	if len(selected.labels) == 0 {
		return removalResource{}, foreignEnvironmentRemoval()
	}
	if err := inspectRemovalResource(ctx, taskRunner, input, selected); err != nil {
		return removalResource{}, err
	}
	return selected, nil
}

func inspectRemovalResource(ctx context.Context, taskRunner runner.Runner, input hierarchyexecution.EnvironmentInput, selected removalResource) error {
	observed, err := removalResourceInspection(ctx, taskRunner, selected.kind, selected.id)
	if err != nil {
		return err
	}
	if observed.Name != selected.name || observed.Labels["com.groundplane.environment-id"] != input.Removal.EnvironmentId ||
		observed.Labels["com.groundplane.managed"] != "true" || !hasAllExpectedLabels(observed.Labels, selected.labels) {
		return foreignEnvironmentRemoval()
	}
	if selected.service != nil {
		return inspectRestorationImage(ctx, taskRunner, selected.id, selected.service)
	}
	if selected.volume != nil {
		inspection, present, failure, err := inspectManagedComposeVolume(ctx, taskRunner, selected.id)
		if err != nil {
			return err
		}
		if failure != nil || !present || !managedVolumeMatches(inspection, selected.volume,
			filepath.Join(input.Removal.ExpectedVolumeDir, selected.volume.ComposeName), input.Removal.Compose.ProjectName) {
			return foreignEnvironmentRemoval()
		}
	}
	return nil
}

func removalResourceInspection(ctx context.Context, taskRunner runner.Runner, kind, id string) (removalInspection, error) {
	raw, err := environmentDocker(ctx, taskRunner, kind, "inspect", "--format", "{{json .}}", id)
	if err != nil {
		return removalInspection{}, err
	}
	var inspected removalInspection
	if json.Unmarshal(raw, &inspected) != nil {
		return removalInspection{}, foreignEnvironmentRemoval()
	}
	if kind == "container" {
		inspected.Labels = inspected.Config.Labels
	}
	if kind == "volume" && inspected.Name != id || kind != "volume" && inspected.ID != id {
		return removalInspection{}, foreignEnvironmentRemoval()
	}
	return inspected, nil
}

func environmentDocker(ctx context.Context, taskRunner runner.Runner, args ...string) ([]byte, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
		Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 1 << 20})
	defer clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		clear(result.Stdout)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errs.New(errs.KindRequestFailed, "Environment runtime inspection or removal failed")
	}
	return result.Stdout, nil
}

func foreignEnvironmentRemoval() error {
	return errs.New(errs.KindStateConflict, "Environment runtime ownership differs from the captured removal input")
}
