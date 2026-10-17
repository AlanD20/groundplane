package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A captured name is not permission to adopt an unrelated container. Inspect
// all current replicas against the baseline labels and image before Compose can
// recreate them. No current container means no startup merely to remove a mount.
func inspectCapturedWorkloads(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact, name string) ([]string, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service == nil {
			return nil, errs.New(errs.KindValidationFailed, "captured workload metadata is incomplete")
		}
		if service.ComposeName == name {
			selected = service
			break
		}
	}
	if selected == nil || selected.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
		return nil, errs.New(errs.KindValidationFailed, "operation must select a captured workload")
	}
	listed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "ls", "--all", "--quiet", "--no-trunc",
			"--filter", "label=com.docker.compose.project=" + artifact.ProjectName,
			"--filter", "label=com.docker.compose.service=" + name},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
	})
	if err != nil || listed.ExitCode != 0 || len(listed.Stdout) >= 1024*1024 {
		return nil, errs.New(errs.KindRequestFailed, "captured workload inventory is unavailable")
	}
	seen := make(map[string]bool)
	for _, id := range strings.Fields(string(listed.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return nil, errs.New(errs.KindStateConflict, "captured workload inventory is invalid")
		}
		seen[id] = true
		inspected, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", id},
			Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024,
		})
		var labels map[string]string
		if err != nil || inspected.ExitCode != 0 || json.Unmarshal(inspected.Stdout, &labels) != nil ||
			labels["com.docker.compose.project"] != artifact.ProjectName || labels["com.docker.compose.service"] != name ||
			labels["com.groundplane.managed"] != "true" || labels["com.groundplane.environment-id"] != artifact.OwnerId ||
			labels["com.groundplane.service-id"] != selected.ServiceId ||
			!hasAllExpectedLabels(labels, selected.ExpectedLabels) {
			return nil, errs.New(errs.KindStateConflict, "captured workload ownership differs from the captured workload")
		}
		if err := inspectRestorationImage(ctx, taskRunner, id, selected); err != nil {
			return nil, err
		}
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	slices.Sort(result)
	return result, nil
}
