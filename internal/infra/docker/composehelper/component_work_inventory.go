package composehelper

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type componentWorkContainer struct {
	ID     string `json:"Id"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool `json:"Running"`
		Health  *struct {
			Status string `json:"Status"`
		} `json:"Health"`
	} `json:"State"`
}

type componentWorkInventory struct {
	Candidate []componentWorkContainer
	Prior     []componentWorkContainer
}

// Inventory admits only identities from the captured candidate and predecessor.
// A matching human-readable name alone never grants removal authority.
func inspectComponentWork(
	ctx context.Context,
	taskRunner runner.Runner,
	input componentexecution.HelperInput,
) (componentWorkInventory, error) {
	var inventory componentWorkInventory
	listed, err := runComponentWork(ctx, taskRunner, "container", "ls", "--all", "--no-trunc",
		"--filter", "label=com.groundplane.environment-id="+input.Procedure.EnvironmentId,
		"--filter", "label=com.groundplane.component-id="+input.Procedure.ComponentId, "--format", "{{.ID}}")
	if err != nil {
		return inventory, err
	}
	seen := make(map[string]bool)
	for _, id := range strings.Fields(string(listed.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return inventory, invalidComponentInventory()
		}
		seen[id] = true
		result, err := runComponentWork(ctx, taskRunner, "container", "inspect", "--format", "{{json .}}", id)
		if err != nil {
			return inventory, err
		}
		var container componentWorkContainer
		if json.Unmarshal(result.Stdout, &container) != nil || container.ID != id {
			return inventory, invalidComponentInventory()
		}
		switch {
		case componentWorkMatches(container, input.Procedure.Candidate):
			inventory.Candidate = append(inventory.Candidate, container)
		case componentWorkMatches(container, input.Procedure.Predecessor.Runtime):
			inventory.Prior = append(inventory.Prior, container)
		default:
			return inventory, invalidComponentInventory()
		}
	}
	if len(inventory.Candidate) > 1 || len(inventory.Prior) > 1 {
		return inventory, invalidComponentInventory()
	}
	return inventory, nil
}

func componentWorkMatches(container componentWorkContainer, artifact *agentpb.ComposeArtifact) bool {
	if artifact == nil || len(artifact.Services) != 1 || artifact.Services[0] == nil {
		return false
	}
	service := artifact.Services[0]
	labels := container.Config.Labels
	return labels["com.docker.compose.project"] == artifact.ProjectName &&
		labels["com.docker.compose.service"] == service.ComposeName &&
		labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.environment-id"] == artifact.OwnerId &&
		labels["com.groundplane.component-id"] == service.OwnerComponentId &&
		hasAllExpectedLabels(labels, service.ExpectedLabels) && container.Config.Image == service.ImageReference
}

func componentWorkReady(containers []componentWorkContainer) bool {
	return len(containers) == 1 && containers[0].State.Running &&
		(containers[0].State.Health == nil || containers[0].State.Health.Status == "healthy")
}

func invalidComponentInventory() error {
	return errs.New(
		errs.KindStateConflict,
		"Component containers do not match the captured candidate or previous runtime; no containers were selected for removal",
	)
}

func runComponentWork(ctx context.Context, taskRunner runner.Runner, args ...string) (runner.Result, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
		Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: maximumComponentConfig})
	if ctx.Err() != nil {
		return runner.Result{}, ctx.Err()
	}
	if err != nil {
		return runner.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	if result.ExitCode != 0 {
		return runner.Result{}, errs.New(
			errs.KindRequestFailed,
			"Docker could not inspect or update the captured Component runtime",
		)
	}
	return result, nil
}
