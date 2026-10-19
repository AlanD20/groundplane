package composehelper

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func inspectResolverRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	input resolverexecution.HelperInput,
) ([]componentWorkContainer, error) {
	listed, err := runComponentWork(ctx, taskRunner, "container", "ls", "--all", "--no-trunc",
		"--filter", "label=com.groundplane.component-id="+input.Layout.ComponentID,
		"--filter", "label=com.docker.compose.project=groundplane-infra", "--format", "{{.ID}}")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var inventory []componentWorkContainer
	for _, id := range strings.Fields(string(listed.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return nil, invalidResolverRuntime()
		}
		seen[id] = true
		result, err := runComponentWork(ctx, taskRunner, "container", "inspect", "--format", "{{json .}}", id)
		if err != nil {
			return nil, err
		}
		var container componentWorkContainer
		decodeErr := json.Unmarshal(result.Stdout, &container)
		clear(result.Stdout)
		clear(result.Stderr)
		if decodeErr != nil || container.ID != id {
			return nil, invalidResolverRuntime()
		}
		candidate, previous := input.Assignment.Input.Candidate, input.Assignment.Input.Previous
		if (candidate == nil || !resolverContainerMatches(container, candidate.Runtime)) &&
			(previous == nil || !resolverContainerMatches(container, previous.Runtime)) {
			return nil, invalidResolverRuntime()
		}
		inventory = append(inventory, container)
		if len(inventory) > 2 {
			return nil, invalidResolverRuntime()
		}
	}
	return inventory, nil
}

func resolverContainerMatches(container componentWorkContainer, artifact *agentpb.ComposeArtifact) bool {
	if artifact == nil || len(artifact.Services) != 1 || artifact.Services[0] == nil {
		return false
	}
	service := artifact.Services[0]
	labels := container.Config.Labels
	return labels["com.docker.compose.project"] == artifact.ProjectName &&
		labels["com.docker.compose.service"] == service.ComposeName &&
		labels["com.groundplane.managed"] == "true" &&
		labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == service.OwnerComponentId &&
		labels["com.groundplane.environment-id"] == "" &&
		hasAllExpectedLabels(labels, service.ExpectedLabels) &&
		container.Config.Image == service.ImageReference
}

func resolverRuntimeReady(inventory []componentWorkContainer, artifact *agentpb.ComposeArtifact) bool {
	return len(inventory) == 1 && resolverContainerMatches(inventory[0], artifact) && componentWorkReady(inventory)
}

func reconcileResolverRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	input resolverexecution.HelperInput,
	restore bool,
) error {
	configuration := input.Assignment.Input
	target := configuration.Candidate
	if restore {
		if !configuration.PreviousEnabled {
			return removeResolverRuntime(ctx, taskRunner, input)
		}
		target = configuration.Previous
	}
	if target == nil {
		return invalidResolverRuntime()
	}
	inventory, err := inspectResolverRuntime(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if resolverRuntimeReady(inventory, target.Runtime) {
		return nil
	}
	for _, container := range inventory {
		if !resolverContainerMatches(container, target.Runtime) {
			if err := removeComponentContainer(ctx, taskRunner, container.ID); err != nil {
				return err
			}
		}
	}
	if err := applySingletonArtifact(ctx, taskRunner, target.Runtime); err != nil {
		return err
	}
	for {
		inventory, err := inspectResolverRuntime(ctx, taskRunner, input)
		if err != nil {
			return err
		}
		if resolverRuntimeReady(inventory, target.Runtime) {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func removeResolverRuntime(ctx context.Context, taskRunner runner.Runner, input resolverexecution.HelperInput) error {
	inventory, err := inspectResolverRuntime(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	for _, container := range inventory {
		if err := removeComponentContainer(ctx, taskRunner, container.ID); err != nil {
			return err
		}
	}
	remaining, err := inspectResolverRuntime(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return invalidResolverRuntime()
	}
	return nil
}

func invalidResolverRuntime() error {
	return errs.New(
		errs.KindStateConflict,
		"CoreDNS runtime does not match the captured container; no unrelated runtime was selected for removal",
	)
}
