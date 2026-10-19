package composehelper

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveLifecycleHookContainer is read-only and accepts only the single exact
// captured workload, never a proxy or a replacement with the same Service name.
func ResolveLifecycleHookContainer(
	ctx context.Context,
	taskRunner runner.Runner,
	input *agentpb.ServiceLifecycleWorkInput,
) (string, error) {
	if executionplan.ValidateServiceLifecycleWorkInput(input) != nil || input.Hook == nil || taskRunner == nil {
		return "", errs.New(errs.KindValidationFailed, "Lifecycle hook requires its captured workload")
	}
	var target string
	for _, member := range input.Members {
		service := member.Artifact.Services[0]
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		containers, err := inspectCapturedContainers(ctx, taskRunner, member.Artifact, service)
		if err != nil {
			return "", err
		}
		if len(containers) != 1 || target != "" {
			return "", errs.New(
				errs.KindStateConflict,
				"Lifecycle hook requires exactly one captured backing container",
			)
		}
		target = containers[0]
	}
	if target == "" {
		return "", errs.New(errs.KindStateConflict, "Lifecycle hook backing container is absent")
	}
	state, err := lifecycleDocker(
		ctx,
		taskRunner,
		"container",
		"inspect",
		"--format",
		"{{.State.Running}} {{.State.Paused}} {{.State.Restarting}}",
		target,
	)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(state.Stdout)) != "true false false" {
		return "", errs.New(errs.KindStateConflict, "Lifecycle hook backing container is not running")
	}
	return target, nil
}
