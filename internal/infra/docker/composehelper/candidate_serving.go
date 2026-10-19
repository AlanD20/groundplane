package composehelper

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func servingMemberProven(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	services []*agentpb.ComposeService,
	proxy *agentpb.ComposeService,
) (bool, error) {
	proven := true
	for _, service := range services {
		ready, err := servingPredecessorProven(ctx, taskRunner, artifact, service)
		if err != nil {
			return false, err
		}
		proven = proven && ready
	}
	if proxy == nil {
		return proven, nil
	}
	proxyProven, err := servingPredecessorProven(ctx, taskRunner, artifact, proxy)
	return proven && proxyProven, err
}

// Newly restored workloads may still be starting. Readiness consumes the same
// compensation deadline; it never starts another Compose action or renews time.
func waitServingMember(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact,
	services []*agentpb.ComposeService, proxy *agentpb.ComposeService,
) (bool, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		proven, err := servingMemberProven(ctx, taskRunner, artifact, services, proxy)
		if err != nil || proven {
			return proven, err
		}
		select {
		case <-ctx.Done():
			return false, errs.Wrap(errs.KindRequestFailed, ctx.Err())
		case <-ticker.C:
		}
	}
}

func servingPredecessorProxy(artifact *agentpb.ComposeArtifact, serviceID string) (*agentpb.ComposeService, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if selected != nil {
			return nil, errs.New(errs.KindStateConflict, "serving predecessor proxy is ambiguous")
		}
		selected = service
	}
	return selected, nil
}

func servingPredecessorProven(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	service *agentpb.ComposeService,
) (bool, error) {
	expectedRole, roleErr := sealedServingPredecessorRuntimeRole(service)
	workloadWithoutImage := service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
		service.GetImageReference() == ""
	if roleErr != nil || workloadWithoutImage {
		return false, errs.New(errs.KindValidationFailed, "serving predecessor runtime identity is incomplete")
	}
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{
			"container",
			"ls",
			"--all",
			"--filter",
			"label=com.docker.compose.project=" + artifact.GetProjectName(),
			"--format",
			"{{.ID}}",
		},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "serving predecessor observation failed")
	}
	count := uint32(0)
	healthy := true
	seen := make(map[string]bool)
	for _, containerID := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(containerID) || seen[containerID] {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container identity is invalid")
		}
		seen[containerID] = true
		inspected, inspectErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if inspectErr != nil || inspected.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
			labels["com.docker.compose.project"] != artifact.GetProjectName() {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container evidence is ambiguous")
		}
		if labels["com.groundplane.service-id"] != service.GetServiceId() {
			continue
		}
		if labels["com.groundplane.runtime-role"] != expectedRole {
			continue
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA &&
			labels["com.docker.compose.service"] != service.GetComposeName() {
			continue // The complete inventory separately rejects uncaptured replicas.
		}
		expectedSlot := expectedLabel(service.GetExpectedLabels(), "com.groundplane.slot")
		if expectedSlot != "" && labels["com.groundplane.slot"] != expectedSlot {
			continue
		}
		if !hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
			return false, errs.New(errs.KindStateConflict, "serving predecessor observed foreign service state")
		}
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			image, imageErr := taskRunner.Run(ctx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{.Config.Image}}", containerID},
				Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
			})
			if imageErr != nil || image.ExitCode != 0 ||
				strings.TrimSpace(string(image.Stdout)) != service.GetImageReference() {
				return false, errs.New(errs.KindStateConflict, "serving predecessor workload image diverges")
			}
		}
		state, stateErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .State}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		observed := struct {
			Running bool `json:"Running"`
			Health  *struct {
				Status string `json:"Status"`
			} `json:"Health"`
		}{}
		if stateErr != nil || state.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(state.Stdout))), &observed) != nil {
			return false, errs.New(errs.KindStateConflict, "serving predecessor workload state is ambiguous")
		}
		if !observed.Running ||
			service.GetHasHealthcheck() && (observed.Health == nil || observed.Health.Status != "healthy") {
			healthy = false
		}
		count++
	}
	if count > service.GetExpectedReplicas() {
		return false, errs.New(errs.KindStateConflict, "serving predecessor workload count is ambiguous")
	}
	return count == service.GetExpectedReplicas() && healthy, nil
}

func sealedServingPredecessorRuntimeRole(service *agentpb.ComposeService) (string, error) {
	role := ""
	switch service.GetRole() {
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
		role = "singleton"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
		role = "slot"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
		role = "replica"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
		role = "proxy"
	default:
		return "", errs.New(errs.KindValidationFailed, "serving predecessor runtime role is unsupported")
	}
	if expectedLabel(service.GetExpectedLabels(), "com.groundplane.runtime-role") != role {
		return "", errs.New(errs.KindValidationFailed, "serving predecessor runtime role diverges")
	}
	return role, nil
}

func expectedLabel(labels []*agentpb.LabelPair, key string) string {
	for _, label := range labels {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}
