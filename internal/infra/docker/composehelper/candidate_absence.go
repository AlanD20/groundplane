package composehelper

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func exactCandidateContainers(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	candidate *agentpb.ReleaseRestorationCandidate,
	projectName string,
) ([]string, error) {
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{
			"container",
			"ls",
			"--all",
			"--filter",
			"label=com.docker.compose.project=" + projectName,
			"--format",
			"{{.ID}}",
		},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return nil, errs.New(errs.KindRequestFailed, "candidate absence observation failed")
	}
	found := make([]string, 0)
	for _, containerID := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(containerID) {
			return nil, errs.New(errs.KindStateConflict, "candidate absence observed an invalid container identity")
		}
		inspected, inspectErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if inspectErr != nil || inspected.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
			labels["com.docker.compose.project"] != projectName {
			return nil, errs.New(errs.KindStateConflict, "candidate absence container evidence is ambiguous")
		}
		if labels["com.groundplane.service-id"] != candidate.GetServiceId() {
			continue
		}
		matched := false
		for _, service := range artifact.GetServices() {
			releaseMatches := labels["com.groundplane.release-id"] == candidate.GetReleaseId()
			if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				releaseMatches = labels["com.groundplane.release-id"] == "" &&
					expectedLabel(service.GetExpectedLabels(), "com.groundplane.release-id") == ""
			}
			if service.GetServiceId() == labels["com.groundplane.service-id"] &&
				releaseMatches && hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
				matched = true
				break
			}
		}
		if !matched {
			return nil, errs.New(errs.KindStateConflict, "candidate absence observed foreign or stale candidate state")
		}
		found = append(found, containerID)
	}
	return found, nil
}
