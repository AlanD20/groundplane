package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type servingInventory struct {
	candidates        []string
	proxyID           string
	proxyNeedsRestore bool
}

// inspectServingInventory permits effects only on the exact old or candidate
// runtime. Observed names, owner labels and actual images must agree together.
func inspectServingInventory(
	ctx context.Context,
	taskRunner runner.Runner,
	predecessor, candidate *agentpb.ComposeArtifact,
	serviceID, candidateReleaseID string,
	priorProxy *agentpb.ComposeService,
) (servingInventory, error) {
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{
			"container",
			"ls",
			"--all",
			"--filter",
			"label=com.docker.compose.project=" + predecessor.ProjectName,
			"--format",
			"{{.ID}}",
		},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return servingInventory{}, errs.New(errs.KindRequestFailed, "restoration inventory unavailable")
	}
	result := servingInventory{}
	seen := make(map[string]bool)
	for _, id := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return servingInventory{}, foreignRestorationRuntime()
		}
		seen[id] = true
		observed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", id},
			Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if err != nil || observed.ExitCode != 0 || json.Unmarshal(observed.Stdout, &labels) != nil ||
			labels["com.docker.compose.project"] != predecessor.ProjectName {
			return servingInventory{}, foreignRestorationRuntime()
		}
		if labels["com.groundplane.service-id"] != serviceID {
			if restorationOwnsComposeName(predecessor, serviceID, labels["com.docker.compose.service"]) ||
				restorationOwnsComposeName(candidate, serviceID, labels["com.docker.compose.service"]) {
				return servingInventory{}, foreignRestorationRuntime()
			}
			continue
		}
		prior := matchingRestorationService(predecessor, serviceID, labels)
		next := matchingRestorationService(candidate, serviceID, labels)
		selected := prior
		isCandidate := false
		if selected == nil {
			selected, isCandidate = next, true
		}
		if selected == nil {
			return servingInventory{}, foreignRestorationRuntime()
		}
		proxy := selected.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
		if !proxy && isCandidate && labels["com.groundplane.release-id"] != candidateReleaseID {
			return servingInventory{}, foreignRestorationRuntime()
		}
		if err := inspectRestorationImage(ctx, taskRunner, id, selected); err != nil {
			return servingInventory{}, err
		}
		if proxy && priorProxy != nil {
			if result.proxyID != "" {
				return servingInventory{}, foreignRestorationRuntime()
			}
			result.proxyID, result.proxyNeedsRestore = id, isCandidate
		} else if isCandidate {
			result.candidates = append(result.candidates, id)
		}
	}
	slices.Sort(result.candidates)
	return result, nil
}

func matchingRestorationService(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
) *agentpb.ComposeService {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID || service.GetComposeName() != labels["com.docker.compose.service"] ||
			!hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
			continue
		}
		if _, err := sealedServingPredecessorRuntimeRole(service); err != nil {
			continue
		}
		releaseID := expectedLabel(service.GetExpectedLabels(), "com.groundplane.release-id")
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			if releaseID == "" && labels["com.groundplane.release-id"] == "" {
				return service
			}
		} else if releaseID != "" && labels["com.groundplane.release-id"] == releaseID {
			return service
		}
	}
	return nil
}

func restorationOwnsComposeName(artifact *agentpb.ComposeArtifact, serviceID, name string) bool {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetComposeName() == name {
			return true
		}
	}
	return false
}

func inspectRestorationImage(
	ctx context.Context,
	taskRunner runner.Runner,
	id string,
	service *agentpb.ComposeService,
) error {
	format := "{{.Config.Image}}\n{{.Image}}"
	managed := service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
	if managed {
		format += "\n{{json .ImageManifestDescriptor}}"
	}
	observed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "inspect", "--format", format, id},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
	})
	if err != nil || observed.ExitCode != 0 {
		return foreignRestorationRuntime()
	}
	parts := strings.Split(strings.TrimSpace(string(observed.Stdout)), "\n")
	if len(parts) < 2 || parts[0] != service.GetImageReference() {
		return foreignRestorationRuntime()
	}
	if !managed {
		if len(parts) != 2 || !workloadimage.LocalIDValid(service.GetImageReference()) ||
			parts[1] != service.GetImageReference() {
			return foreignRestorationRuntime()
		}
		return nil
	}
	if len(parts) != 3 || len(service.GetImageChildDigest()) != sha256.Size ||
		len(service.GetImageConfigDigest()) != sha256.Size {
		return foreignRestorationRuntime()
	}
	var descriptor *ocispec.Descriptor
	if json.Unmarshal([]byte(parts[2]), &descriptor) != nil {
		return foreignRestorationRuntime()
	}
	return managedimage.Verify(
		parts[1],
		descriptor,
		"sha256:"+hex.EncodeToString(
			service.GetImageChildDigest(),
		),
		"sha256:"+hex.EncodeToString(service.GetImageConfigDigest()),
		ocispec.Platform{
			OS:           service.GetImageOs(),
			Architecture: service.GetImageArchitecture(),
			Variant:      service.GetImageVariant(),
		},
	)
}

func foreignRestorationRuntime() error {
	return errs.New(errs.KindStateConflict, "restoration inventory contains foreign or ambiguous runtime")
}
