package composehelper

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type componentCommandRun func(agentpb.ComposeHelperDiagnostic, ...string) (runner.Result, *agentpb.ComposeHelperResponse, error)

// Absence is distinct from a failed inventory or an ownership collision. Both
// activation and read-only recovery proof authenticate the same exact runtime.
func selectComponentActionContainer(run componentCommandRun, artifact *agentpb.ComposeArtifact,
	action *agentpb.ComponentApply, recipe ComponentActionRecipe,
) (string, *agentpb.ComposeHelperResponse, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() != action.GetComponentId() {
			continue
		}
		if selected != nil {
			return "", nil, errs.New(errs.KindValidationFailed, "Component action Service ownership is not unique")
		}
		selected = service
	}
	if selected == nil {
		return "", nil, errs.New(errs.KindValidationFailed, "Component action Service ownership is absent")
	}
	diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED
	listed, failure, err := run(diagnostic, "container", "ls",
		"--filter", "label=com.groundplane.managed=true",
		"--filter", "label=com.groundplane.kind=service",
		"--filter", "label=com.groundplane.environment-id="+artifact.GetOwnerId(),
		"--filter", "label=com.groundplane.component-id="+action.GetComponentId(),
		"--filter", "label=com.groundplane.service-id="+selected.GetServiceId(),
		"--filter", "status=running", "--format", "{{.ID}}")
	if err != nil || failure != nil {
		return "", failure, err
	}
	containers := strings.Fields(string(listed.Stdout))
	if len(containers) == 0 {
		return "", nil, nil
	}
	if len(containers) != 1 || !validComponentContainerID(containers[0]) {
		return "", nil, errs.New(errs.KindStateConflict, "Component action found multiple or invalid owned containers")
	}
	containerID := containers[0]
	labelsResult, failure, err := run(
		diagnostic,
		"container",
		"inspect",
		"--format",
		"{{json .Config.Labels}}",
		containerID,
	)
	if err != nil || failure != nil {
		return "", failure, err
	}
	labels := map[string]string{}
	if json.Unmarshal([]byte(strings.TrimSpace(string(labelsResult.Stdout))), &labels) != nil ||
		!hasAllExpectedLabels(labels, selected.GetExpectedLabels()) {
		return "", nil, errs.New(errs.KindStateConflict, "Component container labels differ from the captured runtime")
	}
	image, failure, err := run(diagnostic, "container", "inspect", "--format",
		"{{.Config.Image}}\n{{.Image}}\n{{json .ImageManifestDescriptor}}", containerID)
	if err != nil || failure != nil {
		return "", failure, err
	}
	if !recipe.matchesImage(image.Stdout) {
		return "", nil, errs.New(errs.KindStateConflict, "Component container image differs from the compiled recipe")
	}
	mounted, failure, err := run(diagnostic, "container", "inspect", "--format", "{{json .Mounts}}", containerID)
	if err != nil || failure != nil {
		return "", failure, err
	}
	mounts := []componentConfigMount{}
	configPath := filepath.Join(artifact.GetAuthorizedVolumeDir(), filepath.FromSlash(recipe.relativePath))
	if json.Unmarshal([]byte(strings.TrimSpace(string(mounted.Stdout))), &mounts) != nil ||
		!hasExactComponentConfigMount(mounts, configPath, recipe.containerPath) {
		return "", nil, errs.New(
			errs.KindStateConflict,
			"Component container configuration mount differs from the captured directory",
		)
	}
	return containerID, nil, nil
}
