package composehelper

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
	"path/filepath"
	"strings"
	"time"
)

type componentConfigMount struct {
	Type        string `json:"Type"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	RW          bool   `json:"RW"`
}

// ComponentActionCatalog resolves a sealed Component action to one compiled
// registered recipe. No recipe field is accepted over the helper protocol.
type ComponentActionCatalog interface {
	ResolveContainerConfigAction(*agentpb.ComponentApply) (ComponentActionRecipe, error)
}

func executeComponentContainerConfigAction(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	artifact *agentpb.ComposeArtifact,
	apply *agentpb.ComponentApply,
	recipe ComponentActionRecipe,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	fail := func(exitCode int, diagnostic agentpb.ComposeHelperDiagnostic) *agentpb.ComposeHelperResponse {
		if exitCode <= 0 || exitCode > math.MaxInt32 {
			exitCode = 1
		}
		return &agentpb.ComposeHelperResponse{
			Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
			ExitCode: int32(exitCode), Diagnostic: diagnostic,
		}
	}
	run := func(
		diagnostic agentpb.ComposeHelperDiagnostic,
		args ...string,
	) (runner.Result, *agentpb.ComposeHelperResponse, error) {
		result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: args, Dir: WorkDirectory,
			Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		if contextErr := executionCtx.Err(); contextErr != nil {
			return runner.Result{}, nil, contextErr
		}
		if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
			return runner.Result{}, nil, errs.New(
				errs.KindInternal,
				"Component action command returned an invalid exit code",
			)
		}
		if runErr != nil || result.ExitCode != 0 {
			return result, fail(result.ExitCode, diagnostic), nil
		}
		return result, nil, nil
	}
	configPath := filepath.Join(artifact.AuthorizedVolumeDir, filepath.FromSlash(recipe.relativePath))
	var selectedService *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() == apply.GetComponentId() {
			if selectedService != nil {
				return nil, errs.New(errs.KindValidationFailed, "Component action service ownership is not unique")
			}
			selectedService = service
		}
	}
	if selectedService == nil {
		return nil, errs.New(errs.KindValidationFailed, "Component action service ownership is absent")
	}
	serviceID := selectedService.GetServiceId()
	listed, failure, err := run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		"container", "ls",
		"--filter", "label=com.groundplane.managed=true",
		"--filter", "label=com.groundplane.kind=service",
		"--filter", "label=com.groundplane.environment-id="+artifact.OwnerId,
		"--filter", "label=com.groundplane.component-id="+apply.GetComponentId(),
		"--filter", "label=com.groundplane.service-id="+serviceID,
		"--filter", "status=running", "--format", "{{.ID}}",
	)
	if err != nil || failure != nil {
		return failure, err
	}
	containers := strings.Fields(string(listed.Stdout))
	if len(containers) != 1 || !validComponentContainerID(containers[0]) {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), nil
	}
	containerID := containers[0]
	inspectedLabels, failure, err := run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		"container", "inspect", "--format", "{{json .Config.Labels}}", containerID,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	labels := map[string]string{}
	if json.Unmarshal([]byte(strings.TrimSpace(string(inspectedLabels.Stdout))), &labels) != nil ||
		!hasAllExpectedLabels(labels, selectedService.GetExpectedLabels()) {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), nil
	}
	inspectedImage, failure, err := run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		"container",
		"inspect",
		"--format",
		"{{.Config.Image}}\n{{.Image}}\n{{json .ImageManifestDescriptor}}",
		containerID,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	if !recipe.matchesImage(inspectedImage.Stdout) {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), nil
	}
	inspectedMounts, failure, err := run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		"container", "inspect", "--format", "{{json .Mounts}}", containerID,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	mounts := []componentConfigMount{}
	if json.Unmarshal([]byte(strings.TrimSpace(string(inspectedMounts.Stdout))), &mounts) != nil ||
		!hasExactComponentConfigMount(mounts, configPath, recipe.containerPath) {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), nil
	}
	if matches, hashErr := containerConfigMatches(executionCtx, run, containerID, recipe.containerPath, apply.GetArtifactDigest()); hashErr != nil ||
		!matches {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), hashErr
	}
	validateCommand := append([]string{"container", "exec", containerID}, recipe.validateArgs...)
	_, failure, err = run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED,
		validateCommand...,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	if matches, hashErr := containerConfigMatches(executionCtx, run, containerID, recipe.containerPath, apply.GetArtifactDigest()); hashErr != nil ||
		!matches {
		return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), hashErr
	}
	activateCommand := append([]string{"container", "exec", containerID}, recipe.activateArgs...)
	_, failure, err = run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		activateCommand...,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	return completedResponse(), nil
}

func containerConfigMatches(
	_ context.Context,
	run func(agentpb.ComposeHelperDiagnostic, ...string) (runner.Result, *agentpb.ComposeHelperResponse, error),
	containerID string,
	containerPath string,
	expected []byte,
) (bool, error) {
	result, failure, err := run(
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		"container", "exec", containerID, "sha256sum", "--", containerPath,
	)
	if err != nil {
		return false, err
	}
	if failure != nil {
		return false, nil
	}
	fields := strings.Fields(string(result.Stdout))
	if len(fields) != 2 || len(fields[0]) != 64 || fields[1] != containerPath {
		return false, nil
	}
	decoded, decodeErr := hex.DecodeString(fields[0])
	return decodeErr == nil && subtle.ConstantTimeCompare(decoded, expected) == 1, nil
}

func hasAllExpectedLabels(actual map[string]string, expected []*agentpb.LabelPair) bool {
	for _, label := range expected {
		if label == nil || actual[label.GetKey()] != label.GetValue() {
			return false
		}
	}
	return true
}

func hasExactComponentConfigMount(mounts []componentConfigMount, source string, destination string) bool {
	sourceDirectory, destinationDirectory := filepath.Dir(source), filepath.Dir(destination)
	matches := 0
	for _, mount := range mounts {
		if mount.Destination != destinationDirectory {
			if mount.Destination == destination || strings.HasPrefix(destination, mount.Destination+"/") &&
				strings.HasPrefix(mount.Destination, destinationDirectory+"/") {
				return false
			}
			continue
		}
		if mount.Type != "bind" || filepath.Clean(mount.Source) != sourceDirectory || mount.RW {
			return false
		}
		matches++
	}
	return matches == 1
}

func validComponentContainerID(value string) bool {
	if len(value) < 12 || len(value) > 64 {
		return false
	}
	for _, character := range []byte(value) {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validRelativeComponentConfigPath(value string) bool {
	return value != "" && len(value) <= 240 && !filepath.IsAbs(value) &&
		filepath.Clean(filepath.FromSlash(value)) == filepath.FromSlash(value) &&
		value != "." && !strings.HasPrefix(value, "../") && !strings.ContainsRune(value, 0)
}

func validComponentCommand(arguments []string) bool {
	if len(arguments) == 0 || len(arguments) > 32 {
		return false
	}
	for _, argument := range arguments {
		if argument == "" || len(argument) > 1024 || strings.ContainsRune(argument, 0) {
			return false
		}
	}
	return true
}

func validImmutableImageReference(value string) bool {
	separator := strings.LastIndex(value, "@sha256:")
	if separator <= 0 || len(value)-separator != len("@sha256:")+64 {
		return false
	}
	for _, character := range value[separator+len("@sha256:"):] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
