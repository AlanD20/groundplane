package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
	"path/filepath"
	"strings"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
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
	if recipe.activation == registeredcatalog.ContainerConfigActivationHTTPDiscovery &&
		apply.GetContainerConfigTransition() == nil ||
		recipe.activation == registeredcatalog.ContainerConfigActivationCommand &&
			apply.GetContainerConfigTransition() != nil {
		return nil, errs.New(errs.KindStateConflict,
			"Component activation mode does not match its captured configuration transition")
	}
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
			CaptureLimitBytes: envoyrouter.MaximumObservationBytes,
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
	containerID, failure, err := selectComponentActionContainer(run, artifact, apply, recipe)
	if err != nil || failure != nil {
		return failure, err
	}
	if containerID == "" {
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
	if recipe.activation == registeredcatalog.ContainerConfigActivationHTTPDiscovery {
		content, failure, err := run(
			agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
			"container", "exec", containerID, "cat", "--", recipe.containerPath,
		)
		if err != nil || failure != nil {
			return failure, err
		}
		digest := sha256.Sum256(content.Stdout)
		if !bytes.Equal(digest[:], apply.GetArtifactDigest()) {
			return fail(1, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED), nil
		}
		if err := activateHTTPDiscoveryConfiguration(executionCtx, taskRunner, containerID,
			apply.GetContainerConfigTransition().GetPreviousSource(), content.Stdout); err != nil {
			return nil, err
		}
		if matches, hashErr := containerConfigMatches(executionCtx, run, containerID, recipe.containerPath, apply.GetArtifactDigest()); hashErr != nil ||
			!matches {
			return fail(
				1,
				agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
			), hashErr
		}
		return completedResponse(), nil
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
