// Package composehelper owns the task-scoped Docker Compose helper protocol
// and its exact CLI procedure. It never owns task policy or observed state.
package composehelper

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion          = 1
	maximumFramedBytes     = executionplan.MaximumPlanBytes + 64*1024
	frameHeaderBytes       = 4
	maximumTimeout         = uint32(math.MaxInt32)
	maximumComponentConfig = 1024 * 1024
)

// MarshalRequest returns one complete request frame for helper stdin.
func MarshalRequest(request *agentpb.ComposeHelperRequest) ([]byte, error) {
	owned, _, _, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(encoded)
}

// ArtifactForRequest returns the one owned artifact authorized for the
// selected helper step. It applies the same validation as frame encoding.
func ArtifactForRequest(request *agentpb.ComposeHelperRequest) (*agentpb.ComposeArtifact, error) {
	_, _, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if artifact == nil {
		return nil, nil
	}
	return proto.Clone(artifact).(*agentpb.ComposeArtifact), nil
}

// ReadRequest decodes exactly one request and rejects trailing stdin bytes.
func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.ComposeHelperRequest, error) {
	encoded, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	request := &agentpb.ComposeHelperRequest{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, request); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper request protobuf is invalid")
	}
	owned, _, _, err := validateRequest(request)
	return owned, err
}

// WriteResponse writes one complete helper response frame.
func WriteResponse(ctx context.Context, output io.Writer, response *agentpb.ComposeHelperResponse) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateResponse(response); err != nil {
		return err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	framed, err := frame(encoded)
	if err != nil {
		return err
	}
	for len(framed) != 0 {
		written, writeErr := output.Write(framed)
		if writeErr != nil {
			return errs.Wrap(errs.KindInternal, writeErr)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "Compose helper response writer made no progress")
		}
		framed = framed[written:]
	}
	return nil
}

// ReadResponse decodes exactly one response from helper stdout.
func ReadResponse(ctx context.Context, input io.Reader) (*agentpb.ComposeHelperResponse, error) {
	encoded, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	response := &agentpb.ComposeHelperResponse{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, response); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper response protobuf is invalid")
	}
	if err := validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

// Execute validates one request, runs its closed Compose procedure, and
// returns only a bounded diagnostic category. Raw process output is discarded.
func Execute(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return execute(ctx, taskRunner, request, nil)
}

// ExecuteWithComponentCatalog additionally enables the closed registered
// Component procedure resolved inside the helper process.
func ExecuteWithComponentCatalog(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
	catalog ComponentActionCatalog,
) (*agentpb.ComposeHelperResponse, error) {
	if catalog == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper Component catalog is required")
	}
	return execute(ctx, taskRunner, request, catalog)
}

func execute(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest,
	catalog ComponentActionCatalog,
) (*agentpb.ComposeHelperResponse, error) {
	if taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper Runner is required")
	}
	owned, step, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if response, handled, removeErr := executeManagedRemove(ctx, taskRunner, owned, step); handled {
		return response, removeErr
	}
	if ensure := step.GetManagedNetworkEnsure(); ensure != nil {
		return executeManagedNetworkEnsure(ctx, taskRunner, owned.TimeoutSeconds, artifact, ensure)
	}
	if ensure := step.GetManagedVolumeEnsure(); ensure != nil {
		return executeManagedVolumeEnsure(ctx, taskRunner, owned.TimeoutSeconds, artifact, ensure)
	}
	if apply := step.GetComponentApply(); apply != nil {
		if owned.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			return nil, errs.New(
				errs.KindValidationFailed,
				"managed Component action is not a container helper procedure",
			)
		}
		if catalog == nil {
			return nil, errs.New(errs.KindValidationFailed, "Component action has no compiled helper catalog")
		}
		recipe, resolveErr := catalog.ResolveContainerConfigAction(apply)
		if resolveErr != nil {
			return nil, resolveErr
		}
		return executeComponentContainerConfigAction(
			ctx, taskRunner, owned.TimeoutSeconds, artifact, apply, recipe,
		)
	}
	if step.GetServiceProxySwitch() != nil || step.GetServiceProxyProbe() != nil ||
		step.GetServiceProxyCompensate() != nil {
		return executeServiceProxy(ctx, taskRunner, owned.TimeoutSeconds, owned.Plan, artifact, step)
	}
	if step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil {
		switch executionplan.RestorationTargetForService(owned.GetRestorationAuthority(), restorationStepServiceID(step)) {
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
			return executeServingPredecessor(ctx, taskRunner, owned.TimeoutSeconds, owned, artifact, step)
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
			return executeCandidateAbsence(ctx, taskRunner, owned.TimeoutSeconds, owned, artifact, step)
		default:
			return nil, errs.New(errs.KindValidationFailed, "candidate restoration member target is invalid")
		}
	}
	commands, err := commandsFor(owned, step, artifact)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(owned.TimeoutSeconds)*time.Second)
	defer cancel()
	for index, command := range commands {
		result, runErr := taskRunner.Run(executionCtx, command)
		if contextErr := executionCtx.Err(); contextErr != nil {
			return nil, contextErr
		}
		if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
			if runErr == nil {
				runErr = errors.New("process returned an invalid exit code")
			}
			return nil, errs.Wrap(errs.KindInternal, runErr)
		}
		if runErr != nil || result.ExitCode != 0 {
			diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
			if index == 0 && step.GetComposeApply() != nil {
				diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED
			}
			return &agentpb.ComposeHelperResponse{
				Schema:   SchemaVersion,
				Outcome:  agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
				ExitCode: int32(result.ExitCode), Diagnostic: diagnostic,
			}, nil
		}
		if index == 0 && needsManagedVolumeEnsure(step, artifact) {
			failure, ensureErr := ensureManagedComposeVolumes(executionCtx, taskRunner, artifact)
			if ensureErr != nil {
				return nil, ensureErr
			}
			if failure != nil {
				return failure, nil
			}
		}
	}
	response := &agentpb.ComposeHelperResponse{
		Schema:     SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
	if compensate := step.GetServiceRecreateCompensate(); compensate != nil {
		response.RecreateEvidence = &agentpb.ServiceRecreateEvidence{
			ServiceId:   compensate.ServiceId,
			ReleaseId:   compensate.PriorReleaseId,
			ArtifactId:  compensate.ArtifactId,
			Compensated: true,
			Target:      compensate.PriorTarget,
		}
	}
	return response, nil
}

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
	matches := 0
	for _, mount := range mounts {
		if mount.Destination != destination {
			continue
		}
		if mount.Type != "bind" || filepath.Clean(mount.Source) != filepath.Clean(source) || mount.RW {
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

func executeManagedNetworkRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	remove *agentpb.ManagedNetworkRemove,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	run := func(args ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
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
				"managed network command returned an invalid exit code",
			)
		}
		if runErr != nil && result.ExitCode == 0 {
			return runner.Result{}, nil, errs.Wrap(errs.KindInternal, runErr)
		}
		if runErr != nil || result.ExitCode != 0 {
			exitCode := result.ExitCode
			if exitCode == 0 {
				exitCode = 1
			}
			return result, failedResponse(int32(exitCode)), nil
		}
		return result, nil, nil
	}

	listed, failure, err := run(
		"network", "ls", "--filter", "name=^"+remove.DockerName+"$", "--format", "{{.Name}}",
	)
	if err != nil || failure != nil {
		return failure, err
	}
	found := false
	for _, name := range strings.Fields(string(listed.Stdout)) {
		if name == remove.DockerName {
			found = true
			break
		}
	}
	if !found {
		return completedResponse(), nil
	}

	inspected, failure, err := run(
		"network", "inspect", "--format", "{{json .Labels}}", remove.DockerName,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	labels := map[string]string{}
	if json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
		labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "network" ||
		labels["com.groundplane.environment-id"] != remove.EnvironmentId {
		return failedResponse(1), nil
	}

	connected, failure, err := run(
		"network", "inspect", "--format",
		`{{range $id, $_ := .Containers}}{{$id}}{{"\n"}}{{end}}`, remove.DockerName,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	containerIDs := strings.Fields(string(connected.Stdout))
	sort.Strings(containerIDs)
	for _, containerID := range containerIDs {
		if len(containerID) > 128 || strings.ContainsAny(containerID, "\x00/\\") {
			return nil, errs.New(errs.KindInternal, "managed network contains an invalid container identity")
		}
		_, failure, err = run("network", "disconnect", "--force", remove.DockerName, containerID)
		if err != nil || failure != nil {
			return failure, err
		}
	}
	_, failure, err = run("network", "rm", remove.DockerName)
	if err != nil || failure != nil {
		return failure, err
	}
	return completedResponse(), nil
}

func completedResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
}

func failedResponse(exitCode int32) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: exitCode, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
	}
}

func frame(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 || len(encoded) > maximumFramedBytes {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame length is invalid")
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	return framed, nil
}

func readFrame(ctx context.Context, input io.Reader) ([]byte, error) {
	if ctx == nil || input == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper frame input is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var header [frameHeaderBytes]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame header is incomplete")
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > maximumFramedBytes {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame length is invalid")
	}
	encoded := make([]byte, int(length))
	if _, err := io.ReadFull(input, encoded); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame payload is incomplete")
	}
	extra, err := io.ReadAll(io.LimitReader(input, 1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(extra) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame has trailing bytes")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encoded, nil
}
