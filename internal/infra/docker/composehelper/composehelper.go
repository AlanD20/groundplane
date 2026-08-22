// Package composehelper owns the task-scoped Docker Compose helper protocol
// and its exact CLI procedure. It never owns task policy or observed state.
package composehelper

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion      = 1
	WorkDirectory      = "/run/groundplane/compose"
	DockerExecutable   = "docker"
	maximumFramedBytes = executionplan.MaximumPlanBytes + 64*1024
	frameHeaderBytes   = 4
	maximumTimeout     = uint32(math.MaxInt32)
)

var fixedEnvironment = []string{
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	"HOME=/nonexistent",
	"DOCKER_HOST=unix:///var/run/docker.sock",
	"COMPOSE_DISABLE_ENV_FILE=1",
}

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
	if taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper Runner is required")
	}
	owned, step, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if remove := step.GetManagedNetworkRemove(); remove != nil {
		return executeManagedNetworkRemove(ctx, taskRunner, owned.TimeoutSeconds, remove)
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
	}
	return &agentpb.ComposeHelperResponse{
		Schema:     SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}, nil
}

func validateRequest(
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperRequest, *agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	if request == nil || request.Schema != SchemaVersion {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper request schema is unsupported")
	}
	if err := executionplan.RejectUnknown(request); err != nil {
		return nil, nil, nil, err
	}
	if ids.Validate(ids.KindTask, request.TaskId) != nil ||
		ids.Validate(ids.KindOperation, request.OperationId) != nil {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper task identity is invalid")
	}
	if request.TimeoutSeconds == 0 || request.TimeoutSeconds > maximumTimeout {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper timeout is invalid")
	}
	plan, err := executionplan.Validate(request.Plan)
	if err != nil {
		return nil, nil, nil, err
	}
	var selected *agentpb.ExecutionStep
	for _, step := range plan.Steps {
		if step.StepId == request.StepId {
			selected = step
			break
		}
	}
	if selected == nil || request.TimeoutSeconds > selected.TimeoutSeconds {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper step selection is invalid")
	}
	artifactID := ""
	switch payload := selected.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		artifactID = payload.ComposeApply.ArtifactId
	case *agentpb.ExecutionStep_ComposeStop:
		artifactID = payload.ComposeStop.ArtifactId
	case *agentpb.ExecutionStep_ComposeRemove:
		artifactID = payload.ComposeRemove.ArtifactId
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		artifactID = ""
	default:
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper step payload is unsupported")
	}
	var artifact *agentpb.ComposeArtifact
	for _, candidate := range plan.Artifacts {
		if candidate.ArtifactId == artifactID {
			artifact = candidate
			break
		}
	}
	if artifactID != "" && artifact == nil {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper artifact selection is invalid")
	}
	owned := proto.Clone(request).(*agentpb.ComposeHelperRequest)
	owned.Plan = plan
	for _, step := range owned.Plan.Steps {
		if step.StepId == request.StepId {
			selected = step
			break
		}
	}
	for _, candidate := range owned.Plan.Artifacts {
		if candidate.ArtifactId == artifactID {
			artifact = candidate
			break
		}
	}
	return owned, selected, artifact, nil
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

func commandsFor(
	request *agentpb.ComposeHelperRequest,
	step *agentpb.ExecutionStep,
	artifact *agentpb.ComposeArtifact,
) ([]runner.RunCmdOpts, error) {
	prefix := []string{
		"compose", "--project-name", artifact.ProjectName,
		"--project-directory", WorkDirectory, "--file", "-",
	}
	base := runner.RunCmdOpts{
		Name: DockerExecutable, Dir: WorkDirectory,
		Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		Stdin: append([]byte(nil), artifact.CanonicalYaml...),
	}
	var commands []runner.RunCmdOpts
	if apply := step.GetComposeApply(); apply != nil {
		validation := base
		validation.Args = append(append([]string(nil), prefix...), "config", "--quiet", "--no-interpolate")
		commands = append(commands, validation)
		mutation := base
		mutation.Args = append(append([]string(nil), prefix...), "up", "--detach")
		if apply.FullReconcile {
			mutation.Args = append(mutation.Args, "--remove-orphans")
		} else {
			mutation.Args = append(mutation.Args, serviceNames(artifact, apply.ServiceIds)...)
		}
		commands = append(commands, mutation)
		return commands, nil
	}
	mutation := base
	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeStop:
		mutation.Args = append(append([]string(nil), prefix...),
			"stop", "--timeout", strconv.FormatUint(uint64(payload.ComposeStop.GraceSeconds), 10))
		mutation.Args = append(mutation.Args, serviceNames(artifact, payload.ComposeStop.ServiceIds)...)
	case *agentpb.ExecutionStep_ComposeRemove:
		if payload.ComposeRemove.WholeProject {
			mutation.Args = append(append([]string(nil), prefix...), "down", "--remove-orphans")
		} else {
			mutation.Args = append(append([]string(nil), prefix...), "rm", "--stop", "--force")
			mutation.Args = append(mutation.Args, serviceNames(artifact, payload.ComposeRemove.ServiceIds)...)
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "Compose helper step payload is unsupported")
	}
	return []runner.RunCmdOpts{mutation}, nil
}

func serviceNames(artifact *agentpb.ComposeArtifact, selected []string) []string {
	namesByID := make(map[string]string, len(artifact.Services))
	for _, service := range artifact.Services {
		namesByID[service.ServiceId] = service.ComposeName
	}
	names := make([]string, len(selected))
	for index, serviceID := range selected {
		names[index] = namesByID[serviceID]
	}
	return names
}

func validateResponse(response *agentpb.ComposeHelperResponse) error {
	if response == nil || response.Schema != SchemaVersion {
		return errs.New(errs.KindValidationFailed, "Compose helper response schema is unsupported")
	}
	if err := executionplan.RejectUnknown(response); err != nil {
		return err
	}
	switch response.Outcome {
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED:
		if response.ExitCode != 0 ||
			response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
			return errs.New(errs.KindValidationFailed, "completed Compose helper response is inconsistent")
		}
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED:
		if response.ExitCode <= 0 ||
			(response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED &&
				response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED) {
			return errs.New(errs.KindValidationFailed, "failed Compose helper response is inconsistent")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Compose helper response outcome is unsupported")
	}
	return nil
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
