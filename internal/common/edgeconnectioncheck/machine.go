package edgeconnectioncheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	TaskTimeoutSeconds   = 90
	ExchangeTimeout      = PrivateTimeout + 25*time.Second
	MaximumEnvelopeBytes = 528 << 10
	MaximumResultBytes   = MaximumOutputBytes + 1024
	ExecutionFailure     = "execution_failed"
	AgentUnavailable     = "agent_unavailable"
	ConfigurationChanged = "configuration_changed"
	CleanupFailure       = "cleanup"
)

func HelperName(taskID string) string { return "gp-edge-check-" + taskID }

// Each exchange gets a fresh raw request ID. The helper Task and parameters'
// operation ID stay fixed through cleanup Retry; a late probe response cannot
// satisfy a newer cleanup exchange.
func ValidateMachineRequest(request *agentpb.CheckEdgeConnections) error {
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		ids.Validate(
			ids.KindOperation,
			"op_"+request.RequestId,
		) != nil || ids.Validate(ids.KindTask, request.TaskId) != nil ||
		!imageref.IsDigestPinned(request.HelperImage) || !validContainerID(request.ConnectorContainerId) ||
		proto.Size(request) > MaximumEnvelopeBytes || request.Action == nil || request.Runtime == nil ||
		proto.Size(request.Action) > 4096 || proto.Size(request.Runtime) > 512<<10 {
		return invalidMachine()
	}
	parameters, err := ReadRequest(bytes.NewReader(request.ParametersJson))
	if err != nil {
		return err
	}
	return ValidateCapturedRuntime(parameters, request.Runtime, request.Action)
}

// ValidateCapturedRuntime binds the shared wire/storage authority. The Agent
// additionally authenticates the action and image through its compiled catalog.
func ValidateCapturedRuntime(
	parameters Request,
	artifact *agentpb.ComposeArtifact,
	action *agentpb.ComponentApply,
) error {
	if parameters.Validate() != nil || artifact == nil || action == nil ||
		executionplan.RejectUnknown(artifact) != nil || executionplan.RejectUnknown(action) != nil ||
		proto.Size(artifact) > 512<<10 || proto.Size(action) > 4096 ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != parameters.EnvironmentID ||
		artifact.ProjectName == "" || len(artifact.ProjectName) > 128 || len(artifact.CanonicalYaml) == 0 ||
		action.ComponentId != parameters.ComponentID || action.ArtifactId != artifact.ArtifactId ||
		ids.Validate(ids.KindConfig, artifact.ArtifactId) != nil || action.Generation == 0 ||
		len(action.DefinitionDigest) != sha256.Size || len(action.CatalogDigest) != sha256.Size ||
		len(action.ArtifactDigest) != sha256.Size || action.ActionId == "" || len(action.ActionId) > 128 ||
		action.ManagedConfigContent || len(action.ExpectedPreviousArtifactDigest) != 0 ||
		action.ExpectedPreviousArtifactId != "" || action.ExpectedPreviousGeneration != 0 ||
		!bytes.Equal(
			action.ArtifactDigest,
			artifact.YamlSha256,
		) || hex.EncodeToString(artifact.YamlSha256) != parameters.RuntimeSHA256 {
		return invalidMachine()
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if hex.EncodeToString(digest[:]) != parameters.RuntimeSHA256 {
		return invalidMachine()
	}
	selected := 0
	for _, service := range artifact.Services {
		if service == nil {
			return invalidMachine()
		}
		if service.OwnerComponentId == parameters.ComponentID {
			if service.ServiceId != parameters.ServiceID || service.ExpectedReplicas != 1 {
				return invalidMachine()
			}
			selected++
		}
	}
	if selected != 1 {
		return invalidMachine()
	}
	return nil
}

func ValidateMachineCancel(request *agentpb.CancelEdgeConnections) error {
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil {
		return invalidMachine()
	}
	return nil
}

func ValidateMachineResult(request *agentpb.CheckEdgeConnections, result *agentpb.EdgeConnectionResult) error {
	if ValidateMachineRequest(request) != nil || result == nil || executionplan.RejectUnknown(result) != nil ||
		proto.Size(
			result,
		) > MaximumResultBytes || result.RequestId != request.RequestId || result.TaskId != request.TaskId ||
		result.ConnectorContainerId != request.ConnectorContainerId || result.HelperName != HelperName(request.TaskId) ||
		result.CleanupOnly != request.CleanupOnly || result.CleanupRequired && result.Failure == "" {
		return invalidMachine()
	}
	switch result.Failure {
	case "",
		ExecutionFailure,
		AgentUnavailable,
		ConfigurationChanged,
		CleanupFailure,
		string(TimeoutFailure),
		string(Canceled):
	default:
		return invalidMachine()
	}
	if request.CleanupOnly {
		if len(result.PrivateResultJson) != 0 {
			return invalidMachine()
		}
		return nil
	}
	if len(result.PrivateResultJson) == 0 {
		if result.Failure == "" {
			return invalidMachine()
		}
		return nil
	}
	parameters, err := ReadRequest(bytes.NewReader(request.ParametersJson))
	if err != nil {
		return err
	}
	_, err = ReadPrivateResult(bytes.NewReader(result.PrivateResultJson), parameters)
	return err
}

func MachineFailure(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return string(TimeoutFailure)
	}
	if errors.Is(err, context.Canceled) {
		return string(Canceled)
	}
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindStateConflict:
		return ConfigurationChanged
	case errs.KindStorageUnavailable:
		return AgentUnavailable
	default:
		return ExecutionFailure
	}
}

func validContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func invalidMachine() error {
	return errs.New(errs.KindValidationFailed, "Tunnel diagnostic machine message is invalid")
}
