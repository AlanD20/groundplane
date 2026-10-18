// Package backingprovision owns the closed host procedure for creating a shared
// backing Service. It contains no persistence, provider selection or host I/O.
package backingprovision

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const MaximumInputBytes = 512 * 1024

func ValidateInput(planID string, input *agentpb.BackingProvisionInput) error {
	if input == nil || proto.Size(input) > MaximumInputBytes || executionplan.RejectUnknown(input) != nil ||
		ids.Validate(ids.KindTask, input.TaskId) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemId) != nil ||
		ids.Validate(ids.KindOperation, input.OperationId) != nil || ids.Validate(ids.KindService, input.ServiceId) != nil ||
		ids.Validate(ids.KindHost, input.HostId) != nil || input.CreatedAt == nil || input.CreatedAt.CheckValid() != nil ||
		input.CreatedAt.AsTime().IsZero() || input.Deadline == nil || input.Deadline.CheckValid() != nil ||
		!input.Deadline.AsTime().After(input.CreatedAt.AsTime()) || input.Deadline.AsTime().Sub(input.CreatedAt.AsTime()) > 24*time.Hour ||
		input.RenderGeneration != 1 || len(input.ManagedVolumeIds) > 1 || len(input.Files) > 1 {
		return invalid("backing creation input is invalid or exceeds its facade limits")
	}
	scope, err := environmentpath.Parse(input.VolumeRoot, input.VolumeDirectory)
	if err != nil {
		return err
	}
	if scope.ProjectKind != environmentpath.ProjectKindBacking || scope.ProjectID != input.ProjectId || scope.EnvironmentID != input.EnvironmentId {
		return invalid("backing creation directory differs from its captured Project and Environment")
	}
	if err := executionplan.ValidateCapturedArtifact(planID, input.RenderGeneration, input.Runtime); err != nil {
		return err
	}
	runtime := input.Runtime
	if runtime.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || runtime.OwnerId != input.EnvironmentId ||
		runtime.AuthorizedVolumeDir != input.VolumeDirectory || len(runtime.Services) != 1 || len(runtime.Networks) != 1 ||
		len(runtime.Volumes) != len(input.ManagedVolumeIds) {
		return invalid("backing creation runtime must contain its one Service and shared Zone")
	}
	service := runtime.Services[0]
	if service.ServiceId != input.ServiceId || service.OwnerComponentId != "" || service.ExpectedReplicas != 1 ||
		service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
		return invalid("backing creation runtime differs from its captured singleton Service")
	}
	for index, id := range input.ManagedVolumeIds {
		if ids.Validate(ids.KindVolume, id) != nil || runtime.Volumes[index].VolumeId != id ||
			index > 0 && input.ManagedVolumeIds[index-1] >= id {
			return invalid("backing creation Volume ownership is invalid")
		}
	}
	if (len(input.Files) != 0) != (input.Configuration != nil) {
		return invalid("backing creation files require their captured configuration")
	}
	if configuration := input.Configuration; configuration != nil &&
		(ids.Validate(ids.KindConfig, configuration.ConfigurationId) != nil || configuration.EnvironmentId != input.EnvironmentId ||
			configuration.Generation != input.RenderGeneration || len(configuration.Sha256) != sha256.Size) {
		return invalid("backing creation configuration reference is invalid")
	}
	if err := executionplan.ValidateCapturedMaterializations(input.RenderGeneration, runtime, input.Files); err != nil {
		return err
	}
	for _, file := range input.Files {
		if file.ServiceId != input.ServiceId || file.OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV {
			return invalid("backing creation can materialize only its Service environment file")
		}
	}
	if hook := input.AfterStart; hook != nil {
		definition := backinghook.Definition{Command: slices.Clone(hook.Command), TimeoutSeconds: hook.TimeoutSeconds}
		if err := backinghook.ValidateConfiguration(backinghook.Configuration{AfterStart: &definition}); err != nil {
			return err
		}
		if reference := hook.EncryptedInputs; reference != nil &&
			(reference.InputId != input.OperationId || len(reference.Sha256) != sha256.Size) {
			return invalid("backing creation hook values differ from its captured operation")
		}
	}
	return nil
}

func Reference(planID string, input *agentpb.BackingProvisionInput) (workexecution.Input, error) {
	if err := ValidateInput(planID, input); err != nil {
		return workexecution.Input{}, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(input)
	if err != nil {
		return workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: planID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func Open(attempt workexecution.Attempt, input *agentpb.BackingProvisionInput) (*agentpb.BackingProvisionInput, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		attempt.FinishedAt != nil || input == nil {
		return nil, invalid("backing creation requires its active Agent assignment")
	}
	reference, err := Reference(attempt.Input.ID, input)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input || input.TaskId != attempt.TaskID || input.WorkItemId != attempt.WorkItemID ||
		input.HostId != attempt.Executor.HostID || attempt.AssignedAt.Before(input.CreatedAt.AsTime()) {
		return nil, invalid("backing creation assignment differs from its captured input or host")
	}
	return proto.CloneOf(input), nil
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
