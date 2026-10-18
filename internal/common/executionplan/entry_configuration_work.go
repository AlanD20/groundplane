package executionplan

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateEntryConfigurationWorkInput binds files and optional runtime effects
// to the Controller's captured Environment. It accepts no step program, image
// selection, dependency startup or whole-project reconciliation authority.
func ValidateEntryConfigurationWorkInput(input *agentpb.EntryConfigurationWorkInput) error {
	if input == nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(ids.KindOperation, input.OperationId) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil ||
		input.RenderGeneration == 0 || len(input.Files) == 0 || len(input.Files) > 512 || len(input.RunningConsumerIds) > 512 {
		return invalidEntryConfigurationWork()
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	for _, artifact := range []*agentpb.ComposeArtifact{input.Baseline, input.Candidate} {
		if err := validateArtifactContent(artifact); err != nil {
			return err
		}
		if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.OwnerId != input.EnvironmentId {
			return invalidEntryConfigurationWork()
		}
	}
	if input.Baseline.ArtifactId == input.Candidate.ArtifactId ||
		!entryArtifactRuntimeEqual(input.Baseline, input.Candidate) ||
		input.Removal && len(input.RunningConsumerIds) != 0 {
		return invalidEntryConfigurationWork()
	}
	if (len(input.Files) != 0) != (input.Configuration != nil) {
		return invalidEntryConfigurationWork()
	}
	for _, reference := range []*agentpb.WorkConfigurationReference{input.Configuration, input.PriorConfiguration} {
		if reference != nil && (reference.EnvironmentId != input.EnvironmentId || reference.Generation == 0 ||
			reference.Generation > input.RenderGeneration || len(reference.Sha256) != sha256.Size) {
			return invalidEntryConfigurationWork()
		}
	}
	if input.Configuration != nil && input.Configuration.Generation != input.RenderGeneration {
		return invalidEntryConfigurationWork()
	}
	artifacts := map[string]*agentpb.ComposeArtifact{input.Candidate.ArtifactId: input.Candidate}
	previous := ""
	materializations := make(map[string]bool, len(input.Files))
	for _, file := range input.Files {
		if file == nil || file.Destination <= previous || file.ArtifactId != input.Candidate.ArtifactId ||
			file.EnvironmentId != input.EnvironmentId || materializations[file.MaterializationId] {
			return invalidEntryConfigurationWork()
		}
		if err := validateMaterializeFile(input.RenderGeneration, file, artifacts); err != nil {
			return err
		}
		materializations[file.MaterializationId] = true
		previous = file.Destination
	}
	previous = ""
	for _, id := range input.RunningConsumerIds {
		if ids.Validate(ids.KindService, id) != nil || id <= previous || len(input.Files) == 0 {
			return invalidEntryConfigurationWork()
		}
		found := false
		for _, service := range input.Candidate.Services {
			if service.ServiceId == id && service.OwnerComponentId == "" &&
				service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				found = true
				break
			}
		}
		if !found {
			return invalidEntryConfigurationWork()
		}
		previous = id
	}
	return nil
}

func CaptureEntryConfigurationWorkInput(inputID string, input *agentpb.EntryConfigurationWorkInput) (
	*agentpb.EntryConfigurationWorkInput, []byte, workexecution.Input, error,
) {
	if err := ValidateEntryConfigurationWorkInput(input); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	owned := proto.CloneOf(input)
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(digest[:])}
	if err := reference.Validate(); err != nil {
		clear(raw)
		return nil, nil, workexecution.Input{}, err
	}
	return owned, raw, reference, nil
}

func OpenEntryConfigurationWorkInput(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) (
	*agentpb.EntryConfigurationWorkInput, error,
) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || attempt.FinishedAt != nil {
		return nil, invalidEntryConfigurationWork()
	}
	owned, raw, reference, err := CaptureEntryConfigurationWorkInput(attempt.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "Entry configuration assignment differs from captured input")
	}
	return owned, nil
}

func invalidEntryConfigurationWork() error {
	return errs.New(errs.KindValidationFailed, "Entry configuration requires captured files and exact running consumers; removal cannot restart workloads")
}
