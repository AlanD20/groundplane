package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateComponentWorkInput confines host effects to one Environment singleton.
// Recovery selects the captured predecessor, never current desired state.
func ValidateComponentWorkInput(input *agentpb.ComponentWorkInput) error {
	if input == nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(ids.KindOperation, input.OperationId) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil ||
		ids.Validate(ids.KindComponent, input.ComponentId) != nil || input.RenderGeneration == 0 ||
		input.Predecessor == nil || len(input.Files) > 512 {
		return invalidComponentWork()
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	if input.Remove {
		if input.Candidate != nil {
			return invalidComponentWork()
		}
	} else if err := validateComponentWorkArtifact(input.Candidate, input.EnvironmentId, input.ComponentId); err != nil {
		return err
	}
	prior := input.Predecessor
	if prior.Absent {
		if prior.Runtime != nil || prior.Configuration != nil {
			return invalidComponentWork()
		}
	} else if err := validateComponentWorkArtifact(prior.Runtime, input.EnvironmentId, input.ComponentId); err != nil {
		return err
	}
	if (len(input.Files) != 0) != (input.Configuration != nil) || input.Remove && (len(input.Files) != 0 || input.ConfigurationAction != nil) {
		return invalidComponentWork()
	}
	for _, reference := range []*agentpb.WorkConfigurationReference{input.Configuration, input.PriorConfiguration} {
		if reference != nil && (ids.Validate(ids.KindConfig, reference.ConfigurationId) != nil ||
			reference.EnvironmentId != input.EnvironmentId || reference.Generation == 0 ||
			reference.Generation > input.RenderGeneration || len(reference.Sha256) != sha256.Size) {
			return invalidComponentWork()
		}
	}
	if input.Configuration != nil && input.Configuration.Generation != input.RenderGeneration {
		return invalidComponentWork()
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, 1)
	if input.Candidate != nil {
		artifacts[input.Candidate.ArtifactId] = input.Candidate
	}
	previous := ""
	for _, file := range input.Files {
		if file == nil || file.Destination <= previous || file.ArtifactId != input.Candidate.ArtifactId ||
			file.EnvironmentId != input.EnvironmentId {
			return invalidComponentWork()
		}
		if err := validateMaterializeFile(input.RenderGeneration, file, artifacts); err != nil {
			return err
		}
		previous = file.Destination
	}
	if action := input.ConfigurationAction; action != nil {
		if err := validateComponentAction(action); err != nil {
			return err
		}
		if action.ComponentId != input.ComponentId || action.Generation != input.RenderGeneration ||
			input.Configuration == nil {
			return invalidComponentWork()
		}
	}
	return validateComponentWorkRetainedConfiguration(input)
}

func validateComponentWorkArtifact(artifact *agentpb.ComposeArtifact, environmentID, componentID string) error {
	if err := validateArtifactContent(artifact); err != nil {
		return err
	}
	if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != environmentID ||
		len(artifact.Services) != 1 || artifact.Services[0] == nil || artifact.Services[0].OwnerComponentId != componentID ||
		artifact.Services[0].ExpectedReplicas != 1 {
		return invalidComponentWork()
	}
	return nil
}

func validateComponentWorkRetainedConfiguration(input *agentpb.ComponentWorkInput) error {
	retained := input.Predecessor.Configuration
	if retained == nil {
		return nil
	}
	if retained.File == nil || !validComponentDigest(retained.DefinitionDigest) ||
		!validComponentDigest(retained.CatalogDigest) || !validComponentActionID(retained.ActionId) ||
		retained.Generation == 0 || retained.Generation > input.RenderGeneration ||
		retained.File.EnvironmentId != input.EnvironmentId || input.PriorConfiguration == nil {
		return invalidComponentWork()
	}
	digest := sha256.Sum256(retained.Source)
	if !bytes.Equal(digest[:], retained.File.Sha256) || uint64(len(retained.Source)) != retained.File.Length ||
		retained.Generation != input.PriorConfiguration.Generation {
		return invalidComponentWork()
	}
	return validateMaterializeFile(retained.Generation, retained.File,
		map[string]*agentpb.ComposeArtifact{input.Predecessor.Runtime.ArtifactId: input.Predecessor.Runtime})
}

func CaptureComponentWorkInput(inputID string, input *agentpb.ComponentWorkInput) (*agentpb.ComponentWorkInput, []byte, workexecution.Input, error) {
	if err := ValidateComponentWorkInput(input); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	owned := proto.Clone(input).(*agentpb.ComponentWorkInput)
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

func invalidComponentWork() error {
	return errs.New(errs.KindValidationFailed, "Component work requires one owned runtime and its captured configuration and predecessor")
}

// OpenComponentWorkInput binds authenticated assignment content to the attempt.
// Recovery additionally requires its operation-owned failure/boundary evidence.
func OpenComponentWorkInput(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) (*agentpb.ComponentWorkInput, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || attempt.FinishedAt != nil {
		return nil, invalidComponentWork()
	}
	owned, raw, reference, err := CaptureComponentWorkInput(attempt.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "Component assignment differs from its captured input")
	}
	return owned, nil
}
