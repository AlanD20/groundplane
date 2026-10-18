package entryexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// FileInput carries the already-authorized file stage, never a general command.
type FileInput struct {
	Attempt   workexecution.Attempt
	Procedure *agentpb.EntryConfigurationWorkInput
	Progress  Progress
	Recovery  *Recovery
}

func ConfigurationSelection(input FileInput) (*agentpb.WorkConfigurationReference, string, uint64, error) {
	if input.Recovery != nil {
		return recoveryConfigurationSelection(input)
	}
	if input.Progress.Validate(input.Attempt, input.Procedure) != nil || !input.Progress.InFlight ||
		(input.Progress.Stage != WriteConfiguration && input.Progress.Stage != VerifyConfiguration) ||
		input.Procedure.Configuration == nil {
		return nil, "", 0, invalidLayout()
	}
	checkpoint, err := input.Progress.Checkpoint(input.Attempt, input.Procedure)
	if err != nil {
		return nil, "", 0, err
	}
	return input.Procedure.Configuration, checkpoint.StageID, input.Procedure.RenderGeneration, nil
}

// ValidateConfigurationFiles binds the transient plaintext delivery to the
// granted write boundary. A different consumer, generation, mode, destination
// or payload cannot be substituted under an otherwise valid attempt.
func ValidateConfigurationFiles(selection FileInput, files []entrymaterialization.Header) error {
	if selection.Recovery != nil {
		return validateRecoveryConfigurationFiles(selection, files)
	}
	attempt, input, progress := selection.Attempt, selection.Procedure, selection.Progress
	if progress.Validate(attempt, input) != nil || !progress.InFlight ||
		(progress.Stage != WriteConfiguration && progress.Stage != VerifyConfiguration) || input.Configuration == nil || len(files) != len(input.Files) {
		return invalidLayout()
	}
	checkpoint, err := progress.Checkpoint(attempt, input)
	if err != nil {
		return err
	}
	for index, header := range files {
		file := input.Files[index]
		digest := header.Digest()
		if header.TaskID() != attempt.TaskID || header.StepID() != checkpoint.StageID ||
			header.EnvironmentID() != input.EnvironmentId || header.Generation() != input.RenderGeneration ||
			header.Destination() != file.Destination || header.ServiceID() != file.ServiceId ||
			header.ServiceName() != file.ServiceName || header.UID() != file.Uid || header.GID() != file.Gid ||
			uint32(header.Mode()) != file.Mode || header.Length() != file.Length ||
			!bytes.Equal(digest[:], file.Sha256) || !outputMatches(header.OutputKind(), file.OutputKind) {
			return invalidLayout()
		}
	}
	return nil
}

func outputMatches(kind entrymaterialization.OutputKind, wire agentpb.MaterializationOutputKind) bool {
	switch wire {
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV:
		return kind == entrymaterialization.OutputGeneratedEnv
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE:
		return kind == entrymaterialization.OutputPlainFile
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE:
		return kind == entrymaterialization.OutputSecretFile
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV:
		return kind == entrymaterialization.OutputRemoveGeneratedEnv
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE:
		return kind == entrymaterialization.OutputRemovePlainFile
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE:
		return kind == entrymaterialization.OutputRemoveSecretFile
	default:
		return false
	}
}
