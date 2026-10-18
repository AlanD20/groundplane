package componentexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ValidateConfigurationFiles(input HelperInput, files []entrymaterialization.Header) error {
	if input.Recovery != nil {
		return ValidateRecoveryConfigurationFiles(input, files)
	}
	if input.Recovery != nil || input.Procedure == nil || input.Procedure.Configuration == nil ||
		input.Progress.Validate(input.Attempt, input.Procedure) != nil || !input.Progress.InFlight ||
		input.Progress.Stage != WriteConfiguration || len(files) != len(input.Procedure.Files) {
		return invalidProgress()
	}
	checkpoint, err := input.Progress.Checkpoint(input.Attempt, input.Procedure)
	if err != nil {
		return err
	}
	for index, header := range files {
		file := input.Procedure.Files[index]
		digest := header.Digest()
		if header.TaskID() != input.Attempt.TaskID || header.StepID() != checkpoint.StageID ||
			header.EnvironmentID() != file.EnvironmentId || header.Generation() != input.Procedure.Configuration.Generation ||
			header.Destination() != file.Destination || header.ServiceID() != file.ServiceId || header.ServiceName() != file.ServiceName ||
			header.UID() != file.Uid || header.GID() != file.Gid || uint32(header.Mode()) != file.Mode ||
			header.Length() != file.Length || !bytes.Equal(digest[:], file.Sha256) || !componentOutputMatches(header.OutputKind(), file.OutputKind) {
			return invalidProgress()
		}
	}
	return nil
}

func componentOutputMatches(kind entrymaterialization.OutputKind, wire agentpb.MaterializationOutputKind) bool {
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
	}
	return false
}
