package componentexecution

import (
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strings"
)

// RecoveryConfigurationStage identifies only restoration of this attempt's
// captured file set. It is not a new forward execution grant.
func RecoveryConfigurationStage(attempt workexecution.Attempt) (string, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || attempt.StartedAt == nil {
		return "", invalidProgress()
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindStep, at, attempt.ID, "component/restore-configuration"), nil
}

func ConfigurationSelection(input HelperInput) (*agentpb.WorkConfigurationReference, string, uint64, error) {
	if input.Procedure == nil {
		return nil, "", 0, invalidProgress()
	}
	if input.Recovery != nil {
		if input.Recovery.ValidateFor(input.Attempt, input.Procedure) != nil ||
			input.Recovery.Failure.Checkpoint == nil {
			return nil, "", 0, invalidProgress()
		}
		checkpoint, err := input.Progress.Checkpoint(input.Recovery.Forward, input.Procedure)
		if err != nil || checkpoint != *input.Recovery.Failure.Checkpoint {
			return nil, "", 0, invalidProgress()
		}
		stage, err := RecoveryConfigurationStage(input.Attempt)
		return input.Procedure.PriorConfiguration, stage, input.Procedure.RenderGeneration, err
	}
	if input.Procedure.Configuration == nil || !input.Progress.InFlight || input.Progress.Stage != WriteConfiguration {
		return nil, "", 0, invalidProgress()
	}
	checkpoint, err := input.Progress.Checkpoint(input.Attempt, input.Procedure)
	return input.Procedure.Configuration, checkpoint.StageID, input.Procedure.Configuration.Generation, err
}

func ValidateRecoveryConfigurationFiles(input HelperInput, files []entrymaterialization.Header) error {
	_, stage, generation, err := ConfigurationSelection(input)
	if err != nil || input.Recovery == nil || len(files) != len(input.Procedure.Files) {
		return invalidProgress()
	}
	for index, header := range files {
		changed := input.Procedure.Files[index]
		if header.TaskID() != input.Attempt.TaskID || header.StepID() != stage || header.Generation() != generation ||
			header.EnvironmentID() != input.Procedure.EnvironmentId || header.Destination() != changed.Destination {
			return invalidProgress()
		}
		// The prior source can change content and file mode, but cannot turn
		// an ordinary file into a Secret (or vice versa) at this boundary.
		kind := strings.TrimPrefix(header.OutputKind().String(), "remove_")
		parsed, parseErr := entrymaterialization.ParseOutputKind(kind)
		if parseErr != nil {
			return invalidProgress()
		}
		if !componentOutputMatches(parsed, changed.OutputKind) {
			removed, parseErr := entrymaterialization.ParseOutputKind("remove_" + kind)
			if parseErr != nil || !componentOutputMatches(removed, changed.OutputKind) {
				return invalidProgress()
			}
		}
	}
	return nil
}
