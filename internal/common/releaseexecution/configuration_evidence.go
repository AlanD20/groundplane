package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

// ConfigurationProof attests verification of the complete captured selection.
// File contents and plaintext digests are not duplicated into execution history.
type ConfigurationProof struct {
	ID              string `json:"id"`
	EnvironmentID   string `json:"environment_id"`
	Generation      uint64 `json:"generation"`
	SHA256          string `json:"sha256"`
	SelectionSHA256 string `json:"selection_sha256"`
}

func ConfigurationResult(
	input Input,
	report CheckpointReport,
	files []entrymaterialization.Header,
) (EffectResult, error) {
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight || report.Progress.Phase != Configuration || input.Release.Configuration == nil ||
		len(files) != len(input.Release.ConfigurationDestinations) {
		return EffectResult{}, invalidProgress()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return EffectResult{}, invalidProgress()
	}
	reference := input.Release.Configuration
	for index, file := range files {
		if file.TaskID() != input.Attempt.TaskID || file.StepID() != stage ||
			file.EnvironmentID() != reference.EnvironmentId ||
			file.Generation() != reference.Generation ||
			file.Destination() != input.Release.ConfigurationDestinations[index] {
			return EffectResult{}, invalidProgress()
		}
	}
	return EffectResult{
		Configuration: &ConfigurationProof{ID: reference.ConfigurationId, EnvironmentID: reference.EnvironmentId,
			Generation: reference.Generation, SHA256: hex.EncodeToString(reference.Sha256), SelectionSHA256: configurationSelection(input.Release.ConfigurationDestinations)},
	}, nil
}

func (proof ConfigurationProof) validate(input Input, progress Progress) error {
	reference := input.Release.Configuration
	if progress.Phase != Configuration || reference == nil || proof.ID != reference.ConfigurationId ||
		proof.EnvironmentID != reference.EnvironmentId ||
		proof.Generation != reference.Generation ||
		proof.SHA256 != hex.EncodeToString(reference.Sha256) ||
		proof.SelectionSHA256 != configurationSelection(input.Release.ConfigurationDestinations) {
		return invalidProgress()
	}
	return nil
}

func configurationSelection(destinations []string) string {
	raw, _ := json.Marshal(destinations)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
