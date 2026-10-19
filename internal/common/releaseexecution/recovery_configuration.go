package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryConfigurationReference is selected by the captured recovery decision,
// never by the current Environment head. Preserving a proved serving candidate
// verifies its files; restoration uses the exact pre-operation snapshot.
func (input RecoveryInput) RecoveryConfigurationReference() *agentpb.WorkConfigurationReference {
	if input.PreservesServing() {
		return input.Input.Release.Configuration
	}
	return input.Input.Release.PriorConfiguration
}

func DecodeRecoveryConfigurationFile(
	input RecoveryInput,
	report RecoveryReport,
	file *agentpb.ReleaseConfigurationFile,
) (entrymaterialization.Header, error) {
	if file == nil || executionplan.RejectUnknown(file) != nil || input.Validate() != nil ||
		len(
			file.Sha256,
		) != sha256.Size || file.Length > entrymaterialization.MaximumContentBytes || input.Input.Release.Configuration == nil {
		return entrymaterialization.Header{}, invalidProgress()
	}
	generation := input.Input.Release.Configuration.Generation
	if selected := input.RecoveryConfigurationReference(); selected != nil {
		generation = selected.Generation
	}
	kind, err := entrymaterialization.ParseOutputKind(file.OutputKind)
	if err != nil {
		return entrymaterialization.Header{}, err
	}
	var digest entrymaterialization.Digest
	copy(digest[:], file.Sha256)
	header, err := entrymaterialization.NewHeader(
		entrymaterialization.HeaderSpec{TaskID: input.Input.Attempt.TaskID, StepID: report.StageID,
			EnvironmentID: input.Input.Release.EnvironmentId, Generation: generation, Destination: file.Destination, ServiceID: file.ServiceId,
			ServiceName: file.ServiceName, OutputKind: kind, UID: file.Uid, GID: file.Gid, Mode: entrymaterialization.Mode(file.Mode), Length: file.Length, Digest: digest},
	)
	if err != nil {
		return entrymaterialization.Header{}, err
	}
	if err := ValidateRecoveryFile(input, report, header); err != nil {
		return entrymaterialization.Header{}, err
	}
	return header, nil
}

// RecoveryConfigurationProof distinguishes captured absence from a restored
// snapshot. It contains no file contents or individual plaintext digests.
type RecoveryConfigurationProof struct {
	Absent          bool                `json:"absent"`
	Configuration   *ConfigurationProof `json:"configuration,omitempty"`
	SelectionSHA256 string              `json:"selection_sha256"`
}

func (proof RecoveryConfigurationProof) Validate(input RecoveryInput) error {
	if input.Validate() != nil ||
		proof.SelectionSHA256 != configurationSelection(input.Input.Release.ConfigurationDestinations) {
		return invalidProgress()
	}
	reference := input.RecoveryConfigurationReference()
	if reference == nil {
		if !proof.Absent || proof.Configuration != nil {
			return invalidProgress()
		}
		return nil
	}
	if proof.Absent || proof.Configuration == nil || *proof.Configuration != (ConfigurationProof{
		ID: reference.ConfigurationId, EnvironmentID: reference.EnvironmentId, Generation: reference.Generation,
		SHA256: hex.EncodeToString(reference.Sha256), SelectionSHA256: proof.SelectionSHA256}) {
		return invalidProgress()
	}
	return nil
}

func ValidateRecoveryFile(input RecoveryInput, report RecoveryReport, header entrymaterialization.Header) error {
	if report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight ||
		(report.Progress.Phase != RestoreConfiguration && report.Progress.Phase != VerifyRestoration) ||
		header.TaskID() != input.Input.Attempt.TaskID || header.StepID() != report.StageID ||
		header.EnvironmentID() != input.Input.Release.EnvironmentId ||
		!slices.Contains(input.Input.Release.ConfigurationDestinations, header.Destination()) {
		return invalidProgress()
	}
	reference := input.RecoveryConfigurationReference()
	if reference != nil {
		if header.Generation() != reference.Generation {
			return invalidProgress()
		}
		return nil
	}
	// No previous snapshot exists: every selected destination must be absent.
	// The candidate's captured generation identifies these removal descriptors;
	// it does not select candidate contents for restoration.
	if input.Input.Release.Configuration == nil || header.Generation() != input.Input.Release.Configuration.Generation {
		return invalidProgress()
	}
	switch header.OutputKind() {
	case entrymaterialization.OutputRemoveGeneratedEnv,
		entrymaterialization.OutputRemovePlainFile,
		entrymaterialization.OutputRemoveSecretFile:
		return nil
	default:
		return invalidProgress()
	}
}

func RecoveryConfigurationResult(
	input RecoveryInput,
	report RecoveryReport,
	files []entrymaterialization.Header,
) (RecoveryConfigurationProof, error) {
	if report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight ||
		(report.Progress.Phase != RestoreConfiguration && report.Progress.Phase != VerifyRestoration) ||
		len(files) != len(input.Input.Release.ConfigurationDestinations) {
		return RecoveryConfigurationProof{}, invalidProgress()
	}
	for index, file := range files {
		if ValidateRecoveryFile(input, report, file) != nil ||
			file.Destination() != input.Input.Release.ConfigurationDestinations[index] {
			return RecoveryConfigurationProof{}, invalidProgress()
		}
	}
	selection := configurationSelection(input.Input.Release.ConfigurationDestinations)
	proof := RecoveryConfigurationProof{
		Absent:          input.RecoveryConfigurationReference() == nil,
		SelectionSHA256: selection,
	}
	if reference := input.RecoveryConfigurationReference(); reference != nil {
		proof.Configuration = &ConfigurationProof{ID: reference.ConfigurationId, EnvironmentID: reference.EnvironmentId,
			Generation: reference.Generation, SHA256: hex.EncodeToString(reference.Sha256), SelectionSHA256: selection}
	}
	return proof, nil
}
