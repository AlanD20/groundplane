package hierarchydeletion

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// EnvironmentResult retains the assignment authority needed to verify its final
// checkpoint after attempt history has been collected. It contains no Compose
// document or secret payload.
type EnvironmentResult struct {
	Recovery *hierarchyexecution.EnvironmentRecovery `json:"recovery,omitempty"`
	Attempt  workexecution.Attempt                   `json:"attempt"`
	Report   hierarchyexecution.EnvironmentReport    `json:"report"`
}

func EncodeEnvironmentResult(result EnvironmentResult) ([]byte, error) {
	if result.Attempt.Validate() != nil || result.Attempt.StartedAt == nil || result.Attempt.FinishedAt != nil ||
		result.Report.Progress.Phase != hierarchyexecution.EnvironmentRemoved {
		return nil, CorruptHierarchyDeletion()
	}
	if result.Attempt.Direction == workexecution.Recovery {
		if result.Recovery == nil || result.Recovery.ValidateFor(result.Attempt) != nil {
			return nil, CorruptHierarchyDeletion()
		}
	} else if result.Recovery != nil {
		return nil, CorruptHierarchyDeletion()
	}
	return recordcodec.Encode("hierarchy-environment-result", result)
}

func DecodeEnvironmentResult(raw []byte) (EnvironmentResult, error) {
	if len(raw) > 8192 {
		return EnvironmentResult{}, CorruptHierarchyDeletion()
	}
	result, err := recordcodec.Decode[EnvironmentResult](raw, "hierarchy-environment-result")
	if err != nil {
		return EnvironmentResult{}, err
	}
	if _, err := EncodeEnvironmentResult(result); err != nil {
		return EnvironmentResult{}, err
	}
	return result, nil
}

func (result EnvironmentResult) Input(source EnvironmentInput) (hierarchyexecution.EnvironmentInput, error) {
	observation := result.Report.Progress.Observation
	if observation == nil || observation.EnvironmentID != source.EnvironmentID {
		return hierarchyexecution.EnvironmentInput{}, CorruptHierarchyDeletion()
	}
	raw, err := EncodeEnvironmentInput(source)
	if err != nil {
		return hierarchyexecution.EnvironmentInput{}, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	if observation.Source.SHA256 != hex.EncodeToString(digest[:]) {
		return hierarchyexecution.EnvironmentInput{}, CorruptHierarchyDeletion()
	}
	input := hierarchyexecution.EnvironmentInput{Attempt: result.Attempt, Recovery: result.Recovery, Removal: &agentpb.EnvironmentRemovalInput{
		EnvironmentId: source.EnvironmentID, ExpectedVolumeDir: source.VolumeDirectory,
		CapturedSource: &agentpb.WorkInputReference{InputId: observation.Source.ID, Sha256: digest[:]},
	}}
	if _, err := result.Report.Checkpoint(input); err != nil {
		return hierarchyexecution.EnvironmentInput{}, err
	}
	return input, nil
}

func EnvironmentRecoveryResultKey(attemptID string) string {
	return "/v1/hierarchy-removal-recovery-results/" + attemptID
}
