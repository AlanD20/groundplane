package taskassignments

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/oklog/ulid/v2"
)

// BackupAuthorityFence is the durable assignment projection of the sealed
// schema-one authority. It authorizes checkpoint identity only, never effects.
type BackupAuthorityFence struct {
	AssignmentGeneration uint64                     `json:"assignment_generation"`
	AuthoritySHA256      string                     `json:"authority_sha256"`
	Steps                []BackupStepAuthorityFence `json:"steps"`
}

type BackupStepAuthorityFence struct {
	StepID          string `json:"step_id"`
	ExecutionID     string `json:"execution_id"`
	AuthoritySHA256 string `json:"authority_sha256"`
}

func cloneBackupAuthorityFence(fence *BackupAuthorityFence) *BackupAuthorityFence {
	if fence == nil {
		return nil
	}
	owned := *fence
	owned.Steps = append([]BackupStepAuthorityFence(nil), fence.Steps...)
	return &owned
}

func validateBackupAuthorityFence(fence *BackupAuthorityFence) error {
	if fence == nil {
		return nil
	}
	if fence.AssignmentGeneration == 0 || !recordcodec.ValidSHA256(fence.AuthoritySHA256) ||
		len(fence.Steps) == 0 || len(fence.Steps) > 12 {
		return CorruptTaskAssignment()
	}
	steps := make(map[string]struct{}, len(fence.Steps))
	executions := make(map[string]struct{}, len(fence.Steps))
	for _, step := range fence.Steps {
		if ids.Validate(ids.KindStep, step.StepID) != nil || len(step.ExecutionID) != 26 ||
			step.ExecutionID != strings.ToUpper(step.ExecutionID) || !recordcodec.ValidSHA256(step.AuthoritySHA256) {
			return CorruptTaskAssignment()
		}
		if _, err := ulid.ParseStrict(step.ExecutionID); err != nil {
			return CorruptTaskAssignment()
		}
		if _, exists := steps[step.StepID]; exists {
			return CorruptTaskAssignment()
		}
		if _, exists := executions[step.ExecutionID]; exists {
			return CorruptTaskAssignment()
		}
		steps[step.StepID] = struct{}{}
		executions[step.ExecutionID] = struct{}{}
	}
	return nil
}

func (fence *BackupAuthorityFence) MatchesCheckpoint(
	generation uint64,
	stepID, executionID, authoritySHA256 string,
) bool {
	if fence == nil || validateBackupAuthorityFence(fence) != nil || generation != fence.AssignmentGeneration {
		return false
	}
	for _, step := range fence.Steps {
		if step.StepID == stepID {
			return step.ExecutionID == executionID && step.AuthoritySHA256 == authoritySHA256
		}
	}
	return false
}
