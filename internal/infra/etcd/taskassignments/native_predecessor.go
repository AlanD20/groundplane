package taskassignments

import (
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
)

type ReleaseNativePredecessorAuthority struct {
	ServiceID             string `json:"service_id"`
	CurrentArtifact       []byte `json:"current_artifact,omitempty"`
	RetainedPriorArtifact []byte `json:"retained_prior_artifact,omitempty"`
}

func cloneReleaseNativePredecessors(values []ReleaseNativePredecessorAuthority) []ReleaseNativePredecessorAuthority {
	result := slices.Clone(values)
	for index := range result {
		result[index].CurrentArtifact = slices.Clone(result[index].CurrentArtifact)
		result[index].RetainedPriorArtifact = slices.Clone(result[index].RetainedPriorArtifact)
	}
	return result
}

func validateNativeRestorationMemberWitness(authority ReleaseRestorationAuthority) error {
	if len(authority.NativePredecessors) != len(authority.Candidates) ||
		len(authority.NativePredecessors) > releases.MaximumReleasePublicationMembers {
		return CorruptTaskAssignment()
	}
	if authority.AppliedPredecessor != nil {
		if _, err := OpenRestorationWitness(authority.EnvironmentID, authority.AppliedPredecessor.ComposeArtifact); err != nil {
			return err
		}
	}
	total := 0
	for index, witness := range authority.NativePredecessors {
		candidate := authority.Candidates[index]
		total += len(witness.CurrentArtifact) + len(witness.RetainedPriorArtifact)
		if witness.ServiceID != candidate.ServiceID || total > taskjournal.MaximumTaskRecordBytes {
			return CorruptTaskAssignment()
		}
		if len(witness.CurrentArtifact) == 0 {
			if candidate.Target != ReleaseRestorationCandidateAbsence || len(witness.RetainedPriorArtifact) != 0 {
				return CorruptTaskAssignment()
			}
			continue
		}
		if candidate.Target != ReleaseRestorationServingPredecessor {
			return CorruptTaskAssignment()
		}
		if err := executionplan.ValidateNativePredecessorWitness(authority.EnvironmentID, witness.ServiceID, witness.CurrentArtifact, witness.RetainedPriorArtifact); err != nil {
			return CorruptTaskAssignment()
		}
	}
	return nil
}
