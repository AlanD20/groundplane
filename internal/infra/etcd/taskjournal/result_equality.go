package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"slices"
)

func TaskResultsEqual(left, right TaskResultRecord) bool {
	if !taskprogress.FailuresEqual(left.Failure, right.Failure) || left.Kind != right.Kind ||
		left.ExitCode != right.ExitCode ||
		left.ExecutionEpoch != right.ExecutionEpoch ||
		left.AssignmentGeneration != right.AssignmentGeneration ||
		left.ReleaseRecoveryRecordSHA256 != right.ReleaseRecoveryRecordSHA256 ||
		left.FailedStepID != right.FailedStepID ||
		left.Diagnostic != right.Diagnostic ||
		left.ReconciliationRequired != right.ReconciliationRequired ||
		len(left.Projects) != len(right.Projects) ||
		len(left.ProxyEvidence) != len(right.ProxyEvidence) ||
		len(left.RecreateEvidence) != len(right.RecreateEvidence) ||
		!slices.Equal(left.RollingEvidence, right.RollingEvidence) ||
		!TaskCandidateAbsenceEvidenceEqual(left.CandidateAbsenceEvidence, right.CandidateAbsenceEvidence) ||
		!TaskComponentRestorationEvidenceEqual(left.ComponentRestorationEvidence, right.ComponentRestorationEvidence) {

		return false
	}
	if !TaskServiceAbsenceEvidenceEqual(left.ServiceAbsenceEvidence, right.ServiceAbsenceEvidence) {
		return false
	}
	for index := range left.Projects {
		if left.Projects[index] != right.Projects[index] {
			return false
		}
	}
	for index := range left.ProxyEvidence {
		if left.ProxyEvidence[index] != right.ProxyEvidence[index] {
			return false
		}
	}
	for index := range left.RecreateEvidence {
		if left.RecreateEvidence[index] != right.RecreateEvidence[index] {
			return false
		}
	}
	return TaskDNSResolverEvidenceEqual(left.DNSResolverCandidateObservation, right.DNSResolverCandidateObservation) &&
		TaskDNSResolverEvidenceEqual(left.DNSResolverRollbackObservation, right.DNSResolverRollbackObservation)
}

func TaskServiceAbsenceEvidenceEqual(left, right *TaskServiceAbsenceEvidence) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left == nil || left.ServiceID == right.ServiceID && left.ArtifactID == right.ArtifactID &&
		left.ComposeProjectName == right.ComposeProjectName && left.AbsenceProven == right.AbsenceProven &&
		slices.Equal(left.ComposeNames, right.ComposeNames)
}

func TaskComponentRestorationEvidenceEqual(left, right *TaskComponentRestorationEvidence) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left == nil || *left == *right
}

func CloneTaskComponentRestorationEvidence(value *TaskComponentRestorationEvidence) *TaskComponentRestorationEvidence {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func TaskCandidateAbsenceEvidenceEqual(left, right *TaskCandidateAbsenceEvidence) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	if left == nil {
		return true
	}
	return left.AssignmentID == right.AssignmentID && left.PlanHash == right.PlanHash &&
		left.AuthoritySHA256 == right.AuthoritySHA256 && left.ComposeProjectName == right.ComposeProjectName &&
		left.CandidateArtifactID == right.CandidateArtifactID && left.AbsenceProven == right.AbsenceProven &&
		slices.Equal(left.Candidates, right.Candidates)
}
