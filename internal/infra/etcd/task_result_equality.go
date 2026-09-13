package etcd

import "slices"

func taskResultsEqual(left, right TaskResultRecord) bool {
	if left.Kind != right.Kind || left.ExitCode != right.ExitCode || left.ExecutionEpoch != right.ExecutionEpoch ||
		left.ReleaseRecoveryRecordSHA256 != right.ReleaseRecoveryRecordSHA256 ||
		left.FailedStepID != right.FailedStepID || left.Diagnostic != right.Diagnostic ||
		left.ReconciliationRequired != right.ReconciliationRequired || len(left.Projects) != len(right.Projects) ||
		len(left.ProxyEvidence) != len(right.ProxyEvidence) ||
		len(left.RecreateEvidence) != len(right.RecreateEvidence) ||
		!taskCandidateAbsenceEvidenceEqual(left.CandidateAbsenceEvidence, right.CandidateAbsenceEvidence) {
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
	return taskDNSResolverEvidenceEqual(left.DNSResolverCandidateObservation, right.DNSResolverCandidateObservation) &&
		taskDNSResolverEvidenceEqual(left.DNSResolverRollbackObservation, right.DNSResolverRollbackObservation)
}

func taskCandidateAbsenceEvidenceEqual(left, right *TaskCandidateAbsenceEvidence) bool {
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
