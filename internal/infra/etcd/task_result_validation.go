package etcd

import (
	"crypto/sha512"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"unicode/utf8"
)

func validateTaskResult(result TaskResultRecord, steps []taskjournal.TaskStepRecord, status taskjournal.TaskStatus) error {
	if result.Kind != taskjournal.TaskResultCompose && result.Kind != taskjournal.TaskResultEnvironmentDirectory {
		return errs.New(errs.KindValidationFailed, "task result kind is invalid")
	}
	switch result.Diagnostic {
	case taskjournal.TaskResultDiagnosticNone, taskjournal.TaskResultDiagnosticConfigRejected, taskjournal.TaskResultDiagnosticComposeFailed,
		taskjournal.TaskResultDiagnosticTimeoutBeforeEffect:
	default:
		return errs.New(errs.KindValidationFailed, "task result diagnostic is invalid")
	}
	if result.FailedStepID != "" {
		if err := recordcodec.ValidateID(ids.KindStep, result.FailedStepID); err != nil {
			return err
		}
		found := false
		for _, step := range steps {
			found = found || step.ID == result.FailedStepID
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "task result failed step does not belong to the task")
		}
	}
	if status == taskjournal.TaskStatusCompleted && (result.ExitCode != 0 || result.FailedStepID != "" ||
		result.Diagnostic != taskjournal.TaskResultDiagnosticNone || result.ReconciliationRequired) {
		return errs.New(errs.KindValidationFailed, "completed task result is inconsistent")
	}
	if result.Kind == taskjournal.TaskResultEnvironmentDirectory &&
		(len(result.Projects) != 0 || result.Diagnostic != taskjournal.TaskResultDiagnosticNone || result.ReconciliationRequired) {
		return errs.New(errs.KindValidationFailed, "environment directory task result is inconsistent")
	}
	if len(result.Projects) > 64 {
		return errs.New(errs.KindValidationFailed, "task result has too many project summaries")
	}
	for index, project := range result.Projects {
		if project.ProjectName == "" || !utf8.ValidString(project.ProjectName) ||
			strings.IndexByte(project.ProjectName, 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "task result project name is invalid")
		}
		if index > 0 && result.Projects[index-1].ProjectName >= project.ProjectName {
			return errs.New(errs.KindValidationFailed, "task result projects are not strictly sorted")
		}
		if err := recordcodec.ValidateTimestamp("task result observed_at", project.ObservedAt); err != nil {
			return err
		}
		if project.ContainerCount > 4096 || project.NetworkCount > 4096 ||
			project.VolumeCount > 4096 || project.CollisionCount > 4096 {
			return errs.New(errs.KindValidationFailed, "task result project count exceeds its limit")
		}
	}
	if len(result.ProxyEvidence) > 32 {
		return errs.New(errs.KindValidationFailed, "task result has too much proxy evidence")
	}
	for index, evidence := range result.ProxyEvidence {
		if ids.Validate(ids.KindService, evidence.ServiceID) != nil || !validReleaseEvidenceTarget(evidence.Target) ||
			evidence.ProxyGeneration == 0 ||
			!recordcodec.ValidSHA256(evidence.ConfigSHA256) || evidence.ReleaseID == "" ||
			(index > 0 && result.ProxyEvidence[index-1].ServiceID >= evidence.ServiceID) {
			return errs.New(errs.KindValidationFailed, "task result proxy evidence is invalid or unsorted")
		}
	}
	if len(result.RecreateEvidence) > 32 {
		return errs.New(errs.KindValidationFailed, "task result has too much recreate evidence")
	}
	for index, evidence := range result.RecreateEvidence {
		if ids.Validate(ids.KindService, evidence.ServiceID) != nil || !validReleaseEvidenceTarget(evidence.Target) ||
			ids.Validate(ids.KindDeployment, evidence.ReleaseID) != nil ||
			ids.Validate(ids.KindConfig, evidence.ArtifactID) != nil ||
			(index > 0 && result.RecreateEvidence[index-1].ServiceID >= evidence.ServiceID) {
			return errs.New(errs.KindValidationFailed, "task result recreate evidence is invalid or unsorted")
		}
	}
	if evidence := result.CandidateAbsenceEvidence; evidence != nil {
		if result.Kind != taskjournal.TaskResultCompose || ids.Validate(ids.KindAssignment, evidence.AssignmentID) != nil ||
			!recordcodec.ValidSHA256(evidence.PlanHash) || !recordcodec.ValidSHA256(evidence.AuthoritySHA256) ||
			evidence.ComposeProjectName == "" || ids.Validate(ids.KindConfig, evidence.CandidateArtifactID) != nil ||
			len(evidence.Candidates) == 0 || len(evidence.Candidates) > 32 {
			return errs.New(errs.KindValidationFailed, "task candidate absence evidence is invalid")
		}
		previous := ""
		for _, candidate := range evidence.Candidates {
			identity := candidate.ServiceID + "\x00" + candidate.ReleaseID
			if ids.Validate(ids.KindService, candidate.ServiceID) != nil ||
				ids.Validate(ids.KindDeployment, candidate.ReleaseID) != nil ||
				identity <= previous {
				return errs.New(errs.KindValidationFailed, "task candidate absence evidence is invalid or unsorted")
			}
			previous = identity
		}
	}
	for _, candidate := range []*TaskDNSResolverObservationEvidence{
		result.DNSResolverCandidateObservation,
		result.DNSResolverRollbackObservation,
	} {
		if err := validateTaskDNSResolverObservationEvidence(result.Kind, candidate); err != nil {
			return err
		}
	}
	return nil
}

func cloneTaskDNSResolverObservationEvidence(
	evidence *TaskDNSResolverObservationEvidence,
) *TaskDNSResolverObservationEvidence {
	if evidence == nil {
		return nil
	}
	cloned := *evidence
	cloned.CanonicalEvidence = append([]byte(nil), evidence.CanonicalEvidence...)
	return &cloned
}

func validSHA512(value string) bool {
	if len(value) != sha512.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha512.Size
}

func validReleaseEvidenceTarget(value string) bool {
	return value == "singleton" || value == "blue" || value == "green"
}

func cloneTaskResult(result *TaskResultRecord) *TaskResultRecord {
	if result == nil {
		return nil
	}
	cloned := *result
	cloned.Projects = append([]TaskObservedProjectSummary(nil), result.Projects...)
	cloned.ProxyEvidence = append([]TaskProxyEvidence(nil), result.ProxyEvidence...)
	cloned.RecreateEvidence = append([]TaskRecreateEvidence(nil), result.RecreateEvidence...)
	cloned.CandidateAbsenceEvidence = cloneTaskCandidateAbsenceEvidence(result.CandidateAbsenceEvidence)
	cloned.DNSResolverCandidateObservation = cloneTaskDNSResolverObservationEvidence(
		result.DNSResolverCandidateObservation,
	)
	cloned.DNSResolverRollbackObservation = cloneTaskDNSResolverObservationEvidence(
		result.DNSResolverRollbackObservation,
	)
	return &cloned
}

func cloneTaskCandidateAbsenceEvidence(evidence *TaskCandidateAbsenceEvidence) *TaskCandidateAbsenceEvidence {
	if evidence == nil {
		return nil
	}
	clone := *evidence
	clone.Candidates = append([]TaskCandidateAbsenceCandidate(nil), evidence.Candidates...)
	return &clone
}

func cloneTaskTerminalAssignment(
	identity *TaskTerminalAssignmentRecord,
) *TaskTerminalAssignmentRecord {
	if identity == nil {
		return nil
	}
	cloned := *identity
	return &cloned
}
