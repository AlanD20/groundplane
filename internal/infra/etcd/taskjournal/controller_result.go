package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerOutcome separates a settled failure from an execution error that
// still requires recovery. Only the former permits terminal acknowledgement.
type ControllerOutcome struct {
	Status  TaskStatus
	Failure *taskprogress.Failure
}

// ControllerResult records only the native execution observation. Resource
// settlement and restoration proof remain with their existing operation owners.
func ControllerResult(failure *taskprogress.Failure) TaskResultRecord {
	return TaskResultRecord{Kind: TaskResultController, Diagnostic: TaskResultDiagnosticNone,
		Failure: taskprogress.CloneFailure(failure)}
}

func validateControllerResult(result TaskResultRecord, status TaskStatus) error {
	if err := result.Failure.Validate(); err != nil {
		return err
	}
	if !IsTerminalTaskStatus(status) || status == TaskStatusCompleted && result.Failure != nil ||
		result.Failure != nil && result.Failure.ExitCode != 0 {
		return errs.New(errs.KindValidationFailed, "Controller failure does not match its terminal status")
	}
	// Closed native results cannot carry machine execution or restoration proof.
	if result.ExitCode != 0 || result.FailedStepID != "" || result.Diagnostic != TaskResultDiagnosticNone ||
		result.DNSWithdrawalTaskID != "" || result.ReconciliationRequired || len(result.Projects) != 0 ||
		len(result.BackingObservations) != 0 || len(result.ProxyEvidence) != 0 || len(result.RecreateEvidence) != 0 ||
		len(result.RollingEvidence) != 0 || result.CandidateAbsenceEvidence != nil ||
		result.ServiceAbsenceEvidence != nil ||
		result.DNSResolverCandidateObservation != nil || result.DNSResolverRollbackObservation != nil ||
		result.ExecutionEpoch != 0 || result.ReleaseRecoveryRecordSHA256 != "" || result.AssignmentGeneration != 0 {
		return errs.New(errs.KindValidationFailed, "Controller result carries unrelated execution evidence")
	}
	return nil
}
