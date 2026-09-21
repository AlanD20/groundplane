package etcd

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strconv"
	"time"
)

func releaseFailedMemberOrdinal(task TaskRecord, terminalStatus taskjournal.TaskStatus, result taskjournal.TaskResultRecord) (uint32, error) {
	if terminalStatus == taskjournal.TaskStatusCompleted {
		return 0, nil
	}
	ordinal := releaseFailedOrdinalFromResult(task, result)
	if ordinal == 0 &&
		(result.Diagnostic == taskjournal.TaskResultDiagnosticTimeoutBeforeEffect || unassignedReleaseAbort(task, terminalStatus)) {
		return 1, nil
	}
	if ordinal == 0 {
		return 0, errs.New(errs.KindStateConflict, "release failed step is outside the frozen procedure")
	}
	return ordinal, nil
}

func releaseFailedOrdinalFromResult(task TaskRecord, result taskjournal.TaskResultRecord) uint32 {
	for index, step := range task.Steps {
		if step.ID == result.FailedStepID {
			if value := task.Params[ReleaseHookStepMemberParam(step.ID)]; value != "" {
				ordinal, err := strconv.ParseUint(value, 10, 32)
				if err != nil || ordinal == 0 {
					return 0
				}
				return uint32(ordinal)
			}
			if task.Params[ReleaseHookStepExecutionParam(step.ID)] != "" {
				return 0
			}
			return uint32(index/5 + 1)
		}
	}
	return 0
}

func releaseMemberTerminalState(
	head releases.ReleaseOperationHead,
	ordinal uint32,
	terminalStatus taskjournal.TaskStatus,
	failedOrdinal uint32,
	result taskjournal.TaskResultRecord,
	compensated bool,
) (domain.State, bool, bool) {
	if terminalStatus == taskjournal.TaskStatusCompleted {
		return domain.StateCompleted, true, false
	}
	if compensated {
		return domain.StateFailed, false, false
	}
	if ordinal < failedOrdinal {
		if head.FailurePolicy == domain.OnFailureSwitchBack {
			return domain.StateRecoveryRequired, true, true
		}
		return domain.StateCompleted, true, false
	}
	if ordinal > failedOrdinal {
		return domain.StateAborted, false, false
	}
	if result.ReconciliationRequired {
		return domain.StateRecoveryRequired, false, true
	}
	switch terminalStatus {
	case taskjournal.TaskStatusTimedOut:
		return domain.StateTimedOut, false, false
	case taskjournal.TaskStatusAborted:
		return domain.StateAborted, false, false
	default:
		return domain.StateFailed, false, false
	}
}

func releaseProxyEvidence(result taskjournal.TaskResultRecord, serviceID string) (taskjournal.TaskProxyEvidence, bool) {
	for _, evidence := range result.ProxyEvidence {
		if evidence.ServiceID == serviceID {
			return evidence, true
		}
	}
	return taskjournal.TaskProxyEvidence{}, false
}

func releaseRecreateEvidence(result taskjournal.TaskResultRecord, serviceID string) (taskjournal.TaskRecreateEvidence, bool) {
	for _, evidence := range result.RecreateEvidence {
		if evidence.ServiceID == serviceID {
			return evidence, true
		}
	}
	return taskjournal.TaskRecreateEvidence{}, false
}

func releaseOperationTerminalState(status taskjournal.TaskStatus) domain.State {
	switch status {
	case taskjournal.TaskStatusCompleted:
		return domain.StateCompleted
	case taskjournal.TaskStatusTimedOut:
		return domain.StateTimedOut
	case taskjournal.TaskStatusAborted:
		return domain.StateAborted
	default:
		return domain.StateFailed
	}
}

func releaseRollbackMaterial(intent domain.Intent, state domain.State, terminalAt time.Time) domain.RollbackMaterial {
	references := []string{intent.RenderInputID, intent.CandidateWorkload.LocalImageID}
	digest, _ := domain.Digest(references)
	material := domain.RollbackMaterial{
		ReleaseID: intent.ID, Status: domain.RetentionAvailable,
		References: references, Digest: digest, Revision: 1,
	}
	if state != domain.StateCompleted {
		material.Status = domain.RetentionExpired
		expired := terminalAt
		material.ExpiredAt = &expired
	}
	return material
}

func releaseTerminalSummary(
	intent domain.Intent,
	state domain.State,
	servingReleaseID string,
	evidence []domain.EffectEvidence,
	attempts []domain.Attempt,
	materialDigest string,
	terminalAt time.Time,
) domain.TerminalSummary {
	effectDigests := make([]string, len(evidence))
	for index := range evidence {
		effectDigests[index] = evidence[index].EffectDigest
	}
	attemptIDs := make([]string, len(attempts))
	for index := range attempts {
		attemptIDs[index] = attempts[index].TaskID
	}
	return domain.TerminalSummary{
		ReleaseID: intent.ID, Outcome: state, FinalServingReleaseID: servingReleaseID,
		EffectDigests: effectDigests, AttemptIDs: attemptIDs,
		RollbackMaterialDigest: materialDigest, CompletedAt: terminalAt,
	}
}

func releasePriorServingEvidence(result taskjournal.TaskResultRecord, serviceID string) string {
	evidence, found := releaseProxyEvidence(result, serviceID)
	if found {
		return evidence.ReleaseID
	}
	recreate, found := releaseRecreateEvidence(result, serviceID)
	if found {
		return recreate.ReleaseID
	}
	return ""
}
