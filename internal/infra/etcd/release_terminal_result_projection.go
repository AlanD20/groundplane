package etcd

import (
	"slices"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func unassignedReleaseTerminal(task TaskRecord, status taskjournal.TaskStatus) bool {
	preflightFailure := task.Result != nil && !task.Result.ReconciliationRequired &&
		(status == taskjournal.TaskStatusFailed &&
			(task.Result.Diagnostic == taskjournal.TaskResultDiagnosticReleaseNetworkFailed || task.Result.Diagnostic == taskjournal.TaskResultDiagnosticDNSActivationFailed) ||
			status == taskjournal.TaskStatusTimedOut && task.Result.Diagnostic == taskjournal.TaskResultDiagnosticDNSActivationTimedOut)
	return (status == taskjournal.TaskStatusAborted || preflightFailure) && task.Status == status &&
		task.StartedAt == nil &&
		task.FinishedAt != nil &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		(task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback) &&
		task.Params[releaserender.TaskReleasePublicationParam] != ""
}

func releasePreparedRuntime(
	marker releases.ReleasePublicationMarker,
	serviceID string,
) (executionplan.CandidateRuntime, error) {
	if marker.CandidateReleaseDescriptor.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		for _, input := range marker.BlueprintRuntimes {
			if input.ServiceID == serviceID {
				return executionplan.OpenBlueprintRuntime(marker.ExecutedComposeArtifact, input)
			}
		}
	} else {
		for _, runtime := range marker.PreparedRuntimes {
			if runtime.ServiceID == serviceID {
				return runtime, nil
			}
		}
	}
	return executionplan.CandidateRuntime{}, errs.New(
		errs.KindStateConflict,
		"Release member has no captured final runtime",
	)
}

func releaseRollingEvidence(result taskjournal.TaskResultRecord, serviceID string) (rollingrelease.Completion, bool) {
	for _, evidence := range result.RollingEvidence {
		if evidence.ServiceID == serviceID {
			return evidence, true
		}
	}
	return rollingrelease.Completion{}, false
}

// Completion belongs to the sealed member, not a fixed five-step array offset.
func releaseCompletionStep(marker releases.ReleasePublicationMarker, serviceID string) (string, error) {
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil {
		return "", err
	}
	for _, member := range procedure.GetMembers() {
		if member.GetServiceId() == serviceID && len(member.GetForwardStepIds()) != 0 {
			return member.GetForwardStepIds()[len(member.GetForwardStepIds())-1], nil
		}
	}
	return "", errs.New(errs.KindStateConflict, "Release completion has no captured member step")
}

func releaseFailedMemberOrdinal(
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	marker releases.ReleasePublicationMarker,
	members []domain.GroupMember,
) (uint32, error) {
	if terminalStatus == taskjournal.TaskStatusCompleted {
		return 0, nil
	}
	ordinal, err := releaseFailedOrdinalFromResult(task, result, marker, members)
	if err != nil {
		return 0, err
	}
	if ordinal == 0 &&
		(result.Diagnostic == taskjournal.TaskResultDiagnosticTimeoutBeforeEffect || unassignedReleaseTerminal(task, terminalStatus)) {
		return 1, nil
	}
	if ordinal == 0 {
		return 0, errs.New(errs.KindStateConflict, "release failed step is outside the frozen procedure")
	}
	return ordinal, nil
}

func releaseFailedOrdinalFromResult(task TaskRecord, result taskjournal.TaskResultRecord,
	marker releases.ReleasePublicationMarker, members []domain.GroupMember,
) (uint32, error) {
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil {
		return 0, err
	}
	for _, step := range task.Steps {
		if step.ID == result.FailedStepID {
			if value := task.Params[releaserender.ReleaseHookStepMemberParam(step.ID)]; value != "" {
				ordinal, err := strconv.ParseUint(value, 10, 32)
				if err != nil || ordinal == 0 || ordinal > uint64(len(members)) ||
					task.Params[releaserender.ReleaseHookStepExecutionParam(step.ID)] == "" {
					return 0, nil
				}
				return uint32(ordinal), nil
			}
			if task.Params[releaserender.ReleaseHookStepExecutionParam(step.ID)] != "" {
				return 0, nil
			}
			for _, captured := range procedure.GetMembers() {
				serving, absence := captured.GetServingPredecessor(), captured.GetCandidateAbsence()
				if !slices.Contains(captured.GetForwardStepIds(), step.ID) &&
					serving.GetProbeStepId() != step.ID && serving.GetCompensateStepId() != step.ID &&
					absence.GetProbeStepId() != step.ID && absence.GetCompensateStepId() != step.ID {
					continue
				}
				for _, member := range members {
					if member.ServiceID == captured.GetServiceId() &&
						member.ReleaseID == captured.GetCandidateReleaseId() {
						return member.Ordinal, nil
					}
				}
			}
		}
	}
	return 0, nil
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

func releaseRecreateEvidence(
	result taskjournal.TaskResultRecord,
	serviceID string,
) (taskjournal.TaskRecreateEvidence, bool) {
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
