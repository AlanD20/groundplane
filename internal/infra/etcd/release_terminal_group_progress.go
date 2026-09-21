package etcd

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

func releaseTerminalGroupProgress(
	head ReleaseOperationHead,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (domain.GroupProgress, error) {
	executor, err := domain.NewGroupExecutor(domain.GroupManifest{
		OperationID: head.OperationID, ReleaseGroupID: head.ReleaseGroupID, EnvironmentID: head.EnvironmentID,
		FailurePolicy: head.FailurePolicy, Members: head.Members,
		ConfiguredTimeoutSeconds: head.ConfiguredTimeoutSeconds, ComputedBudgetSeconds: head.ComputedBudgetSeconds,
	})
	if err != nil || head.Progress == nil {
		return domain.GroupProgress{}, corruptReleaseRecord()
	}
	progress := *head.Progress
	progress.Results = nil
	progress.NextMemberOrdinal = 1
	progress.Compensating = false
	progress.NextCompensationOrdinal = 0
	failedOrdinal, err := releaseFailedMemberOrdinal(task, terminalStatus, result)
	if err != nil {
		return domain.GroupProgress{}, err
	}
	limit := uint32(len(head.Members))
	if failedOrdinal > 0 {
		limit = failedOrdinal - 1
	}
	for ordinal := uint32(1); ordinal <= limit; ordinal++ {
		member := head.Members[ordinal-1]
		progress, err = executor.RecordServing(progress, domain.MemberResult{
			Ordinal: ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: domain.MemberServing, ServingReleaseID: member.ReleaseID,
		}, terminalAt)
		if err != nil {
			return domain.GroupProgress{}, err
		}
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		return executor.Complete(progress, terminalAt)
	}
	member := head.Members[failedOrdinal-1]
	progress, err = executor.RecordFailure(progress, domain.MemberResult{
		Ordinal: failedOrdinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
		Outcome: domain.MemberFailed, FailureCode: string(terminalStatus), FailureDetail: string(result.Diagnostic),
	}, terminalAt)
	if err != nil || head.FailurePolicy != domain.OnFailureSwitchBack || result.ReconciliationRequired {
		return progress, err
	}
	for ordinal := failedOrdinal - 1; ordinal > 0; ordinal-- {
		member := head.Members[ordinal-1]
		progress, err = executor.RecordCompensation(progress, domain.MemberResult{
			Ordinal: ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: domain.MemberCompensated, CompensationReleaseID: releasePriorServingEvidence(result, member.ServiceID),
		}, terminalAt)
		if err != nil {
			return domain.GroupProgress{}, err
		}
	}
	return progress, nil
}
