package handlers

import (
	"github.com/AlanD20/groundplane/internal/controller/taskinspection"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func executionSummary(value taskinspection.Summary) apiTypes.TaskSummary {
	return apiTypes.TaskSummary{ID: value.ID, Action: value.Action, TargetID: value.TargetID,
		TargetName: value.TargetName, ResourceKind: value.ResourceKind, Scope: value.Scope, Actor: value.Actor, Status: value.Status,
		Counts: value.Counts, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		Deadline: value.Deadline, FinishedAt: value.FinishedAt, HistoryExpired: value.HistoryExpired,
		StopRequested: value.StopRequested,
		Cancel:        apiTypes.TaskActionAvailability{Allowed: value.Cancel.Allowed, Reason: value.Cancel.Reason}}
}

func executionWork(value taskinspection.Work) apiTypes.TaskWork {
	result := apiTypes.TaskWork{ID: value.ID, TargetID: value.TargetID, Operation: value.Operation,
		Placement: value.Placement, Phase: value.Phase, Outcome: value.Outcome, Wait: value.Wait,
		Cleanup: value.Cleanup, AttemptCount: value.AttemptCount, ActiveAttemptID: value.ActiveAttemptID,
		Retry:     apiTypes.TaskActionAvailability{Allowed: value.Retry.Allowed, Reason: value.Retry.Reason},
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, Deadline: value.Deadline}
	if value.Preparation != nil {
		result.Preparation = &apiTypes.TaskWorkPreparation{Summary: value.Preparation.Summary,
			DependencyID: value.Preparation.DependencyID, FinishedAt: value.Preparation.FinishedAt}
	}
	return result
}

func executionAttempt(value taskinspection.Attempt) apiTypes.TaskAttempt {
	return apiTypes.TaskAttempt{ID: value.ID, Number: value.Number, Executor: value.Executor,
		HostID: value.HostID, AgentID: value.AgentID, Generation: value.Generation,
		Direction: value.Direction, Outcome: value.Outcome, AssignedAt: value.AssignedAt,
		Deadline: value.Deadline, StartedAt: value.StartedAt, FinishedAt: value.FinishedAt}
}
