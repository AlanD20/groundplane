package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func validateBackupSecretResolutionRequest(request backupsecret.Request) error {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, request.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, request.AgentID) != nil || request.AgentGeneration == 0 ||
		request.Deadline.IsZero() || !request.Deadline.Equal(request.Deadline.UTC()) ||
		ids.Validate(ids.KindStep, request.StepID) != nil || request.Plan == nil || request.Step == nil ||
		request.Step.StepId != request.StepID {
		return errs.New(errs.KindValidationFailed, "backup secret resolution identity is invalid")
	}
	return nil
}

func validateBackupSecretTaskAssignment(
	task TaskRecord,
	assignment TaskAssignmentRecord,
	request backupsecret.Request,
) error {
	if task.ID != request.TaskID || task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune ||
		task.Status != taskjournal.TaskStatusRunning || task.Executor != taskjournal.TaskExecutorAgent ||
		assignment.AssignmentID != request.AssignmentID || assignment.TaskID != task.ID ||
		assignment.Executor != taskjournal.TaskExecutorAgent || assignment.AgentID != request.AgentID ||
		assignment.AgentGeneration != request.AgentGeneration ||
		!assignment.Deadline.Equal(request.Deadline) || task.StartedAt == nil ||
		!task.StartedAt.Equal(assignment.AssignedAt) ||
		!assignment.Deadline.Equal(assignment.AssignedAt.Add(
			time.Duration(task.TimeoutSeconds)*time.Second,
		)) {
		return errs.New(errs.KindStateConflict, "backup task assignment is not active")
	}
	return nil
}

func backupSecretStepIndex(plan *agentpb.ExecutionPlan, stepID string) (int, error) {
	index := -1
	for position, step := range plan.Steps {
		if step != nil && step.StepId == stepID {
			if index >= 0 {
				return 0, errs.New(errs.KindValidationFailed, "backup task step identity is duplicated")
			}
			index = position
		}
	}
	if index < 0 {
		return 0, errs.New(errs.KindStateConflict, "backup task step is not in its sealed plan")
	}
	return index, nil
}
