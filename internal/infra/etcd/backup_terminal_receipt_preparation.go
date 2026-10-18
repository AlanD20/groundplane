package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupTerminalTaskEvidence(task TaskRecord) (backupruntime.BackupTerminalTaskEvidence, error) {
	if task.FinishedAt == nil || task.RetainUntil == nil {
		return backupruntime.BackupTerminalTaskEvidence{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt requires terminal Task timestamps",
		)
	}
	resultDigest, err := backupruntime.BackupTerminalResultDigest(task.Result)
	if err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	taskDigest, err := backupTerminalTaskDigest(task)
	if err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	evidence := backupruntime.BackupTerminalTaskEvidence{
		TaskID: task.ID, TaskType: task.Type, OperationID: task.OperationID, RetryOf: task.RetryOf,
		Owner: task.Owner, Actor: task.Actor, Executor: task.Executor, Target: task.Target,
		PlanID: task.PlanID, PlanHash: task.PlanHash, Status: task.Status,
		ResultDigest: resultDigest, TerminalAssignment: taskjournal.CloneTaskTerminalAssignment(task.TerminalAssignment),
		CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		FinishedAt: *task.FinishedAt, RetainUntil: *task.RetainUntil, TaskDigest: taskDigest,
	}
	if task.StartedAt != nil {
		startedAt := *task.StartedAt
		evidence.StartedAt = &startedAt
	}
	if err := backupruntime.ValidateBackupTerminalTaskEvidence(evidence); err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	return evidence, nil
}
