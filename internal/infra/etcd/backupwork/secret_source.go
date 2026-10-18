package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SecretSource selects a source from stored input, not from an Agent-supplied
// plan. The caller must authenticate the attempt before this read and fence the
// returned guards with credential evidence before delivering any plaintext.
type SecretSource struct {
	Input    *agentpb.BackupWorkInput
	TaskID   string
	Recovery bool
	Deadline time.Time
	Ordinal  uint32
	Purposes []agentpb.BackupSecretSlotPurpose
	Guards   workqueue.Change
}

func (repository *InputRepository) LoadSecretSource(ctx context.Context, view workqueue.ResumeView,
	ordinal uint32, at time.Time,
) (SecretSource, error) {
	attempt, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	if at.IsZero() || attempt.StartedAt == nil || at.Before(*attempt.StartedAt) ||
		!at.Before(attempt.Deadline) || attempt.FinishedAt != nil {
		return SecretSource{}, conflict("Backup cannot open credentials outside its active execution window")
	}
	if attempt.Direction == workexecution.Recovery {
		assignment, guards, err := repository.LoadRecoveryAssignment(ctx, view)
		if err != nil {
			return SecretSource{}, err
		}
		if assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
			assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION ||
			uint64(ordinal) >= uint64(len(assignment.Input.Steps)) {
			return SecretSource{}, conflict("Recovery credentials require an unfinished captured Restore")
		}
		purposes, err := backupsecret.SourcePurposes(assignment.Input.Steps[ordinal])
		if err != nil {
			return SecretSource{}, err
		}
		return SecretSource{Input: assignment.Input, TaskID: attempt.TaskID, Recovery: true,
			Deadline: attempt.Deadline, Ordinal: ordinal, Purposes: purposes, Guards: guards}, nil
	}
	if !at.Before(task.Deadline) || task.Stop != nil || item.ForwardRevoked {
		return SecretSource{}, conflict("Backup forward credential authority has ended")
	}
	assignment, guards, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return SecretSource{}, err
	}
	if uint64(ordinal) >= uint64(len(assignment.Input.Steps)) {
		return SecretSource{}, conflict("Backup credential source is not part of captured input")
	}
	step := assignment.Input.Steps[ordinal]
	if !at.Before(time.Unix(0, int64(step.StepDeadlineUnixNano))) {
		return SecretSource{}, conflict("Backup credential source deadline has expired")
	}
	purposes, err := backupsecret.SourcePurposes(step)
	if err != nil {
		return SecretSource{}, err
	}
	return SecretSource{Input: assignment.Input, TaskID: attempt.TaskID,
		Deadline: minTime(attempt.Deadline, time.Unix(0, int64(step.StepDeadlineUnixNano))),
		Ordinal:  ordinal, Purposes: purposes, Guards: guards}, nil
}

func minTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
