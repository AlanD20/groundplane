package backupsecrets

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// WorkEvidence owns encrypted values and all read guards. The delivery owner
// must commit the guards with its delivery grant before opening any plaintext.
type WorkEvidence struct {
	Evidence   Evidence
	Source     backupwork.SecretSource
	Attempt    workexecution.Attempt
	Conditions []keyvalue.Condition
}

func (reader *Reader) ResolveWorkSecretEvidence(ctx context.Context, queue *workqueue.Repository,
	authorization agentregistration.WorkAuthorization, claimed *agentpb.WorkAttemptIdentity, ordinal uint32,
) (WorkEvidence, error) {
	if reader == nil || queue == nil || reader.now == nil {
		return WorkEvidence{}, errs.New(errs.KindInternal, "native Backup credential reader is not configured")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return WorkEvidence{}, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, authorization.Identity())
	if err != nil {
		return WorkEvidence{}, err
	}
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
	if err != nil {
		return WorkEvidence{}, err
	}
	if !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return WorkEvidence{}, errs.New(errs.KindStateConflict, "Backup credential attempt changed")
	}
	tracked := &credentialReadGuards{store: reader.store, revision: view.Work.ReadRevision, conditions: make(map[string]keyvalue.Condition)}
	inputs, err := backupwork.NewInputRepository(tracked)
	if err != nil {
		return WorkEvidence{}, err
	}
	source, err := inputs.LoadSecretSource(ctx, view, ordinal, reader.now())
	if err != nil {
		return WorkEvidence{}, err
	}
	local := &Reader{store: tracked, now: reader.now}
	evidence, err := local.workResourceEvidence(ctx, source, view.Work.ReadRevision)
	if err != nil {
		return WorkEvidence{}, err
	}
	input := source.Input
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	evidence, err = local.resolveCapturedEvidence(ctx, evidence, scope, input.Steps, int(ordinal))
	if err != nil {
		evidence.Clear()
		return WorkEvidence{}, err
	}
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision})
	guards = append(guards, tracked.sortedConditions()...)
	guards = append(guards, source.Guards.Conditions...)
	return WorkEvidence{Evidence: evidence, Source: source, Attempt: view.Attempt.Attempt, Conditions: guards}, nil
}

func (reader *Reader) workResourceEvidence(ctx context.Context, source backupwork.SecretSource, revision int64) (Evidence, error) {
	input, taskID := source.Input, source.TaskID
	key := backupruntime.BackupRunKey(taskID)
	if input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE {
		key = backupruntime.BackupRestoreKey(taskID)
	}
	if input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		key = backupruntime.BackupRecoveryPointPruneDispatchKey(taskID)
	}
	read, err := reader.readFixed(ctx, []string{key}, revision)
	if err != nil {
		return Evidence{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return Evidence{}, errs.New(errs.KindStateConflict, "Backup resource operation is unavailable")
	}
	evidence := Evidence{ReadRevision: revision}
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
		if err != nil {
			return Evidence{}, err
		}
		if run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning || len(run.Sources) != len(input.Steps) {
			return Evidence{}, errs.New(errs.KindStateConflict, "Backup run is not active")
		}
		scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project, EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
		for index, captured := range run.Sources {
			if err := backupplanning.ValidateBackupRunStepAuthority(run, captured, input.Steps[index], scope); err != nil {
				return Evidence{}, err
			}
		}
		evidence.Run = &run
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		restore, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		if err != nil {
			return Evidence{}, err
		}
		if restore.State == backupruntime.BackupRestoreFailedSafe || restore.State == backupruntime.BackupRestoreCompleted ||
			(restore.State == backupruntime.BackupRestoreRecoveryRequired) != source.Recovery {
			return Evidence{}, errs.New(errs.KindStateConflict, "Restore is not active")
		}
		if err := backupruntime.ValidateRestoreWorkInput(restore, input); err != nil {
			return Evidence{}, err
		}
		evidence.Restore = &restore
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[0].Value)
		if err != nil {
			return Evidence{}, err
		}
		evidence.Dispatch, evidence.DispatchRevision = &dispatch, read.Values[0].ModRevision
	default:
		return Evidence{}, errs.New(errs.KindValidationFailed, "Backup credential operation is unsupported")
	}
	return evidence, nil
}
