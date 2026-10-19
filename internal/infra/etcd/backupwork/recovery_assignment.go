package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *InputRepository) LoadRecoveryAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.BackupRecoveryAssignment, workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.CurrentExecutor || !view.ActiveAttempt || !task.PublicationComplete || task.CollectionStartedAt != nil ||
		task.FinishedAt != nil || (item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) || attempt.Direction != workexecution.Recovery ||
		workexecution.ValidateDelivery(item, attempt) != nil {
		return nil, workqueue.Change{}, conflict("Backup recovery no longer owns its host assignment")
	}
	input, change, err := repository.PrepareAdmission(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	source, err := repository.LoadRecovery(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if err := source.Source.ValidateFor(attempt); err != nil {
		return nil, workqueue.Change{}, err
	}
	raw, err := executionplan.EncodeBackupWorkRecovery(source.Source)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	progress, progressGuard, err := repository.LoadProgress(ctx, view.Work, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	mode := agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_UNSPECIFIED
	if item.Operation == workexecution.Restore {
		restore, guard, err := repository.LoadStagingRestore(ctx, StagingSource{View: view.Work, Input: input})
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		mode = agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_NO_LIVE_MUTATION
		if restore.MutationStarted {
			mode = agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION
		}
		change.Conditions = append(change.Conditions, guard)
	}
	assignment, _, _, err := executionplan.OpenBackupRecoveryAssignment(&agentpb.BackupRecoveryAssignment{
		Attempt: identity, Input: input, RecoverySource: raw, Sources: progress, RestoreMode: mode,
	}, attempt.Executor)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		progressGuard,
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: source.Revision},
	)
	return assignment, change, nil
}
