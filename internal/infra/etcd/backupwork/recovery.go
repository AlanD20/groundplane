package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryKey(workID string) string { return "/v1/runtime/backup-work-recovery/" + workID }

type RecoverySnapshot struct {
	Source   executionplan.BackupWorkRecovery
	Revision int64
}

func (repository *InputRepository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySnapshot, error) {
	item := view.Item.Item
	if (item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Restore && item.Operation != workexecution.Backup) || item.Input == nil {
		return RecoverySnapshot{}, conflict("Backup recovery requires captured work")
	}
	read, err := repository.readSettlementKeys(ctx, []string{RecoveryKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoverySnapshot{}, conflict("Backup recovery source is unavailable")
	}
	source, err := executionplan.DecodeBackupWorkRecovery(read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input || source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Forward.Number > item.AttemptNumber || item.Result == nil || *item.Result != source.Evidence {
		return RecoverySnapshot{}, conflict("Backup recovery source differs from its original failure")
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}
