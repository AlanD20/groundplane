package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) taskStagingDeliveryPruneHeld(
	ctx context.Context,
	task TaskRecord,
	taskRevision, revision int64,
) (bool, error) {
	if task.TerminalAssignment == nil || task.TerminalAssignment.AssignmentGeneration == 0 {
		return false, nil
	}
	// Successful workers remove their owned staging before reporting success.
	// Failed/aborted capture and Restore workers may retain files or journals;
	// terminal delivery alone proves no physical cleanup of those resources.
	needsCleanupProof := task.Status != taskjournal.TaskStatusCompleted &&
		(task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskRestore)
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys:     []string{backupruntime.BackupStagingDeliveryKey(task.TerminalAssignment.AgentID)},
			Revision: revision,
		},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, backupStagingConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return needsCleanupProof, nil
	}
	delivery, err := backupruntime.DecodeBackupStagingDelivery(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	if delivery.AgentID != task.TerminalAssignment.AgentID ||
		delivery.AgentGeneration != task.TerminalAssignment.AgentGeneration {
		return false, backupStagingConflict()
	}
	for _, reference := range delivery.Tasks {
		if reference.TaskID == task.ID {
			if delivery.Ack == nil {
				return true, nil
			}
			// A terminal, acknowledged discard/cleanup disposition proves that
			// this Task's stage and Volume journals no longer need its index.
			return needsCleanupProof && (reference.Revision != taskRevision || reference.RequiresTaskAuthority), nil
		}
	}
	// A later acknowledged, quiescent startup inventory may prove absence.
	// An inventory from before this Task finished cannot prove that fact.
	return needsCleanupProof && (delivery.Ack == nil || read.Values[0].ModRevision <= taskRevision), nil
}
