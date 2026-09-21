package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: replay is valid only when every same-commit membership and
// coordination record still names the marker's winning Task exactly.
func (repository *BackupRuntimeRepository) exactBackupRunPublicationSubordinates(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	readRevision int64,
	commitRevision int64,
) bool {
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		return false
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil {
		return false
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return false
	}
	keys := []string{
		membershipKey,
		taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskQueueKey(task.Executor, task.ID),
	}
	for _, exclusion := range exclusions {
		key, keyErr := backupruntime.BackupSourceTargetExclusionKey(exclusion.TargetKind, exclusion.TargetID)
		if keyErr != nil {
			return false
		}
		keys = append(keys, key)
	}
	keys = append(keys, ownerKeys...)
	keys = append(keys, hierarchyrecord.EnvironmentMutationEpochKey(run.EnvironmentID))
	read, err := repository.readFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision != commitRevision {
			return false
		}
	}
	if string(read.Values[0].Value) != task.ID {
		return false
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return false
	}
	defer clear(reference)
	for index := 1; index <= 3; index++ {
		if !bytes.Equal(read.Values[index].Value, reference) {
			return false
		}
	}
	exclusionOffset := 4
	for index, exclusion := range exclusions {
		expected, encodeErr := backupruntime.EncodeBackupSourceTargetExclusionRecord(exclusion)
		if encodeErr != nil {
			return false
		}
		matches := bytes.Equal(read.Values[exclusionOffset+index].Value, expected)
		clear(expected)
		if !matches {
			return false
		}
	}
	ownerOffset := exclusionOffset + len(exclusions)
	for index := range ownerKeys {
		if string(read.Values[ownerOffset+index].Value) != task.ID {
			return false
		}
	}
	epoch, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: run.EnvironmentID,
	})
	if err != nil {
		return false
	}
	defer clear(epoch)
	return bytes.Equal(read.Values[len(read.Values)-1].Value, epoch)
}
