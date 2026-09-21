package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: terminalization releases transient execution ownership but keeps
// immutable Task history and owner indexes. The retained run derives their
// original publication revision, while the receipt and retention indexes must
// be atomic with the terminal marker and Task.
func (repository *BackupRuntimeRepository) exactTerminalBackupRunSubordinates(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	task TaskRecord,
	runValue *etcdstore.KeyValue,
	readRevision int64,
	terminalRevision int64,
) bool {
	if runValue.ModRevision != terminalRevision {
		return false
	}
	run, err := backupruntime.DecodeBackupRunRecord(runValue.Value)
	if err != nil || validateBackupRunTaskBinding(task, run) != nil ||
		run.UpdatedAt != *task.FinishedAt {
		return false
	}
	wantState, err := backupRunStateForTaskStatus(task.Status)
	if err != nil || run.State != wantState {
		return false
	}
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		return false
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return false
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return false
	}
	markerRetentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return false
	}
	keys := []string{
		membershipKey,
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		taskAssignmentIndexKey(task.ID),
	}
	keys = append(keys, ownerKeys...)
	terminalOffset := len(keys)
	keys = append(keys,
		backupruntime.BackupTerminalReceiptKey(task.ID),
		taskRetentionIndexKey(task.ID, *task.RetainUntil),
		markerRetentionKey,
	)
	claimIndex := -1
	if task.TerminalAssignment != nil {
		claimIndex = len(keys)
		keys = append(keys, taskExecutionClaimKey(
			task.Executor, task.TerminalAssignment.AgentID, task.ID,
		))
	}
	read, err := repository.readFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[2] != nil || read.Values[3] != nil || read.Values[4] != nil ||
		(claimIndex >= 0 && read.Values[claimIndex] != nil) {
		return false
	}
	publicationRevision := read.Values[0].ModRevision
	if publicationRevision <= 0 || publicationRevision >= terminalRevision ||
		read.Values[0].Key != keys[0] || read.Values[0].Version != 1 ||
		string(read.Values[0].Value) != task.ID ||
		read.Values[1].Key != keys[1] || read.Values[1].ModRevision != publicationRevision {
		return false
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return false
	}
	defer clear(reference)
	if !bytes.Equal(read.Values[1].Value, reference) {
		return false
	}
	ownerOffset := 5
	for index := range ownerKeys {
		value := read.Values[ownerOffset+index]
		if value == nil || value.Key != ownerKeys[index] ||
			value.ModRevision != publicationRevision || string(value.Value) != task.ID {
			return false
		}
	}
	receiptValue := read.Values[terminalOffset]
	taskRetentionValue := read.Values[terminalOffset+1]
	markerRetentionValue := read.Values[terminalOffset+2]
	if receiptValue == nil || taskRetentionValue == nil || markerRetentionValue == nil ||
		receiptValue.ModRevision != terminalRevision ||
		taskRetentionValue.ModRevision != terminalRevision ||
		markerRetentionValue.ModRevision != terminalRevision {
		return false
	}
	retainedTaskID, err := idempotencyrecord.DecodeTaskReference(taskRetentionValue.Value)
	if err != nil || retainedTaskID != task.ID ||
		idempotencyrecord.DecodeRetentionReference(markerRetentionValue.Value, markerKey) != nil {
		return false
	}
	return repository.currentBackupRunConfigCompanions(
		ctx, run, readRevision, publicationRevision,
	)
}
