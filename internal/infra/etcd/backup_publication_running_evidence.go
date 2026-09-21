package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a running Task has exactly one assignment replacing its queue
// record, while all immutable publication indexes retain the pending marker's
// revision and the current run is paired with the current Environment epoch.
func (repository *BackupRuntimeRepository) exactRunningBackupRunSubordinates(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	runRevision int64,
	taskRevision int64,
	readRevision int64,
	publicationRevision int64,
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
		taskjournal.TaskAssignmentIndexKey(task.ID),
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
	read, err := repository.ReadFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[3] != nil || read.Values[4] == nil {
		return false
	}
	immutable := []int{0, 1, 2}
	exclusionOffset := 5
	for index := range exclusions {
		immutable = append(immutable, exclusionOffset+index)
	}
	ownerOffset := exclusionOffset + len(exclusions)
	for index := range ownerKeys {
		immutable = append(immutable, ownerOffset+index)
	}
	for _, index := range immutable {
		if read.Values[index] == nil || read.Values[index].Key != keys[index] ||
			read.Values[index].ModRevision != publicationRevision {
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
	if !bytes.Equal(read.Values[1].Value, reference) ||
		!bytes.Equal(read.Values[2].Value, reference) {
		return false
	}
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
	for index := range ownerKeys {
		if string(read.Values[ownerOffset+index].Value) != task.ID {
			return false
		}
	}
	epochIndex := len(read.Values) - 1
	if read.Values[epochIndex] == nil || read.Values[epochIndex].Key != keys[epochIndex] ||
		read.Values[epochIndex].ModRevision != runRevision {
		return false
	}
	epoch, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: run.EnvironmentID,
	})
	if err != nil {
		return false
	}
	defer clear(epoch)
	if !bytes.Equal(read.Values[epochIndex].Value, epoch) {
		return false
	}
	assignmentValue := read.Values[4]
	assignment, err := taskassignments.DecodeTaskAssignment(assignmentValue.Value)
	if err != nil || assignmentValue.Key != keys[4] ||
		assignmentValue.ModRevision <= publicationRevision ||
		assignmentValue.ModRevision > taskRevision || assignment.TaskID != task.ID ||
		assignment.Executor != task.Executor || assignment.ClaimedTaskRevision != publicationRevision ||
		task.StartedAt == nil || !assignment.AssignedAt.Equal(*task.StartedAt) {
		return false
	}
	claimKeys := []string{
		taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
		taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline),
	}
	claim, err := repository.ReadFixedKeys(ctx, claimKeys, readRevision)
	if err != nil {
		return false
	}
	defer etcdstore.ClearValues(claim.Values)
	for index, value := range claim.Values {
		if value == nil || value.Key != claimKeys[index] ||
			value.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(value.Value, assignmentValue.Value) {
			return false
		}
	}
	return true
}
