package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type historyMembershipReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// HistoryMembershipRetentionGuards is used only after native execution and all
// recovery effects have settled. History remains attached while its Environment
// exists. Hierarchy removal detaches that membership before deleting the parent;
// collection then requires both to be absent, at one guarded revision. Existing
// hierarchy-deletion fences must also join the collection transaction.
func HistoryMembershipRetentionGuards(ctx context.Context, store historyMembershipReader,
	kind BackupOperationKind, environmentID, taskID string, revision int64,
) ([]keyvalue.Condition, error) {
	var index string
	var err error
	switch kind {
	case BackupOperationBackup:
		index, err = BackupRunEnvironmentIndexKey(environmentID, taskID)
	case BackupOperationRestore:
		index, err = BackupRestoreEnvironmentIndexKey(environmentID, taskID)
	case BackupOperationRotation:
		index, err = BackupKeyRotationEnvironmentIndexKey(environmentID, taskID)
	default:
		return nil, errs.New(errs.KindValidationFailed, "Backup history membership kind is invalid")
	}
	if err != nil {
		return nil, err
	}
	if store == nil || revision <= 0 {
		return nil, errs.New(errs.KindStateConflict, "Backup history requires a fixed membership snapshot")
	}
	keys := []string{index, hierarchy.EnvironmentKey(environmentID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, CorruptBackupRuntimeRecord()
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		conditions[index].Key = keys[index]
		if value == nil {
			continue
		}
		if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return nil, CorruptBackupRuntimeRecord()
		}
		conditions[index].ModRevision = value.ModRevision
	}
	member, environment := read.Values[0], read.Values[1]
	if (member == nil) != (environment == nil) {
		return nil, errs.New(
			errs.KindStateConflict,
			"Backup history membership and Environment removal have not settled",
		)
	}
	if member != nil {
		parent, err := hierarchy.DecodeEnvironment(environment.Value)
		if err != nil || parent.ID != environmentID || member.Version != 1 || string(member.Value) != taskID {
			return nil, CorruptBackupRuntimeRecord()
		}
	}
	return conditions, nil
}
