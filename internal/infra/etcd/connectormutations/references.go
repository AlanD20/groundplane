package connectormutations

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func RequireConnectorReferencesUnused(
	ctx context.Context,
	store persistenceStore,
	connectorID string,
	environmentID string,
	revision int64,
) ([]etcdstore.Condition, error) {
	prefixes := []string{
		backuppolicy.BackupPolicyConnectorReferencePrefix(connectorID),
		backupruntime.BackupRecoveryPointConnectorPrefix + connectorID + "/",
		backupruntime.BackupOrphanConnectorPrefix + connectorID + "/",
	}
	conditions := make([]etcdstore.Condition, 0, len(prefixes))
	for index, prefix := range prefixes {
		result, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, Limit: 1, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if result == nil ||
			result.ReadRevision != revision ||
			len(result.Values) > 1 {
			return nil, errs.New(errs.KindInternal, "connector reference prefix read is incomplete")
		}
		if len(result.Values) != 0 {
			return nil, ClassifyConnectorReference(index, result.Values[0], connectorID, environmentID)
		}
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return conditions, nil
}

func ClassifyConnectorReference(
	index int,
	value etcdstore.KeyValue,
	connectorID string,
	environmentID string,
) error {
	switch index {
	case 0:
		expectedKey := backuppolicy.BackupPolicyConnectorReferenceKey(connectorID, environmentID)
		if value.Key != expectedKey ||
			string(value.Value) != environmentID {
			return errs.New(errs.KindInternal, "connector reference prefix is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by an enabled backup policy")
	case 1:
		recoveryPointID := string(value.Value)
		expectedKey, err := backupruntime.BackupRecoveryPointConnectorIndexKey(connectorID, recoveryPointID)
		if err != nil ||
			value.Key != expectedKey {
			return errs.New(errs.KindInternal, "connector recovery point reference is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by a recovery point")
	case 2:
		recoveryPointID := string(value.Value)
		expectedKey, err := backupruntime.BackupOrphanConnectorIndexKey(connectorID, recoveryPointID)
		if err != nil ||
			value.Key != expectedKey {
			return errs.New(errs.KindInternal, "connector orphan reference is corrupt")
		}
		return errs.New(errs.KindResourceInUse, "connector is referenced by a backup orphan")
	default:
		return errs.New(errs.KindInternal, "connector reference kind is invalid")
	}
}
