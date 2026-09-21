package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *BackupPolicyRepository) loadVolumeRemovalPolicySources(
	ctx context.Context,
	state *volumeRemovalBackupPolicyState,
	revision int64,
) error {
	policy := state.policy
	if len(policy.SourceIDs) != 0 {
		keys := make([]string, len(policy.SourceIDs))
		for index, sourceID := range policy.SourceIDs {
			keys[index] = backuppolicy.BackupSourceKey(sourceID)
		}
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
		defer etcdstore.ClearValues(read.Values)
		for index, value := range read.Values {
			if value == nil {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
			source, err := backuppolicy.DecodeBackupSourceRecord(value.Value)
			if err != nil || source.ID != policy.SourceIDs[index] || source.EnvironmentID != state.environmentID {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
			state.sources = append(state.sources, source)
			state.conditions = append(
				state.conditions,
				etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision},
			)
		}
	}
	keys := make([]string, 0, len(state.sources)*2+1)
	values := make([]string, 0, len(state.sources)*2+1)
	for _, source := range state.sources {
		keys = append(keys, backuppolicy.BackupSourceEnvironmentKey(state.environmentID, source.ID),
			backuppolicy.BackupSourceIdentityKey(state.environmentID, source.Kind, source.TargetID))
		values = append(values, source.ID, source.ID)
	}
	if policy.ConnectorID != "" {
		keys = append(keys, backuppolicy.BackupPolicyConnectorReferenceKey(policy.ConnectorID, state.environmentID))
		expected := ""
		if policy.Enabled {
			expected = state.environmentID
		}
		values = append(values, expected)
	}
	if len(keys) == 0 {
		return nil
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		condition := etcdstore.Condition{Key: keys[index]}
		if values[index] == "" {
			if value != nil {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
		} else {
			if value == nil || string(value.Value) != values[index] {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
			condition.ModRevision = value.ModRevision
		}
		// Source primary records are compared individually above. Catalog index
		// creation is serialized by the Environment mutation epoch already held
		// by this preparation; retirement also changes the primary. Repeating
		// two index comparisons per immutable source would exceed ADR0051's
		// final partition budget without adding source identity authority.
		if policy.ConnectorID != "" && index == len(keys)-1 {
			state.conditions = append(state.conditions, condition)
		}
	}
	return nil
}
