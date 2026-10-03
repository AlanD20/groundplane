package hierarchydeletionfinalization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Preparer) prepareHierarchyDeletionBackupPolicyFinalizer(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, backuppolicy.BackupPolicyKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := backuppolicy.DecodeBackupPolicyRecord(primary.Value)
	if err != nil || record.EnvironmentID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	indexes := []string(nil)
	if record.ConnectorID != "" {
		indexes = append(
			indexes,
			backuppolicy.BackupPolicyConnectorReferenceKey(record.ConnectorID, record.EnvironmentID),
		)
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, indexes)
	if err != nil {
		return Effects{}, err
	}
	return repository.retireBackupPolicySchedule(ctx, record, effects)
}

func (repository *Preparer) retireBackupPolicySchedule(ctx context.Context,
	policy backuppolicy.BackupPolicyRecord,
	effects Effects,
) (Effects, error) {
	key := environmentcoordination.Key(policy.EnvironmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return Effects{}, err
	}
	if read == nil || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	effects.conditions = append(
		effects.conditions,
		keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)},
	)
	if value == nil {
		if policy.Enabled {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return effects, nil
	}
	record, err := environmentcoordination.Decode(value.Value)
	if err != nil || value.Key != key || record.EnvironmentID != policy.EnvironmentID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	state := record.CurrentBackupScheduleState
	if (state != nil) != policy.Enabled {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if state == nil {
		return effects, nil
	}
	digest, err := environmentcoordination.PolicyScheduleDigest(policy)
	if err != nil || state.PolicyDigest != digest || state.Frequency != policy.Frequency {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	record.CurrentBackupScheduleState = nil
	encoded, err := environmentcoordination.Encode(record)
	if err != nil {
		return Effects{}, err
	}
	effects.mutations = append(
		effects.mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded},
	)
	effects.values = append(effects.values, encoded)
	return effects, nil
}

func (repository *Preparer) prepareHierarchyDeletionBackupKeyFinalizer(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	keys := []string{backuppolicy.BackupKeyKey(action.TargetID), backuppolicy.BackupKeyValueKey(action.TargetID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return Effects{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].ModRevision != action.TargetRevision {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	metadata, metadataErr := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	encrypted, encryptedErr := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[1].Value)
	if metadataErr != nil || encryptedErr != nil || metadata.EnvironmentID != action.TargetID ||
		encrypted.EnvironmentID != action.TargetID || metadata.KeyEra != encrypted.KeyEra {
		clear(encrypted.Ciphertext)
		keyvalue.ClearValues(read.Values)
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	clear(encrypted.Ciphertext)
	return Effects{fixedInputDigest: hierarchydeletion.HierarchyDeletionBackupKeyDigest(
		read.Values[0].Value, read.Values[1].Value), conditions: []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
	}, mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: keys[0]},
		{Type: keyvalue.MutationDelete, Key: keys[1]},
	}, values: [][]byte{read.Values[0].Value, read.Values[1].Value}}, nil
}
