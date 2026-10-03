package hierarchydeletionfinalization

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Preparer) prepareHierarchyDeletionAttachFinalizer(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation, action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, attachrecord.AttachKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := attachrecord.DecodeAttachRecord(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	inputKey := hierarchydeletionattach.FrozenKey(operation.Tombstone.OperationID, record.ID)
	input, inputRevision, err := hierarchydeletionattach.Read(ctx, repository.store, inputKey)
	if err != nil {
		return Effects{}, err
	}
	defer hierarchydeletionattach.Clear(&input)
	if input.AttachRevision != action.TargetRevision || input.Attach.ID != record.ID ||
		input.Attach.EnvironmentID != record.EnvironmentID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	expected, err := attachrecord.EncodeAttachRecord(input.Attach)
	if err != nil {
		return Effects{}, err
	}
	defer clear(expected)
	if string(expected) != string(primary.Value) {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, []string{
		attachrecord.AttachGrantedByPrefix(record.ID), attachrecord.AttachCredentialByPrefix(record.ID),
	}); err != nil {
		return Effects{}, err
	}
	keys := []string{attachrecord.AttachNameKey(record.EnvironmentID, record.Name),
		attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID),
		attachrecord.AttachServiceKey(record.ServiceID, record.ID),
		attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID),
		attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID)}
	if !record.OwnsCredential() {
		keys = append(keys, attachrecord.AttachCredentialByKey(record.CredentialAttachID, record.ID))
	}
	for _, grantID := range record.GrantAttachIDs {
		keys = append(keys, attachrecord.AttachGrantedByKey(grantID, record.ID))
	}
	indexCount := len(keys)
	keys = append(keys, attachrecord.AttachFactsKey(record.ID), attachrecord.AttachDependentGrantKey(record.ID))
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return Effects{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.ReadRevision <= 0 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	exclusion, err := attachrecord.RequireAttachBackupSourceExclusionAbsent(
		ctx,
		repository.store,
		record.ID,
		read.ReadRevision,
	)
	if err != nil {
		return Effects{}, err
	}
	conditions := []keyvalue.Condition{{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: inputKey, ModRevision: inputRevision}, {Key: deletions.TombstoneKey("attach", record.ID)}, exclusion,
		{Key: attachrecord.AttachGrantedByPrefix(record.ID), Prefix: true},
		{Key: attachrecord.AttachCredentialByPrefix(record.ID), Prefix: true}}
	mutations := make([]keyvalue.Mutation, 0, len(keys)+1)
	for index := 0; index < indexCount; index++ {
		value := read.Values[index]
		if value == nil || value.Key != keys[index] || string(value.Value) != record.ID {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
	}
	facts := read.Values[indexCount]
	if (facts == nil) != (input.Facts == nil) {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if facts != nil {
		encoded, err := attachrecord.EncodeAttachEncryptedFacts(*input.Facts)
		if err != nil {
			return Effects{}, err
		}
		matches := facts.ModRevision == input.FactsRevision && string(facts.Value) == string(encoded)
		clear(encoded)
		if !matches {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		conditions = append(conditions, keyvalue.Condition{Key: facts.Key, ModRevision: facts.ModRevision})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: facts.Key})
	} else {
		conditions = append(conditions, keyvalue.Condition{Key: keys[indexCount]})
	}
	grants := read.Values[indexCount+1]
	if len(record.GrantAttachIDs) == 0 {
		if grants != nil {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[indexCount+1]})
	} else {
		if grants == nil {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if err := attachrecord.ValidateAttachDependentGrantRange(&keyvalue.RangeResult{
			ReadRevision: read.ReadRevision, Values: []keyvalue.KeyValue{*grants},
		}, record.ID, record.GrantAttachIDs); err != nil {
			return Effects{}, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: grants.Key, ModRevision: grants.ModRevision})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: grants.Key})
	}
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: primary.Key})
	return Effects{fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value),
		conditions: conditions, mutations: mutations}, nil
}
