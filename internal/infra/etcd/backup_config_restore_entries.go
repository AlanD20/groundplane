package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Verify the complete set at one view before recording live mutation intent.
// The operation lock/epoch then fences intervening admissions; each actual
// delete/upsert repeats its own exact comparisons with the journaled ordinal.
func (repository *BackupRuntimeRepository) preflightConfigRestoreEntries(ctx context.Context,
	authority configRestorePublicationAuthority, owner backupconfiguration.ConfigRestoreTransferOwner,
	restored, deleted []entries.Record, previous map[string]entries.Record,
) error {
	for _, record := range deleted {
		if _, err := repository.configRestoreEntryConditions(ctx, authority, record, record, true); err != nil {
			return err
		}
		if _, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, record.Entry.ID, authority.revision); err != nil {
			return err
		}
	}
	for index, record := range restored {
		prior, existed := previous[record.Entry.ID]
		if _, err := repository.configRestoreEntryConditions(ctx, authority, record, prior, existed); err != nil {
			return err
		}
		if _, err := backupconfiguration.PrepareConfigRestoreEntryPublication(ctx, repository.store, owner,
			uint32(index+1), record, authority.revision); err != nil {
			return err
		}
	}
	return nil
}

func (repository *BackupRuntimeRepository) configRestoreEntryConditions(ctx context.Context,
	authority configRestorePublicationAuthority, record, previous entries.Record, existed bool,
) ([]etcdstore.Condition, error) {
	keys := []string{entries.RecordKey(record.Entry.ID), entries.EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
		entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID,
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), record.Entry.ID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: authority.revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != authority.revision || len(read.Values) != len(keys) {
		return nil, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return nil, configTransferAuthorityConflict()
		}
	}
	if read.Values[3] != nil {
		return nil, configTransferAuthorityConflict()
	}
	if existed {
		if read.Values[0] == nil || read.Values[1] == nil || string(read.Values[1].Value) != record.Entry.ID {
			return nil, configTransferAuthorityConflict()
		}
		current, err := entries.DecodeRecord(read.Values[0].Value)
		if err != nil || !entries.EqualRecord(current, previous) {
			return nil, configTransferAuthorityConflict()
		}
	} else if read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil {
		return nil, configTransferAuthorityConflict()
	}
	if read.Values[2] != nil && string(read.Values[2].Value) != record.EnvironmentID {
		return nil, configTransferAuthorityConflict()
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key, ModRevision: revisionOf(read.Values[index])}
	}
	return conditions, nil
}

func (repository *BackupRuntimeRepository) deleteConfigRestoreEntry(ctx context.Context,
	authority configRestorePublicationAuthority, record entries.Record,
) error {
	conditions, err := repository.configRestoreEntryConditions(ctx, authority, record, record, true)
	if err != nil {
		return err
	}
	more, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, record.Entry.ID, authority.revision)
	if err != nil {
		return err
	}
	conditions = append(conditions, more...)
	next := backupruntime.CloneBackupRestoreRecord(authority.current.Record)
	next.ConfigProgress.DeleteEntryOrdinal++
	return repository.commitConfigRestorePublication(ctx, authority, next, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: entries.RecordKey(record.Entry.ID)},
		{Type: etcdstore.MutationDelete, Key: entries.EntryOwnerKey(record.EnvironmentID, record.Entry.ID)},
		{Type: etcdstore.MutationDelete, Key: entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID},
		{Type: etcdstore.MutationDelete, Key: entryvalues.PlainPrefix + record.Entry.ID + "/", Prefix: true},
		{Type: etcdstore.MutationDelete, Key: entryvalues.SecretPrefix + record.Entry.ID + "/", Prefix: true},
	})
}

func (repository *BackupRuntimeRepository) upsertConfigRestoreEntry(ctx context.Context,
	authority configRestorePublicationAuthority, owner backupconfiguration.ConfigRestoreTransferOwner,
	record, previous entries.Record, existed bool,
) error {
	conditions, err := repository.configRestoreEntryConditions(ctx, authority, record, previous, existed)
	if err != nil {
		return err
	}
	ordinal := authority.current.Record.ConfigProgress.UpsertEntryOrdinal + 1
	more, err := backupconfiguration.PrepareConfigRestoreEntryPublication(
		ctx,
		repository.store,
		owner,
		ordinal,
		record,
		authority.revision,
	)
	if err != nil {
		return err
	}
	conditions = append(conditions, more...)
	value, err := entries.EncodeRecord(record)
	if err != nil {
		return err
	}
	defer clear(value)
	next := backupruntime.CloneBackupRestoreRecord(authority.current.Record)
	next.ConfigProgress.UpsertEntryOrdinal = ordinal
	return repository.commitConfigRestorePublication(ctx, authority, next, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: entries.RecordKey(record.Entry.ID), Value: value},
		{
			Type:  etcdstore.MutationPut,
			Key:   entries.EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
			Value: []byte(record.Entry.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID,
			Value: []byte(record.EnvironmentID),
		},
	})
}
