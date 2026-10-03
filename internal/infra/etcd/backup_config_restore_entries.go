package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
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
		if _, err := entries.PrepareProjectedEntryReplacement(ctx, repository.store,
			authority.current.Record.EnvironmentID, authority.revision, record, record, true); err != nil {
			return err
		}
		if _, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, record.Entry.ID, authority.revision); err != nil {
			return err
		}
	}
	for index, record := range restored {
		prior, existed := previous[record.Entry.ID]
		if _, err := entries.PrepareProjectedEntryReplacement(ctx, repository.store,
			authority.current.Record.EnvironmentID, authority.revision, record, prior, existed); err != nil {
			return err
		}
		if _, err := backupconfiguration.PrepareConfigRestoreEntryPublication(ctx, repository.store, owner,
			uint32(index+1), record, authority.revision); err != nil {
			return err
		}
	}
	return nil
}

func (repository *BackupRuntimeRepository) deleteConfigRestoreEntry(ctx context.Context,
	authority configRestorePublicationAuthority, record entries.Record,
) error {
	conditions, err := entries.PrepareProjectedEntryReplacement(ctx, repository.store,
		authority.current.Record.EnvironmentID, authority.revision, record, record, true)
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
	// Keep the predecessor readable until the complete desired head switches.
	return repository.commitConfigRestorePublication(ctx, authority, next, conditions, nil)
}

func (repository *BackupRuntimeRepository) upsertConfigRestoreEntry(ctx context.Context,
	authority configRestorePublicationAuthority, owner backupconfiguration.ConfigRestoreTransferOwner,
	record, previous entries.Record, existed bool,
) error {
	conditions, err := entries.PrepareProjectedEntryReplacement(ctx, repository.store,
		authority.current.Record.EnvironmentID, authority.revision, record, previous, existed)
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
	next := backupruntime.CloneBackupRestoreRecord(authority.current.Record)
	next.ConfigProgress.UpsertEntryOrdinal = ordinal
	return repository.commitConfigRestorePublication(ctx, authority, next, conditions, []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID,
			Value: []byte(record.EnvironmentID),
		},
	})
}

// Removed lookup routes and values can be retired only after the canonical
// head no longer exposes them. Each exact deletion is replayable under the
// original Restore fence; a reconnect repeats the bounded list before return.
func (repository *BackupRuntimeRepository) cleanupConfigRestoreEntries(ctx context.Context,
	generation etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord], deleted []entries.Record,
) error {
	for _, record := range deleted {
		authority, err := repository.loadConfigRestorePublication(ctx, generation)
		if err != nil {
			return err
		}
		if authority.head != authority.current.Record.TaskID ||
			authority.current.Record.ConfigProgress.PublishedHeadRevision == 0 {
			return configTransferAuthorityConflict()
		}
		conditions, mutations, err := entries.PrepareProjectedEntryRetirement(
			ctx,
			repository.store,
			record,
			authority.revision,
		)
		if err != nil {
			return err
		}
		if err := repository.commitConfigRestorePublication(ctx, authority,
			backupruntime.CloneBackupRestoreRecord(authority.current.Record), conditions, mutations); err != nil {
			return err
		}
	}
	return nil
}
