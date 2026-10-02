package etcd

import (
	"context"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PublishConfigRestore advances a single complete, already verified candidate.
// Each live Entry mutation commits with its native ordinal. The ordinary head
// moves only after those records and indexes are complete; host materialization
// remains a separate proof, so canonical publication is not Restore completion.
func (repository *BackupRuntimeRepository) PublishConfigRestore(
	ctx context.Context,
	generation etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
	projection environmentprojection.EnvironmentComposeProjection,
	identities environmentprojection.EnvironmentOwnedIdentities,
) error {
	authority, err := repository.loadConfigRestorePublication(ctx, generation)
	if err != nil {
		return err
	}
	metadata, err := prepareConfigRestoreMetadata(generation, authority.root, projection, identities)
	if err != nil {
		return err
	}
	defer metadata.clear()
	if err := repository.stageConfigRestoreMetadata(ctx, generation, metadata); err != nil {
		return err
	}
	owner := generation.Record.Owner
	baseline, found, err := blueprints.ReadEffectiveProjectionRevision(ctx, repository.store,
		owner.EnvironmentID, owner.BaselineRevisionID, authority.revision)
	if err != nil {
		return err
	}
	if !found ||
		environmentprojection.ValidateEnvironmentComposeProjectionAdvance(baseline.Record, true, projection) != nil {
		return configTransferAuthorityConflict()
	}
	defer clear(baseline.Record.ComposeArtifact)
	defer clear(baseline.Record.NormalizedCompose)
	previous := make(map[string]entries.Record, len(baseline.Record.Entries))
	selected := make(map[string]struct{}, len(projection.Entries))
	for _, record := range baseline.Record.Entries {
		previous[record.Entry.ID] = record
	}
	for index, record := range projection.Entries {
		if index > 0 && projection.Entries[index-1].Entry.ID >= record.Entry.ID {
			return configTransferAuthorityConflict()
		}
		selected[record.Entry.ID] = struct{}{}
	}
	var deleted []entries.Record
	for _, record := range baseline.Record.Entries {
		if _, keep := selected[record.Entry.ID]; !keep {
			deleted = append(deleted, record)
		}
	}
	sort.Slice(deleted, func(i, j int) bool { return deleted[i].Entry.ID < deleted[j].Entry.ID })
	if uint32(len(deleted)) != authority.current.Record.ConfigProgress.DeleteEntryCount {
		return configTransferAuthorityConflict()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		authority, err = repository.loadConfigRestorePublication(ctx, generation)
		if err != nil {
			return err
		}
		current := authority.current.Record
		progress := current.ConfigProgress
		switch current.State {
		case backupruntime.BackupRestoreStaged:
			if err := repository.preflightConfigRestoreEntries(ctx, authority, owner, projection.Entries, deleted, previous); err != nil {
				return err
			}
			next := backupruntime.CloneBackupRestoreRecord(current)
			next.MutationStarted, next.State = true, backupruntime.BackupRestoreApplyingDeletes
			if err := repository.commitConfigRestorePublication(ctx, authority, next, nil, nil); err != nil {
				return err
			}
		case backupruntime.BackupRestoreApplyingDeletes:
			if progress.DeleteEntryOrdinal < progress.DeleteEntryCount {
				if err := repository.deleteConfigRestoreEntry(ctx, authority, deleted[progress.DeleteEntryOrdinal]); err != nil {
					return err
				}
				continue
			}
			next := backupruntime.CloneBackupRestoreRecord(current)
			next.State = backupruntime.BackupRestoreApplyingUpserts
			if err := repository.commitConfigRestorePublication(ctx, authority, next, nil, nil); err != nil {
				return err
			}
		case backupruntime.BackupRestoreApplyingUpserts:
			if progress.UpsertEntryOrdinal < current.Point.ConfigArchive.EntryCount {
				record := projection.Entries[progress.UpsertEntryOrdinal]
				prior, exists := previous[record.Entry.ID]
				if err := repository.upsertConfigRestoreEntry(ctx, authority, generation.Record.Owner, record, prior, exists); err != nil {
					return err
				}
				continue
			}
			next := backupruntime.CloneBackupRestoreRecord(current)
			if authority.head == owner.BaselineRevisionID {
				head, err := idempotency.EncodeTaskReference(current.TaskID)
				if err != nil {
					return err
				}
				err = repository.commitConfigRestorePublication(
					ctx,
					authority,
					next,
					nil,
					[]etcdstore.Mutation{
						{
							Type:  etcdstore.MutationPut,
							Key:   blueprints.EnvironmentBlueprintHeadKey(owner.EnvironmentID),
							Value: head,
						},
					},
				)
				clear(head)
				if err != nil {
					return err
				}
				continue
			}
			// etcd assigns the commit revision. Re-read the exact head+native
			// transaction before recording it; an interrupted receipt resumes here.
			next.State, next.ConfigProgress.PublishedHeadRevision = backupruntime.BackupRestoreCanonicalComplete, authority.headRevision
			if err := repository.commitConfigRestorePublication(ctx, authority, next, nil, nil); err != nil {
				return err
			}
		case backupruntime.BackupRestoreCanonicalComplete, backupruntime.BackupRestoreMaterializing,
			backupruntime.BackupRestoreVerified, backupruntime.BackupRestoreCompleted:
			return nil
		default:
			return configTransferAuthorityConflict()
		}
	}
}

func (repository *BackupRuntimeRepository) commitConfigRestorePublication(ctx context.Context,
	authority configRestorePublicationAuthority, next backupruntime.BackupRestoreRecord,
	conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
) error {
	at := time.Now().UTC()
	if !at.After(next.UpdatedAt) {
		at = next.UpdatedAt.Add(time.Nanosecond)
	}
	next.UpdatedAt = at
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return err
	}
	defer clear(value)
	epoch, err := authority.fence.EpochRewriteMutation()
	if err != nil {
		return err
	}
	defer clear(epoch.Value)
	mutations = append(mutations, epoch, etcdstore.Mutation{Type: etcdstore.MutationPut,
		Key: backupruntime.BackupRestoreKey(next.TaskID), Value: value})
	result, err := repository.TransactRuntime(ctx, append(authority.conditions, conditions...), mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return configTransferAuthorityConflict()
	}
	return nil
}
