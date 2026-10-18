package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// preserveConfigHeadReceipt closes the head-switch/receipt interruption before
// failure stamping changes the Restore record's revision. Only the exact atomic
// head+Restore commit can prove publication; a newer head is never inferred.
func (repository *InputRepository) preserveConfigHeadReceipt(ctx context.Context,
	current backupruntime.BackupRestoreRecord, revision int64,
) (backupruntime.BackupRestoreRecord, []keyvalue.Condition, error) {
	if current.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig || !current.MutationStarted ||
		current.ConfigProgress == nil || current.ConfigProgress.PublishedHeadRevision != 0 {
		return current, nil, nil
	}
	keys := []string{backupruntime.BackupRestoreKey(current.TaskID), blueprints.EnvironmentBlueprintHeadKey(current.EnvironmentID)}
	read, err := repository.readSettlementKeys(ctx, keys, revision)
	if err != nil {
		return backupruntime.BackupRestoreRecord{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return backupruntime.BackupRestoreRecord{}, nil, conflict("Config Restore publication evidence is missing")
	}
	stored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || !backupruntime.BackupRestoreRecordsEqual(stored, current) {
		return backupruntime.BackupRestoreRecord{}, nil, conflict("Config Restore publication cursor changed")
	}
	head, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil {
		return backupruntime.BackupRestoreRecord{}, nil, err
	}
	guards := []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
	}
	target := current.CurrentTarget.Config
	if head == target.BaselineRevisionID && read.Values[1].ModRevision == target.BaselineHeadRevision {
		return current, guards, nil
	}
	progress := current.ConfigProgress
	if head != current.TaskID || current.State != backupruntime.BackupRestoreApplyingUpserts ||
		progress.DeleteEntryOrdinal != progress.DeleteEntryCount || progress.UpsertEntryOrdinal != current.Point.ConfigArchive.EntryCount ||
		read.Values[1].ModRevision != read.Values[0].ModRevision || read.Values[1].ModRevision <= progress.RevisionRootRevision {
		return backupruntime.BackupRestoreRecord{}, nil, conflict("Config Restore head lacks its exact publication commit")
	}
	next := backupruntime.CloneBackupRestoreRecord(current)
	next.State = backupruntime.BackupRestoreCanonicalComplete
	next.ConfigProgress.PublishedHeadRevision = read.Values[1].ModRevision
	if err := backupruntime.ValidateBackupRestoreRecord(next); err != nil {
		return backupruntime.BackupRestoreRecord{}, nil, err
	}
	return next, guards, nil
}
