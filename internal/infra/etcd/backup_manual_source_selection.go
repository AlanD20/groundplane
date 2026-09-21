package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) prepareManualBackupSource(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	source backuppolicy.BackupSourceRecord,
	sourceRevision int64,
	ordinal uint32,
	fixedRevision int64,
	resolvePostgres BackupPostgresIdentityResolver,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	pointID := ids.New(ids.KindRecoveryPoint)
	pointCreatedAt, err := ids.Timestamp(ids.KindRecoveryPoint, pointID)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	attempt := backupruntime.BackupRunSourceAttemptRecord{
		Ordinal:                ordinal,
		SourceID:               source.ID,
		Kind:                   backupruntime.BackupRuntimeSourceKind(source.Kind),
		TargetID:               source.TargetID,
		SourceRevision:         sourceRevision,
		RecoveryPointID:        pointID,
		RecoveryPointCreatedAt: pointCreatedAt,
		ObjectKey:              run.ConnectorPrefix + run.EnvironmentID + "/" + source.ID + "/" + pointID + "/artifact.bin",
		State:                  backupruntime.BackupSourceAttemptPending,
		Phase:                  backupruntime.BackupSourcePhaseCapture,
	}
	switch attempt.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		return repository.prepareManualPostgresSource(
			ctx,
			attempt,
			run.EnvironmentID,
			fixedRevision,
			resolvePostgres,
		)
	case backupruntime.BackupRuntimeSourceVolume:
		return repository.prepareManualVolumeSource(ctx, attempt, run.EnvironmentID, fixedRevision)
	case backupruntime.BackupRuntimeSourceConfig:
		if run.Encryption != backupruntime.BackupRuntimeEncryptionAge || source.TargetID != run.EnvironmentID {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
				errs.KindStateConflict, "config backup source requires age encryption",
			)
		}
		read, readErr := repository.ReadFixedKeys(
			ctx,
			[]string{hierarchyrecord.EnvironmentKey(run.EnvironmentID)},
			fixedRevision,
		)
		if readErr != nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, readErr
		}
		defer etcdstore.ClearValues(read.Values)
		if read.Values[0] == nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
				errs.KindStateConflict,
				"config target is unavailable",
			)
		}
		attempt.TargetRevision = read.Values[0].ModRevision
		attempt.Format = backupruntime.BackupRuntimeFormatConfig
		attempt.Snapshot.Config = &backupruntime.BackupConfigSourceSnapshot{
			ConfigSnapshotID: run.TaskID, ReadRevision: fixedRevision,
		}
		return attempt, nil
	default:
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStrategyNotImplemented, "backup source strategy is not implemented",
		)
	}
}
