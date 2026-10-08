package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DatabaseVersionObserver func(context.Context, backupplanning.DatabaseRestoreSelection) (databaseversion.Target, error)

func (service *RestoreService) PreviewRestore(
	ctx context.Context,
	environmentID string,
	request apiTypes.RestoreRequest,
) (apiTypes.RestorePreview, error) {
	if service == nil || ctx == nil {
		return apiTypes.RestorePreview{}, errs.New(errs.KindInternal, "Restore is not configured")
	}
	identity, intent, err := service.prepareRestoreIntent(ctx, environmentID, request)
	if err != nil {
		return apiTypes.RestorePreview{}, err
	}
	defer clear(identity)
	defer intent.Destroy()
	read, err := service.runtime.ReadCurrentKeys(ctx, []string{backuppolicy.BackupSourceKey(request.SourceID)})
	if err != nil {
		return apiTypes.RestorePreview{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 1 || read.Values[0] == nil {
		return apiTypes.RestorePreview{}, errs.New(errs.KindStateConflict, "Restore source is unavailable")
	}
	source, err := backuppolicy.DecodeBackupSourceRecord(read.Values[0].Value)
	if err != nil || source.EnvironmentID != environmentID {
		return apiTypes.RestorePreview{}, errs.New(
			errs.KindStateConflict,
			"Restore source does not belong to this Environment",
		)
	}
	if source.Kind != core.BackupSourceAttach {
		prepared, err := service.prepareRestore(
			ctx,
			environmentID,
			ids.New(ids.KindTask),
			ids.New(ids.KindOperation),
			request,
			time.Now().UTC().Truncate(time.Millisecond),
			len(identity) != 0,
			true,
		)
		if err != nil {
			return apiTypes.RestorePreview{}, err
		}
		defer prepared.Publication.Clear()
		return apiTypes.RestorePreview{RecoveryPointID: prepared.Restore.Point.ID}, nil
	}
	// The selector fixes the exact Point, Attach, consumers and acknowledged
	// runtime. It performs reads only; no Task, lock or publication is created.
	selected, err := service.runtime.PrepareDatabaseRestoreSelection(ctx, backupplanning.DatabaseRestoreSelectionInput{
		EnvironmentID: environmentID, SourceID: request.SourceID, RecoveryPointID: request.RecoveryPointID,
		TaskID: ids.New(
			ids.KindTask,
		), OperationID: ids.New(ids.KindOperation), CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
		UsesOldIdentity: len(identity) != 0, ResolveDatabase: service.resolveDatabase,
		ResolveServiceFact: service.serviceFacts, ResolveVersions: service.versionObserver, Preview: true,
		FixedRevision: read.ReadRevision,
	})
	if err != nil {
		return apiTypes.RestorePreview{}, err
	}
	return databaseRestorePreview(selected.Restore), nil
}

func databaseRestorePreview(record backupruntime.BackupRestoreRecord) apiTypes.RestorePreview {
	versions := record.TargetVersions
	review := &apiTypes.RestoreDatabaseReview{Family: versions.Family, TargetServerVersion: versions.ServerVersion,
		RestoreToolVersion: versions.RestoreToolVersion, ReviewSHA256: record.VersionReviewSHA256,
		ArtifactFormat: string(
			record.Point.SourceFormat,
		), VersionDifference: backupruntime.RestoreVersionDifference(record), Compatibility: "same-version"}
	if versions.Family == "postgres" {
		review.SourceServerVersion, review.BackupToolVersion = record.Point.PostgresArchive.SourceServerVersion, record.Point.PostgresArchive.BackupToolVersion
	} else {
		review.SourceServerVersion, review.BackupToolVersion = record.Point.MySQLArchive.SourceServerVersion, record.Point.MySQLArchive.BackupToolVersion
	}
	if review.VersionDifference {
		review.Compatibility = "unverified"
	}
	return apiTypes.RestorePreview{RecoveryPointID: record.Point.ID, Database: review}
}
