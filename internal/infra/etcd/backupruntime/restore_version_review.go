package backupruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The review binds immutable archive evidence, surviving Attach and all exact
// runtime/consumer revisions. New Task IDs and execution nonces are not inputs.
func RestoreVersionReviewDigest(record BackupRestoreRecord) (string, error) {
	if record.TargetVersions == nil || record.TargetVersions.Validate() != nil {
		return "", errs.New(errs.KindStateConflict, "Restore target versions are unavailable")
	}
	value, err := json.Marshal(struct {
		Point          BackupRecoveryPointSnapshot
		PointRevision  int64
		SourceRevision int64
		Target         BackupRestoreTargetSnapshot
		Versions       databaseversion.Target
	}{record.Point, record.RecoveryPointRevision, record.SourceRevision, record.CurrentTarget, *record.TargetVersions})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func RestoreVersionDifference(record BackupRestoreRecord) bool {
	if record.TargetVersions == nil {
		return false
	}
	versions := record.TargetVersions
	switch record.Point.SourceFormat {
	case BackupRuntimeFormatPostgres:
		return record.Point.PostgresArchive.SourceServerVersion != versions.ServerVersion ||
			record.Point.PostgresArchive.BackupToolVersion != versions.RestoreToolVersion
	case BackupRuntimeFormatMySQL:
		return record.Point.MySQLArchive.SourceServerVersion != versions.ServerVersion ||
			databaseversion.MySQLToolNumber(
				record.Point.MySQLArchive.BackupToolVersion,
			) != databaseversion.MySQLToolNumber(
				versions.RestoreToolVersion,
			)
	default:
		return true
	}
}

func ValidateRestoreVersionReview(record BackupRestoreRecord) error {
	expected, err := RestoreVersionReviewDigest(record)
	if err != nil || expected != record.VersionReviewSHA256 ||
		RestoreVersionDifference(record) && !record.VersionDifferenceAcknowledged {
		return errs.New(errs.KindStateConflict, "Restore version review is missing or changed")
	}
	if record.Point.SourceFormat == BackupRuntimeFormatPostgres && record.TargetVersions.Family != "postgres" ||
		record.Point.SourceFormat == BackupRuntimeFormatMySQL && record.TargetVersions.Family != "mysql" {
		return errs.New(errs.KindStateConflict, "Restore database family changed")
	}
	return nil
}
