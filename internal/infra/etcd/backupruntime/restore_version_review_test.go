package backupruntime

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
)

// A cross-version acknowledgement must not authorize another Point, runtime
// or target image. Replaying one unchanged review must remain deterministic.
func TestRestoreVersionReviewAcknowledgementIsBoundToExactSelection(t *testing.T) {
	for _, family := range []string{"postgres", "mysql"} {
		t.Run(family, func(t *testing.T) {
			record := BackupRestoreRecord{
				RecoveryPointRevision: 12, SourceRevision: 13,
				TargetVersions: &databaseversion.Target{Family: family,
					ContainerID: strings.Repeat("a", 64), ImageID: "sha256:" + strings.Repeat("b", 64)},
			}
			if family == "postgres" {
				record.CurrentTarget.Postgres = &BackupRestorePostgresTarget{
					Consumers:        []BackupRestoreDatabaseServiceSnapshot{},
					DependentIndexes: []BackupRestoreDatabaseDependentIndex{},
				}
				record.Point.SourceFormat = BackupRuntimeFormatPostgres
				record.Point.PostgresArchive = backuppostgres.ArchiveEvidence{
					SourceServerVersion: "16.8", BackupToolVersion: "16.8",
				}
				record.TargetVersions.ServerVersion, record.TargetVersions.RestoreToolVersion = "16.9", "16.9"
			} else {
				record.CurrentTarget.MySQL = &BackupRestoreMySQLTarget{
					Consumers:        []BackupRestoreDatabaseServiceSnapshot{},
					DependentIndexes: []BackupRestoreDatabaseDependentIndex{},
				}
				record.Point.SourceFormat = BackupRuntimeFormatMySQL
				record.Point.MySQLArchive = backupmysql.ArchiveEvidence{
					SourceServerVersion: "8.4.4", BackupToolVersion: "mysqldump  Ver 8.4.4 for Linux on x86_64",
				}
				record.TargetVersions.ServerVersion = "8.4.5"
				record.TargetVersions.RestoreToolVersion = "mysql  Ver 8.4.5 for Linux on x86_64"
			}
			var err error
			record.VersionReviewSHA256, err = RestoreVersionReviewDigest(record)
			if err != nil {
				t.Fatal(err)
			}
			if !RestoreVersionDifference(record) || ValidateRestoreVersionReview(record) == nil {
				t.Fatal("version difference must require acknowledgement before Restore")
			}
			record.VersionDifferenceAcknowledged = true
			if err := ValidateRestoreVersionReview(record); err != nil {
				t.Fatalf("acknowledged selection rejected: %v", err)
			}
			replay := CloneBackupRestoreRecord(record)
			replay.TaskID, replay.OperationID = "new-task", "new-operation"
			if err := ValidateRestoreVersionReview(replay); err != nil {
				t.Fatalf("same selection with a new Task rejected: %v", err)
			}
			for name, change := range map[string]func(*BackupRestoreRecord){
				"point revision":  func(value *BackupRestoreRecord) { value.RecoveryPointRevision++ },
				"source revision": func(value *BackupRestoreRecord) { value.SourceRevision++ },
				"container":       func(value *BackupRestoreRecord) { value.TargetVersions.ContainerID = strings.Repeat("c", 64) },
				"image":           func(value *BackupRestoreRecord) { value.TargetVersions.ImageID = "sha256:" + strings.Repeat("d", 64) },
				"artifact":        func(value *BackupRestoreRecord) { value.Point.Evidence.SourceSHA256 = strings.Repeat("e", 64) },
			} {
				t.Run(name, func(t *testing.T) {
					changed := CloneBackupRestoreRecord(record)
					change(&changed)
					if ValidateRestoreVersionReview(changed) == nil {
						t.Fatal("old acknowledgement authorized a changed selection")
					}
				})
			}
		})
	}
}

// Different executable names are expected; only their observed version
// numbers participate in the MySQL dump/client compatibility comparison.
func TestMySQLRestoreReviewDoesNotTreatToolNameAsVersionDifference(t *testing.T) {
	record := BackupRestoreRecord{
		Point: BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: BackupRecoveryPointTargetSnapshot{
				SourceFormat: BackupRuntimeFormatMySQL,
			},
			MySQLArchive: backupmysql.ArchiveEvidence{SourceServerVersion: "8.4.5",
				BackupToolVersion: "mysqldump  Ver 8.4.5 for Linux on x86_64"},
		},
		TargetVersions: &databaseversion.Target{Family: "mysql", ServerVersion: "8.4.5",
			RestoreToolVersion: "mysql  Ver 8.4.5 for Linux on x86_64"},
	}
	if RestoreVersionDifference(record) {
		t.Fatal("matching MySQL versions reported as different because tool names differ")
	}
	record.TargetVersions.RestoreToolVersion = "mysql  Ver 8.4.6 for Linux on x86_64"
	if !RestoreVersionDifference(record) {
		t.Fatal("changed restore client version was not reported")
	}
}
