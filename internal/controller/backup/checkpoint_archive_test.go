package backup

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DumpStart must persist observed versions before the dump is permitted to
// execute. Prepared may prove those bytes, never relabel them with new versions.
func TestDatabaseDumpStartPinsArchiveThroughArtifactPreparation(t *testing.T) {
	for _, family := range []string{"postgres", "mysql"} {
		t.Run(family, func(t *testing.T) {
			source := backupruntime.BackupRunSourceAttemptRecord{
				Kind: backupruntime.BackupRuntimeSourceAttach, RecoveryPointID: "point",
				State: backupruntime.BackupSourceAttemptCapturing, Phase: backupruntime.BackupSourcePhaseCapture,
			}
			start := &agentpb.BackupCheckpointRequest{}
			prepared := &agentpb.BackupArtifactPrepared{PointId: "point", Evidence: &agentpb.BackupArtifactEvidence{}}
			if family == "postgres" {
				source.Format = backupruntime.BackupRuntimeFormatPostgres
				archive := &agentpb.BackupPostgresArchiveEvidence{PgDumpMajor: 16, AdapterContractVersion: 1,
					SourceServerVersion: "16.9", BackupToolVersion: "16.9"}
				start.Checkpoint = &agentpb.BackupCheckpointRequest_PostgresDumpStart{
					PostgresDumpStart: &agentpb.BackupPostgresDumpStart{PointId: "point", Archive: archive},
				}
				prepared.Archive = &agentpb.BackupArtifactPrepared_Postgres{Postgres: archive}
			} else {
				source.Format = backupruntime.BackupRuntimeFormatMySQL
				archive := &agentpb.BackupMySQLArchiveEvidence{AdapterContractVersion: 1,
					SourceServerVersion: "8.4.5", BackupToolVersion: "mysqldump  Ver 8.4.5 for Linux on x86_64",
					ArtifactFormat: "mysql-logical-v1"}
				start.Checkpoint = &agentpb.BackupCheckpointRequest_MysqlDumpStart{
					MysqlDumpStart: &agentpb.BackupMySQLDumpStart{PointId: "point", Archive: archive},
				}
				prepared.Archive = &agentpb.BackupArtifactPrepared_Mysql{Mysql: archive}
			}
			if err := applyBackupCheckpointTransition(&source, start, backupruntime.BackupRunRecord{}); err != nil {
				t.Fatal(err)
			}
			if source.State != backupruntime.BackupSourceAttemptReady ||
				source.Phase != backupruntime.BackupSourcePhaseStaging {
				t.Fatal("dump start did not advance its durable source")
			}
			if family == "postgres" && source.PostgresArchive.SourceServerVersion != "16.9" ||
				family == "mysql" && source.MySQLArchive.SourceServerVersion != "8.4.5" {
				t.Fatal("dump start did not persist observed versions")
			}
			ready := source
			request := &agentpb.BackupCheckpointRequest{
				Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{ArtifactPrepared: prepared},
			}
			if err := applyBackupCheckpointTransition(&source, request, backupruntime.BackupRunRecord{}); err != nil {
				t.Fatalf("matching Prepared rejected: %v", err)
			}
			if source.PostgresArchive != ready.PostgresArchive || source.MySQLArchive != ready.MySQLArchive {
				t.Fatal("Prepared changed pinned metadata")
			}
			if family == "postgres" {
				prepared.GetPostgres().SourceServerVersion = "16.10"
			} else {
				prepared.GetMysql().SourceServerVersion = "8.4.6"
			}
			if err := applyBackupCheckpointTransition(&ready, request, backupruntime.BackupRunRecord{}); err == nil {
				t.Fatal("Prepared relabeled captured bytes with different versions")
			}
		})
	}
}
