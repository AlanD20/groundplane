package backupwork

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-102: native resource projection persists the observed dump versions and
// keeps them through artifact preparation. The native transition owner rejects
// changed versions before this projection; its behavior is checked separately.
func TestCaptureProgressPreservesPinnedDatabaseArchive(t *testing.T) {
	for _, family := range []string{"postgres", "mysql"} {
		t.Run(family, func(t *testing.T) {
			source := backupruntime.BackupRunSourceAttemptRecord{
				Kind:  backupruntime.BackupRuntimeSourceAttach,
				State: backupruntime.BackupSourceAttemptCapturing, Phase: backupruntime.BackupSourcePhaseCapture}
			dump := &agentpb.BackupCaptureResume{}
			if family == "postgres" {
				source.Format = backupruntime.BackupRuntimeFormatPostgres
				dump.Checkpoint = &agentpb.BackupCaptureResume_PostgresDumpStart{
					PostgresDumpStart: &agentpb.BackupPostgresDumpStart{Archive: &agentpb.BackupPostgresArchiveEvidence{
						PgDumpMajor: 16, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9"}}}
			} else {
				source.Format = backupruntime.BackupRuntimeFormatMySQL
				dump.Checkpoint = &agentpb.BackupCaptureResume_MysqlDumpStartCheckpoint{
					MysqlDumpStartCheckpoint: &agentpb.BackupMySQLDumpStart{Archive: &agentpb.BackupMySQLArchiveEvidence{
						AdapterContractVersion: 1, SourceServerVersion: "8.4.5",
						BackupToolVersion: "mysqldump  Ver 8.4.5 for Linux on x86_64", ArtifactFormat: "mysql-logical-v1"}}}
			}
			if err := projectCaptureProgress(&source, dump, backupruntime.BackupRunRecord{}, CheckpointEvidence{}); err != nil {
				t.Fatal(err)
			}
			if source.State != backupruntime.BackupSourceAttemptReady ||
				source.Phase != backupruntime.BackupSourcePhaseStaging ||
				family == "postgres" && source.PostgresArchive.SourceServerVersion != "16.9" ||
				family == "mysql" && source.MySQLArchive.SourceServerVersion != "8.4.5" {
				t.Fatal("dump boundary failed to persist observed versions")
			}
			postgres, mysql := source.PostgresArchive, source.MySQLArchive
			prepared := &agentpb.BackupArtifactPrepared{
				Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: 4096},
			}
			cursor := &agentpb.BackupCaptureResume{PreparedArtifact: prepared,
				Checkpoint: &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: prepared}}
			if err := projectCaptureProgress(&source, cursor, backupruntime.BackupRunRecord{}, CheckpointEvidence{}); err != nil {
				t.Fatal(err)
			}
			if source.PostgresArchive != postgres || source.MySQLArchive != mysql ||
				source.Evidence.SourceSizeBytes != 4096 {
				t.Fatal("artifact preparation replaced dump metadata or lost its byte evidence")
			}
		})
	}
}
