package backup

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func attachDatabaseRecoveryGuard(
	source backupwork.StagingSource,
	nativeRestore *backupruntime.BackupRestoreRecord,
	disposition *agentpb.BackupStagingDisposition,
) error {
	if source.Input == nil || uint64(source.Ordinal) >= uint64(len(source.Input.Steps)) {
		return invalidCaptureStaging()
	}
	step := source.Input.Steps[source.Ordinal]
	serviceID := step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if mysql := step.GetCapture().GetMysql(); mysql != nil {
		serviceID = mysql.DatabaseServiceId
	}
	if restore := step.GetRestore().GetPostgres(); restore != nil {
		serviceID = restore.DatabaseServiceId
	}
	if mysql := step.GetRestore().GetMysql(); mysql != nil {
		serviceID = mysql.DatabaseServiceId
	}
	if serviceID == "" {
		return nil
	}
	if source.Input == nil {
		return invalidCaptureStaging()
	}
	guard := &agentpb.BackupDatabaseStagingGuard{TaskId: source.View.Task.Task.ID, Step: proto.CloneOf(step)}
	guard.TerminalCleanup = (source.View.Item.Item.Phase == workexecution.Settled && (source.View.Item.Item.Cleanup == workexecution.CleanupNone || source.View.Item.Item.Cleanup == workexecution.CleanupComplete)) &&
		disposition.GetDiscardRecovered() != nil
	guard.CompletedTask = guard.TerminalCleanup && source.View.Item.Item.Outcome == workexecution.Succeeded
	for _, fact := range source.Input.Services {
		if fact.ServiceId == serviceID {
			if guard.DatabaseService != nil {
				return invalidCaptureStaging()
			}
			guard.DatabaseService = proto.CloneOf(fact)
		}
	}
	if guard.DatabaseService == nil {
		return invalidCaptureStaging()
	}
	for _, artifact := range source.Input.Artifacts {
		for _, service := range artifact.Services {
			if service.ServiceId == serviceID && service.ComposeName == guard.DatabaseService.CurrentName {
				if guard.DatabaseArtifact != nil {
					return invalidCaptureStaging()
				}
				guard.DatabaseArtifact = proto.CloneOf(artifact)
			}
		}
	}
	if guard.DatabaseArtifact == nil {
		return invalidCaptureStaging()
	}
	if disposition.GetRecoveryRequired() != nil {
		native := nativeRestore
		if native == nil || native.DatabaseProgress == nil {
			return invalidCaptureStaging()
		}
		progress := native.DatabaseProgress
		if progress.ApplyStarted && !progress.SourceCleanupCompleted {
			nonce, err := hex.DecodeString(progress.ApplyExecutionNonce)
			if err != nil {
				return err
			}
			repository, err := hex.DecodeString(progress.ApplyRepositoryDigest)
			if err != nil {
				return err
			}
			labels, err := hex.DecodeString(progress.ApplyLabelsSHA256)
			if err != nil {
				return err
			}
			if step.GetRestore().GetPostgres() != nil {
				guard.RecoveryApply = &agentpb.BackupDatabaseStagingGuard_PostgresRecoveryApply{
					PostgresRecoveryApply: &agentpb.BackupPostgresRestoreApplyStartCheckpoint{
						PointId: native.Point.ID, ExecutionNonce: nonce, ContainerId: progress.ContainerID,
						ExecId: progress.ApplyExecID, RepositoryDigest: repository, ExpectedLabelsSha256: labels,
						SourceSizeBytes: step.GetRestore().ExpectedEvidence.SourceSizeBytes,
						SourceSha256:    append([]byte(nil), step.GetRestore().ExpectedEvidence.SourceSha256...),
					},
				}
			} else if step.GetRestore().GetMysql() != nil {
				guard.RecoveryApply = &agentpb.BackupDatabaseStagingGuard_MysqlRecoveryApply{
					MysqlRecoveryApply: &agentpb.BackupMySQLRestoreApplyStartCheckpoint{
						PointId: native.Point.ID, ExecutionNonce: nonce, ContainerId: progress.ContainerID,
						ExecId: progress.ApplyExecID, ImageReferenceSha256: repository,
						ExpectedLabelsSha256: labels,
						SourceSizeBytes:      step.GetRestore().ExpectedEvidence.SourceSizeBytes,
						SourceSha256:         append([]byte(nil), step.GetRestore().ExpectedEvidence.SourceSha256...),
					},
				}
			} else {
				return invalidCaptureStaging()
			}
		}
	}
	disposition.DatabaseGuard = guard
	return nil
}
