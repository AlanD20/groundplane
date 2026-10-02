package agentchannel

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func attachPostgresStagingGuard(source etcd.BackupStagingSource, disposition *agentpb.BackupStagingDisposition) error {
	serviceID := source.Step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if restore := source.Step.GetRestore().GetPostgres(); restore != nil {
		serviceID = restore.DatabaseServiceId
	}
	if serviceID == "" {
		return nil
	}
	if source.Plan == nil || source.Plan.BackupScope == nil {
		return unresolvedBackupStage()
	}
	guard := &agentpb.BackupPostgresStagingGuard{TaskId: source.Task.Record.ID, Step: proto.CloneOf(source.Step)}
	guard.TerminalCleanup = taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) &&
		disposition.GetDiscardRecovered() != nil
	guard.CompletedTask = source.Task.Record.Status == taskjournal.TaskStatusCompleted
	for _, fact := range source.Plan.BackupScope.Services {
		if fact.ServiceId == serviceID {
			if guard.DatabaseService != nil {
				return unresolvedBackupStage()
			}
			guard.DatabaseService = proto.CloneOf(fact)
		}
	}
	if guard.DatabaseService == nil {
		return unresolvedBackupStage()
	}
	for _, artifact := range source.Plan.Artifacts {
		for _, service := range artifact.Services {
			if service.ServiceId == serviceID && service.ComposeName == guard.DatabaseService.CurrentName {
				if guard.DatabaseArtifact != nil {
					return unresolvedBackupStage()
				}
				guard.DatabaseArtifact = proto.CloneOf(artifact)
			}
		}
	}
	if guard.DatabaseArtifact == nil {
		return unresolvedBackupStage()
	}
	if disposition.GetRecoveryRequired() != nil {
		native := source.TerminalRestore
		if native == nil || native.PostgresProgress == nil {
			return unresolvedBackupStage()
		}
		progress := native.PostgresProgress
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
			guard.RecoveryApply = &agentpb.BackupPostgresRestoreApplyStartCheckpoint{
				PointId: native.Point.ID, ExecutionNonce: nonce, ContainerId: progress.ContainerID,
				ExecId: progress.ApplyExecID, RepositoryDigest: repository, ExpectedLabelsSha256: labels,
				SourceSizeBytes: source.Step.GetRestore().ExpectedEvidence.SourceSizeBytes,
				SourceSha256:    append([]byte(nil), source.Step.GetRestore().ExpectedEvidence.SourceSha256...),
			}
		}
	}
	disposition.PostgresGuard = guard
	return nil
}
