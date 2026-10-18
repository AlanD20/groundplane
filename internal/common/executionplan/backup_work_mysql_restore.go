package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func advanceMySQLRestore(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority,
	services []*agentpb.BackupServiceFact, report *agentpb.BackupWorkCheckpointRequest,
) error {
	progress, restore := state.DatabaseProgress, step.GetRestore()
	invalid := func() error {
		return invalidBackupCheckpoint("MySQL restore report conflicts with captured database or saved consumer progress")
	}
	switch {
	case report.GetMysqlContainerObserved() != nil:
		value := report.GetMysqlContainerObserved()
		if state.GetArtifactValidated() == nil || progress.MysqlObserved != nil ||
			!mysqlObservationMatchesService(value, step, services) {
			return invalid()
		}
		progress.MysqlObserved = value
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlContainerObserved{MysqlContainerObserved: value}
	case report.GetMysqlServiceProgress() != nil:
		value := report.GetMysqlServiceProgress()
		if progress.MysqlObserved == nil || (state.MysqlApplyStart != nil && progress.MysqlVerified == nil) {
			return invalid()
		}
		if err := advanceDatabaseConsumer(progress, step, services, value.ServiceCursor, value.ServiceId,
			value.Phase, value.ObservationSha256, progress.MysqlVerified != nil || progress.ConsumersOnly); err != nil {
			return err
		}
		state.Cursor.ServiceCursor = value.ServiceCursor
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlServiceProgress{MysqlServiceProgress: value}
	case report.GetMysqlRestoreApplyStart() != nil:
		value, observed := report.GetMysqlRestoreApplyStart(), progress.MysqlObserved
		if !validBackupMySQLApply(value) || observed == nil || state.MysqlApplyStart != nil ||
			int(progress.StoppedConsumers) != len(step.ConsumerServiceIds) || value.PointId != restore.PointId ||
			value.ContainerId != observed.ContainerId || !bytes.Equal(value.ImageReferenceSha256, observed.ImageReferenceSha256) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) ||
			value.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return invalid()
		}
		state.MysqlApplyStart = proto.CloneOf(value)
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlRestoreApplyStart{MysqlRestoreApplyStart: value}
	case report.GetMysqlRestoreVerified() != nil:
		value := report.GetMysqlRestoreVerified()
		if state.MysqlApplyStart == nil || progress.MysqlVerified != nil || value.PointId != restore.PointId ||
			value.ContainerId != state.MysqlApplyStart.ContainerId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) ||
			!backupCheckpointDigest(value.VerificationSha256) {
			return invalid()
		}
		progress.MysqlVerified = value
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlRestoreVerified{MysqlRestoreVerified: value}
	default:
		return invalid()
	}
	return nil
}
