package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func advancePostgresRestore(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority,
	services []*agentpb.BackupServiceFact, report *agentpb.BackupWorkCheckpointRequest,
) error {
	progress, restore := state.DatabaseProgress, step.GetRestore()
	invalid := func() error {
		return invalidBackupCheckpoint(
			"PostgreSQL restore report conflicts with captured database or saved consumer progress",
		)
	}
	switch {
	case report.GetPostgresContainerObserved() != nil:
		value := report.GetPostgresContainerObserved()
		if state.GetArtifactValidated() == nil || progress.PostgresObserved != nil ||
			!postgresObservationMatchesService(value, step, services) {
			return invalid()
		}
		progress.PostgresObserved = value
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresContainerObserved{PostgresContainerObserved: value}
	case report.GetPostgresServiceProgress() != nil:
		value := report.GetPostgresServiceProgress()
		if progress.PostgresObserved == nil || (state.ApplyStart != nil && progress.PostgresVerified == nil) {
			return invalid()
		}
		if err := advanceDatabaseConsumer(progress, step, services, value.ServiceCursor, value.ServiceId,
			value.Phase, value.ObservationSha256, progress.PostgresVerified != nil || progress.ConsumersOnly); err != nil {
			return err
		}
		state.Cursor.ServiceCursor = value.ServiceCursor
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresServiceProgress{PostgresServiceProgress: value}
	case report.GetPostgresRestoreApplyStart() != nil:
		value, observed := report.GetPostgresRestoreApplyStart(), progress.PostgresObserved
		if !validBackupPostgresApply(value) || observed == nil || state.ApplyStart != nil ||
			int(progress.StoppedConsumers) != len(step.ConsumerServiceIds) || value.PointId != restore.PointId ||
			value.ContainerId != observed.ContainerId || !bytes.Equal(value.RepositoryDigest, observed.RepositoryDigest) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) ||
			value.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return invalid()
		}
		state.ApplyStart = proto.CloneOf(value)
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresRestoreApplyStart{PostgresRestoreApplyStart: value}
	case report.GetPostgresRestoreVerified() != nil:
		value := report.GetPostgresRestoreVerified()
		if state.ApplyStart == nil || progress.PostgresVerified != nil || value.PointId != restore.PointId ||
			value.ContainerId != state.ApplyStart.ContainerId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) ||
			!backupCheckpointDigest(value.VerificationSha256) {
			return invalid()
		}
		progress.PostgresVerified = value
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresRestoreVerified{PostgresRestoreVerified: value}
	default:
		return invalid()
	}
	return nil
}
