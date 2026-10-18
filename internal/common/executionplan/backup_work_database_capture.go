package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupDatabaseCapture returns the next cursor content without a commit
// fence. The receipt and cursor must commit together; only its real receipt
// revision may populate the fence for a subsequent assignment.
func AdvanceBackupDatabaseCapture(assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
) (*agentpb.BackupStepResume, error) {
	owned, _, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupWorkCheckpoint(request, owned.Input, authenticated)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) {
		return nil, invalidBackupCheckpoint("database capture report belongs to another attempt")
	}
	step := owned.Input.Steps[report.SourceOrdinal]
	current := owned.Sources[report.SourceOrdinal].GetCapture()
	if current == nil || (step.GetCapture().GetPostgres() == nil && step.GetCapture().GetMysql() == nil) ||
		current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("database capture report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	switch {
	case report.GetPostgresContainerObserved() != nil:
		value := report.GetPostgresContainerObserved()
		if current.CheckpointSequence != 0 || !postgresObservationMatchesService(value, step, owned.Input.Services) {
			return nil, backupResumeHistoryInvalid()
		}
		next.Checkpoint = &agentpb.BackupCaptureResume_PostgresContainerObserved{PostgresContainerObserved: value}
	case report.GetMysqlContainerObserved() != nil:
		value := report.GetMysqlContainerObserved()
		if current.CheckpointSequence != 0 || !mysqlObservationMatchesService(value, step, owned.Input.Services) {
			return nil, backupResumeHistoryInvalid()
		}
		next.Checkpoint = &agentpb.BackupCaptureResume_MysqlContainerObserved{MysqlContainerObserved: value}
	case report.GetPostgresDumpStart() != nil:
		value, observed := report.GetPostgresDumpStart(), current.GetPostgresContainerObserved()
		if current.CheckpointSequence != 1 || observed == nil || !postgresDumpMatches(value, step) ||
			value.ContainerId != observed.ContainerId || !bytes.Equal(value.RepositoryDigest, observed.RepositoryDigest) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) {
			return nil, backupResumeHistoryInvalid()
		}
		next.DumpStart = proto.CloneOf(value)
		next.Checkpoint = &agentpb.BackupCaptureResume_PostgresDumpStart{PostgresDumpStart: value}
	case report.GetMysqlDumpStart() != nil:
		value, observed := report.GetMysqlDumpStart(), current.GetMysqlContainerObserved()
		if current.CheckpointSequence != 1 || observed == nil || !mysqlDumpMatches(value, step) ||
			value.ContainerId != observed.ContainerId || !bytes.Equal(value.ImageReferenceSha256, observed.ImageReferenceSha256) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) {
			return nil, backupResumeHistoryInvalid()
		}
		next.MysqlDumpStart = proto.CloneOf(value)
		next.Checkpoint = &agentpb.BackupCaptureResume_MysqlDumpStartCheckpoint{MysqlDumpStartCheckpoint: value}
	case report.GetArtifactPrepared() != nil:
		value := report.GetArtifactPrepared()
		if current.CheckpointSequence != 2 || current.PreparedArtifact != nil ||
			value.PointId != step.GetCapture().PointId || !backupResumePreparedSourceMatches(value, step.GetCapture()) {
			return nil, backupResumeHistoryInvalid()
		}
		if step.GetCapture().GetPostgres() != nil {
			if current.DumpStart == nil || value.Evidence.SourceSizeBytes > current.DumpStart.MaxPlaintextBytes ||
				!proto.Equal(value.GetPostgres(), current.DumpStart.Archive) {
				return nil, backupResumeHistoryInvalid()
			}
		} else if current.MysqlDumpStart == nil || value.Evidence.SourceSizeBytes > current.MysqlDumpStart.MaxPlaintextBytes ||
			!proto.Equal(value.GetMysql(), current.MysqlDumpStart.Archive) {
			return nil, backupResumeHistoryInvalid()
		}
		next.PreparedArtifact = proto.CloneOf(value)
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED
		next.Checkpoint = &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: value}
	default:
		if err := advanceBackupCapturedObject(next, current, step, report); err != nil {
			return nil, err
		}
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Capture{Capture: next}}, nil
}

func advanceBackupCapturedObject(next, current *agentpb.BackupCaptureResume, step *agentpb.BackupStepAuthority,
	report *agentpb.BackupWorkCheckpointRequest,
) error {
	switch {
	case report.GetUploadCompleted() != nil:
		value, prepared := report.GetUploadCompleted(), current.PreparedArtifact
		if current.GetArtifactPrepared() == nil || prepared == nil || value.PointId != prepared.PointId ||
			!proto.Equal(value.Evidence, prepared.Evidence) || !proto.Equal(value.Target, step.GetCapture().Target) {
			return backupResumeHistoryInvalid()
		}
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_HEAD_VERIFICATION
		next.Checkpoint = &agentpb.BackupCaptureResume_UploadCompleted{UploadCompleted: value}
	case report.GetUploadVerified() != nil:
		value, uploaded := report.GetUploadVerified(), current.GetUploadCompleted()
		if uploaded == nil || value.PointId != uploaded.PointId || !proto.Equal(value.Evidence, uploaded.Evidence) ||
			!backupResumeObjectMatchesTarget(value.Object, uploaded.Target) || value.MetadataCount != uploaded.MetadataCount ||
			!bytes.Equal(value.MetadataSha256, uploaded.MetadataSha256) ||
			(uploaded.GetReturnedObject() != nil && !proto.Equal(uploaded.GetReturnedObject(), value.Object)) {
			return backupResumeHistoryInvalid()
		}
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT
		next.Checkpoint = &agentpb.BackupCaptureResume_UploadVerified{UploadVerified: value}
	case report.GetSourceCleanupCompleted() != nil:
		value, verified := report.GetSourceCleanupCompleted(), current.GetUploadVerified()
		if verified == nil || value.PointId != verified.PointId || !proto.Equal(value.Evidence, verified.Evidence) {
			return backupResumeHistoryInvalid()
		}
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY
		next.Checkpoint = &agentpb.BackupCaptureResume_SourceCleanupCompleted{SourceCleanupCompleted: value}
	default:
		return invalidBackupCheckpoint("report is not a permitted capture transition")
	}
	return nil
}
