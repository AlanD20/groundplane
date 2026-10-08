package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupCheckpointReplayEntry is one immutable receipt and its actual commit
// revision. Readers supply every sequence from one through the sealed cursor.
type BackupCheckpointReplayEntry struct {
	Request *agentpb.BackupCheckpointRequest
	Fence   *agentpb.CheckpointFence
}

// BackupConfigTransferEvidence comes from the durable transfer owner at the
// same fixed assignment read revision as checkpoint history. Snapshot storage
// chains and the full transfer transcript are not metadata-credit evidence.
type BackupConfigTransferEvidence struct {
	MetadataAccepted *agentpb.BackupConfigCredit
	CommittedCredits []*agentpb.BackupConfigCredit
}

// BackupStepResumeReplay retains cumulative progress and a constant number of
// checkpoint payloads, not the receipt history. An acceptance error poisons the
// replay so a partially advanced state can never be returned as valid progress.
type BackupStepResumeReplay struct {
	authority            *agentpb.BackupTaskAuthority
	step                 *agentpb.BackupStepAuthority
	configEvidence       *BackupConfigTransferEvidence
	resume               *agentpb.BackupStepResume
	capture              *agentpb.BackupCaptureResume
	restore              *agentpb.BackupRestoreResume
	prune                *agentpb.BackupPruneResume
	previous             *agentpb.CheckpointFence
	prepared             *agentpb.BackupArtifactPrepared
	uploaded             *agentpb.BackupUploadCompleted
	verified             *agentpb.BackupUploadVerified
	transfer             *agentpb.BackupConfigTransferCompleted
	configProgress       *agentpb.BackupConfigProgress
	postgresObserved     *agentpb.BackupPostgresContainerObserved
	postgresDumpStart    *agentpb.BackupPostgresDumpStart
	postgresApplyStart   *agentpb.BackupPostgresRestoreApplyStartCheckpoint
	postgresVerified     *agentpb.BackupPostgresRestoreVerified
	mysqlObserved        *agentpb.BackupMySQLContainerObserved
	mysqlDumpStart       *agentpb.BackupMySQLDumpStart
	mysqlApplyStart      *agentpb.BackupMySQLRestoreApplyStartCheckpoint
	mysqlVerified        *agentpb.BackupMySQLRestoreVerified
	databaseStopped      uint32
	databaseRecovered    uint32
	databaseServicePhase agentpb.BackupServicePhase
	records              uint64
	sequence             uint64
	failure              error
}

// NewBackupStepResumeReplay creates a pure, ordered receipt accumulator. Its
// reader must visit the complete sealed cursor at one fixed assignment revision.
func NewBackupStepResumeReplay(authority *agentpb.BackupTaskAuthority, step *agentpb.BackupStepAuthority,
	configEvidence *BackupConfigTransferEvidence,
) (*BackupStepResumeReplay, error) {
	if err := validateBackupTaskAuthority(authority); err != nil {
		return nil, err
	}
	bound := false
	for _, candidate := range authority.Steps {
		if proto.Equal(candidate, step) {
			bound = true
			break
		}
	}
	if !bound {
		return nil, backupResumeHistoryInvalid()
	}
	authority = proto.Clone(authority).(*agentpb.BackupTaskAuthority)
	step = proto.Clone(step).(*agentpb.BackupStepAuthority)
	resume := &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId}
	capture := &agentpb.BackupCaptureResume{
		Phase:  agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING,
		Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1},
	}
	restore := &agentpb.BackupRestoreResume{
		Phase:  agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION,
		Cursor: &agentpb.BackupRestoreCursor{ObjectAttempt: 1},
	}
	prune := &agentpb.BackupPruneResume{}
	switch {
	case step.GetCapture() != nil:
		resume.Operation = &agentpb.BackupStepResume_Capture{Capture: capture}
	case step.GetRestore() != nil:
		resume.Operation = &agentpb.BackupStepResume_Restore{Restore: restore}
	case step.GetPrune() != nil && len(step.GetPrune().Objects) == 1:
		prune.NextObjectOrdinal = step.GetPrune().Objects[0].Ordinal
		resume.Operation = &agentpb.BackupStepResume_Prune{Prune: prune}
	default:
		return nil, backupResumeHistoryInvalid()
	}
	return &BackupStepResumeReplay{authority: authority, step: step, configEvidence: configEvidence,
		resume: resume, capture: capture, restore: restore, prune: prune}, nil
}

// Accept owns one receipt, checks the exact predecessor and folds its evidence.
func (replay *BackupStepResumeReplay) Accept(entry BackupCheckpointReplayEntry) (err error) {
	if replay == nil {
		return backupResumeHistoryInvalid()
	}
	if replay.failure != nil {
		return replay.failure
	}
	defer func() {
		if err != nil {
			replay.failure = err
		}
	}()
	if replay.sequence == ^uint64(0) {
		return backupResumeHistoryInvalid()
	}
	authority, step := replay.authority, replay.step
	capture, restore, prune := replay.capture, replay.restore, replay.prune
	previous := replay.previous
	prepared, uploaded, verified := replay.prepared, replay.uploaded, replay.verified
	transfer, configProgress, records := replay.transfer, replay.configProgress, replay.records
	configEvidence := replay.configEvidence
	request, err := ValidateBackupCheckpointRequest(entry.Request, replay.sequence+1)
	if err != nil {
		return err
	}
	if request.TaskId != authority.TaskId || request.AssignmentId != authority.AssignmentId ||
		request.StepId != step.StepId ||
		request.ExecutionId != step.ExecutionId ||
		!bytes.Equal(request.AuthorityDigest, step.StepDigest) ||
		!proto.Equal(request.PrecedingCheckpoint, previous) ||
		!backupCheckpointFence(entry.Fence, step.StepDigest) ||
		(previous != nil && entry.Fence.DedupeKeyModRevision <= previous.DedupeKeyModRevision) ||
		RejectUnknown(entry.Fence) != nil {
		return backupResumeHistoryInvalid()
	}
	fence := proto.Clone(entry.Fence).(*agentpb.CheckpointFence)
	switch {
	case step.GetPrune() != nil:
		deleted := request.GetPruneObjectDeleted()
		object := step.GetPrune().Objects[0]
		if replay.sequence != 0 || deleted == nil || deleted.Ordinal != object.Ordinal ||
			deleted.PointId != object.PointId ||
			!proto.Equal(deleted.Object, object.Object) {
			return backupResumeHistoryInvalid()
		}
		prune.Checkpoint = &agentpb.BackupPruneResume_ObjectDeleted{ObjectDeleted: deleted}
		prune.CheckpointSequence, prune.PrecedingCheckpoint, prune.NextObjectOrdinal = request.CheckpointSequence, fence, deleted.Ordinal+1
	case request.GetConfig() != nil:
		content := step.GetCapture().GetConfig().GetContent()
		isRestore := step.GetRestore() != nil
		if isRestore {
			content = step.GetRestore().GetConfig().GetExpectedArchive().GetContent()
		}
		if content == nil || (isRestore && restore.GetArtifactValidated() == nil && configProgress == nil) ||
			prepared != nil {
			return backupResumeHistoryInvalid()
		}
		configProgress, records, transfer, err = replayBackupConfigProgress(
			authority,
			step,
			request.GetConfig(),
			configEvidence,
			configProgress,
			records,
			transfer,
			content,
			isRestore,
		)
		if err != nil {
			return err
		}
		if isRestore {
			restore.Checkpoint = &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: configProgress}
			restore.Cursor.ConfigRecordSequence, restore.Cursor.ConfigValueOrdinal = records, configProgress.NextValueOrdinal
			restore.Cursor.CumulativeChainSha256 = append([]byte(nil), configProgress.ValueChainSha256...)
			restore.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
			if transfer != nil {
				restore.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_PUBLICATION
			}
			if configProgress.GetMaterialization() != nil {
				restore.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_CLEANUP
			}
		} else {
			capture.Checkpoint = &agentpb.BackupCaptureResume_ConfigProgress{ConfigProgress: configProgress}
			capture.Cursor.ConfigRecordSequence, capture.Cursor.ConfigValueOrdinal = records, configProgress.NextValueOrdinal
			capture.Cursor.CumulativeChainSha256 = append([]byte(nil), configProgress.ValueChainSha256...)
		}
	case step.GetCapture() != nil:
		switch {
		case request.GetPostgresContainerObserved() != nil || request.GetPostgresDumpStart() != nil:
			if err := replay.postgresCaptureCheckpoint(request); err != nil {
				return err
			}
		case request.GetMysqlContainerObserved() != nil || request.GetMysqlDumpStart() != nil:
			if err := replay.mysqlCaptureCheckpoint(request); err != nil {
				return err
			}
		case request.GetArtifactPrepared() != nil:
			value := request.GetArtifactPrepared()
			if prepared != nil || value.PointId != step.GetCapture().PointId ||
				!backupResumePreparedSourceMatches(value, step.GetCapture()) ||
				(configProgress != nil && transfer == nil) ||
				(step.GetCapture().GetPostgres() != nil && (replay.postgresDumpStart == nil ||
					value.Evidence.SourceSizeBytes > replay.postgresDumpStart.MaxPlaintextBytes)) ||
				(step.GetCapture().GetMysql() != nil && (replay.mysqlDumpStart == nil ||
					value.Evidence.SourceSizeBytes > replay.mysqlDumpStart.MaxPlaintextBytes)) {
				return backupResumeHistoryInvalid()
			}
			prepared = value
			capture.PreparedArtifact = proto.CloneOf(value)
			capture.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED
			capture.Checkpoint = &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: value}
		case request.GetUploadCompleted() != nil:
			value := request.GetUploadCompleted()
			if prepared == nil || uploaded != nil || value.PointId != prepared.PointId ||
				!proto.Equal(value.Evidence, prepared.Evidence) ||
				!proto.Equal(value.Target, step.GetCapture().Target) {
				return backupResumeHistoryInvalid()
			}
			uploaded = value
			capture.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_HEAD_VERIFICATION
			capture.Checkpoint = &agentpb.BackupCaptureResume_UploadCompleted{UploadCompleted: value}
		case request.GetUploadVerified() != nil:
			value := request.GetUploadVerified()
			if uploaded == nil || verified != nil || value.PointId != uploaded.PointId ||
				!proto.Equal(value.Evidence, uploaded.Evidence) ||
				!backupResumeObjectMatchesTarget(value.Object, uploaded.Target) ||
				value.MetadataCount != uploaded.MetadataCount ||
				!bytes.Equal(value.MetadataSha256, uploaded.MetadataSha256) ||
				(uploaded.GetReturnedObject() != nil && !proto.Equal(uploaded.GetReturnedObject(), value.Object)) {
				return backupResumeHistoryInvalid()
			}
			verified = value
			capture.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT
			capture.Checkpoint = &agentpb.BackupCaptureResume_UploadVerified{UploadVerified: value}
		case request.GetSourceCleanupCompleted() != nil:
			value := request.GetSourceCleanupCompleted()
			if verified == nil || capture.GetSourceCleanupCompleted() != nil || value.PointId != verified.PointId ||
				!proto.Equal(value.Evidence, verified.Evidence) {
				return backupResumeHistoryInvalid()
			}
			capture.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY
			capture.Checkpoint = &agentpb.BackupCaptureResume_SourceCleanupCompleted{SourceCleanupCompleted: value}
		default:
			return errs.New(errs.KindNotImplemented, "backup source-specific resume projection is not implemented")
		}
	case request.GetRestoreArtifactValidated() != nil:
		value := request.GetRestoreArtifactValidated()
		if replay.sequence != 0 || !backupResumeRestoreArtifactMatches(value, step.GetRestore()) {
			return backupResumeHistoryInvalid()
		}
		restore.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
		restore.Checkpoint = &agentpb.BackupRestoreResume_ArtifactValidated{ArtifactValidated: value}
	case step.GetRestore().GetVolume() != nil && request.GetVolume() != nil:
		if err := replayBackupVolumeRestore(replay, request.GetVolume()); err != nil {
			return err
		}
	case step.GetRestore().GetPostgres() != nil:
		if err := replay.postgresRestoreCheckpoint(request); err != nil {
			return err
		}
	case step.GetRestore().GetMysql() != nil:
		if err := replay.mysqlRestoreCheckpoint(request); err != nil {
			return err
		}
	case step.GetRestore().GetConfig() != nil && request.GetSourceCleanupCompleted() != nil:
		value := request.GetSourceCleanupCompleted()
		if configProgress.GetMaterialization() == nil || configProgress.SourceCleanupCompleted != nil ||
			value.PointId != step.GetRestore().PointId || !proto.Equal(value.Evidence, step.GetRestore().ExpectedEvidence) {
			return backupResumeHistoryInvalid()
		}
		configProgress = proto.CloneOf(configProgress)
		configProgress.SourceCleanupCompleted = proto.CloneOf(value)
		restore.Checkpoint = &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: configProgress}
		restore.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
	default:
		return errs.New(errs.KindNotImplemented, "backup restore source-specific resume projection is not implemented")
	}
	if step.GetCapture() != nil {
		capture.CheckpointSequence, capture.PrecedingCheckpoint = request.CheckpointSequence, fence
	}
	if step.GetRestore() != nil {
		restore.CheckpointSequence, restore.PrecedingCheckpoint = request.CheckpointSequence, fence
	}
	replay.previous, replay.sequence = fence, request.CheckpointSequence
	replay.prepared, replay.uploaded, replay.verified = prepared, uploaded, verified
	replay.transfer, replay.configProgress, replay.records = transfer, configProgress, records
	return nil
}

// Resume returns an owned projection only after every visited receipt passed.
func (replay *BackupStepResumeReplay) Resume() (*agentpb.BackupStepResume, error) {
	if replay == nil {
		return nil, backupResumeHistoryInvalid()
	}
	if replay.failure != nil {
		return nil, replay.failure
	}
	step, capture, restore, prune := replay.step, replay.capture, replay.restore, replay.prune
	if (step.GetCapture() != nil && !validBackupCaptureResume(capture, step)) ||
		(step.GetRestore() != nil && !validBackupRestoreResume(restore, step)) ||
		(step.GetPrune() != nil && !validBackupPruneResume(prune, step)) {
		return nil, backupResumeHistoryInvalid()
	}
	return proto.Clone(replay.resume).(*agentpb.BackupStepResume), nil
}

func backupResumeHistoryInvalid() error {
	return errs.New(errs.KindValidationFailed, "backup resume history or cumulative transition is invalid")
}
