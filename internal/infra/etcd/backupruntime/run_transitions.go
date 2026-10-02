package backupruntime

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func BackupRunTransitionRequiresCheckpoint(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := ChangedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return (from.Kind == BackupRuntimeSourceAttach && from.State == BackupSourceAttemptPending &&
		to.State == BackupSourceAttemptCapturing) ||
		((from.State == BackupSourceAttemptPending || from.State == BackupSourceAttemptCapturing) &&
			(to.State == BackupSourceAttemptReady || from.Kind == BackupRuntimeSourceVolume && to.State == BackupSourceAttemptStaged)) ||
		(from.State == BackupSourceAttemptReady && to.State == BackupSourceAttemptStaged) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State &&
			from.Phase == BackupSourcePhaseUpload && to.Phase == BackupSourcePhaseHeadVerification) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State && from.Phase == BackupSourcePhaseHeadVerification &&
			to.Phase == BackupSourcePhasePointCommit) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			from.Phase == BackupSourcePhasePointCommit && to.State == BackupSourceAttemptPointCommitted &&
			to.Phase == BackupSourcePhaseRetention) ||
		(from.State == BackupSourceAttemptCleanupPending && to.State == BackupSourceAttemptSucceeded)
}

// BackupRunConfigCaptureCheckpointMatchesTransition admits the capture-ready
// edge only for a typed Config completion. The etcd coordinator separately
// binds that completion to the immutable native credit cursor in the same
// transaction; the ordinary checkpoint writer therefore cannot use this edge.
func BackupRunConfigCaptureCheckpointMatchesTransition(
	request *agentpb.BackupCheckpointRequest,
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
) bool {
	if request == nil {
		return false
	}
	validated, err := executionplan.ValidateBackupCheckpointRequest(request, request.GetCheckpointSequence())
	if err != nil {
		return false
	}
	completed := validated.GetConfig().GetTransferCompleted()
	archive, archiveErr := ConfigArchiveEvidenceFromCapture(completed)
	return archiveErr == nil && current.ConfigArchive == (BackupConfigArchiveEvidence{}) &&
		next.ConfigArchive == archive && current.Kind == BackupRuntimeSourceConfig && next.Kind == current.Kind &&
		(current.State == BackupSourceAttemptPending || current.State == BackupSourceAttemptCapturing) &&
		current.Phase == BackupSourcePhaseCapture && next.State == BackupSourceAttemptReady &&
		next.Phase == BackupSourcePhaseStaging && current.Evidence == next.Evidence &&
		current.Upload == next.Upload && current.Object == next.Object && current.FailureCode == next.FailureCode
}

// BackupRunVolumeCaptureCheckpointMatchesTransition admits the atomic
// manifest-complete and artifact-prepared edge. The writer also compares the
// immutable manifest cursor in the same transaction.
func BackupRunVolumeCaptureCheckpointMatchesTransition(
	request *agentpb.BackupCheckpointRequest,
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
) bool {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return false
	}
	prepared := request.GetArtifactPrepared()
	if prepared == nil || prepared.GetVolume() == nil || current.Kind != BackupRuntimeSourceVolume ||
		current.VolumeArchive != (BackupVolumeArchiveEvidence{}) || next.Kind != current.Kind ||
		(current.State != BackupSourceAttemptPending && current.State != BackupSourceAttemptCapturing) ||
		current.Phase != BackupSourcePhaseCapture || next.State != BackupSourceAttemptStaged ||
		next.Phase != BackupSourcePhaseUpload || prepared.PointId != next.RecoveryPointID ||
		!BackupArtifactEvidenceMatchesWire(next.Evidence, prepared.Evidence) ||
		next.Upload.Kind != BackupUploadPrepared || current.Evidence != (BackupArtifactEvidence{}) ||
		current.Upload != (BackupUploadOutcome{}) || current.Object != next.Object ||
		current.FailureCode != next.FailureCode || current.ConfigArchive != next.ConfigArchive {
		return false
	}
	archive, err := VolumeArchiveEvidenceFromWire(prepared.GetVolume())
	selected := next.VolumeArchive
	selected.Manifest = BackupVolumeManifestReference{}
	return err == nil && selected == archive && next.VolumeArchive.Manifest.Valid() &&
		archive.SourceSizeBytes == next.Evidence.SourceSizeBytes
}

func BackupRunCheckpointMatchesTransition(
	request *agentpb.BackupCheckpointRequest,
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
) bool {
	if request == nil {
		return false
	}
	validated, err := executionplan.ValidateBackupCheckpointRequest(request, request.GetCheckpointSequence())
	if err != nil {
		return false
	}
	request = validated
	if current.ConfigArchive != next.ConfigArchive {
		return false
	}
	if current.VolumeArchive != next.VolumeArchive {
		return false
	}
	if current.Kind == BackupRuntimeSourceAttach && next.Kind == current.Kind &&
		current.Evidence == next.Evidence && current.Upload == next.Upload && current.Object == next.Object &&
		current.FailureCode == next.FailureCode && current.Phase == BackupSourcePhaseCapture {
		if current.State == BackupSourceAttemptPending && next.State == BackupSourceAttemptCapturing &&
			next.Phase == BackupSourcePhaseCapture {
			observed := request.GetPostgresContainerObserved()
			return observed != nil && current.Snapshot.Postgres != nil &&
				observed.ServiceId == current.Snapshot.Postgres.BackingServiceID
		}
		if current.State == BackupSourceAttemptCapturing && next.State == BackupSourceAttemptReady &&
			next.Phase == BackupSourcePhaseStaging {
			started := request.GetPostgresDumpStart()
			return started != nil && started.PointId == next.RecoveryPointID && current.Snapshot.Postgres != nil &&
				started.DatabaseName == current.Snapshot.Postgres.Database && started.RoleName == current.Snapshot.Postgres.Role
		}
	}
	if current.State == BackupSourceAttemptReady && next.State == BackupSourceAttemptStaged {
		prepared := request.GetArtifactPrepared()
		return prepared != nil && prepared.PointId == next.RecoveryPointID &&
			BackupArtifactEvidenceMatchesWire(
				next.Evidence,
				prepared.Evidence,
			) && next.Upload.Kind == BackupUploadPrepared
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State && current.Phase == BackupSourcePhaseUpload && next.Phase == BackupSourcePhaseHeadVerification {
		completed := request.GetUploadCompleted()
		if completed == nil || completed.PointId != next.RecoveryPointID ||
			!BackupArtifactEvidenceMatchesWire(
				next.Evidence,
				completed.Evidence,
			) || !backupTargetMatchesWire(next.Upload.Target, completed.Target) {
			return false
		}
		switch outcome := completed.Outcome.(type) {
		case *agentpb.BackupUploadCompleted_ReturnedObject:
			return outcome != nil && next.Upload.Kind == BackupUploadReturned &&
				backupObjectMatchesWire(next.Upload.ReturnedObject, outcome.ReturnedObject)
		case *agentpb.BackupUploadCompleted_Unknown:
			return outcome != nil && outcome.Unknown != nil && next.Upload.Kind == BackupUploadUnknown &&
				next.Upload.ReturnedObject == (BackupObjectIdentity{})
		default:
			return false
		}
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State && current.Phase == BackupSourcePhaseHeadVerification && next.Phase == BackupSourcePhasePointCommit {
		verified := request.GetUploadVerified()
		return verified != nil && verified.PointId == next.RecoveryPointID &&
			BackupArtifactEvidenceMatchesWire(
				next.Evidence,
				verified.Evidence,
			) && backupObjectMatchesWire(next.Object, verified.Object)
	}
	if current.State == BackupSourceAttemptCleanupPending && next.State == BackupSourceAttemptSucceeded {
		cleanup := request.GetSourceCleanupCompleted()
		return cleanup != nil && cleanup.PointId == next.RecoveryPointID &&
			BackupArtifactEvidenceMatchesWire(next.Evidence, cleanup.Evidence)
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		current.Phase == BackupSourcePhasePointCommit && next.State == BackupSourceAttemptPointCommitted &&
		next.Phase == BackupSourcePhaseRetention {
		cleanup := request.GetSourceCleanupCompleted()
		return cleanup != nil && cleanup.PointId == next.RecoveryPointID &&
			BackupArtifactEvidenceMatchesWire(next.Evidence, cleanup.Evidence) && current.Evidence == next.Evidence &&
			current.Object == next.Object && current.Upload == next.Upload
	}
	return false
}

type backupRunTransitionMode uint8

const (
	BackupRunTransitionOrdinary backupRunTransitionMode = iota + 1
	BackupRunTransitionOrphanCreate
	BackupRunTransitionOrphanDelete
	BackupRunTransitionOrphanTerminal
	BackupRunTransitionPointCommit
	BackupRunTransitionRetentionComplete
	BackupRunTransitionCaptureComplete
)

func ValidateBackupRunTransition(
	current BackupRunRecord,
	next BackupRunRecord,
	mode backupRunTransitionMode,
) error {
	if ValidateBackupRunRecord(current) != nil || ValidateBackupRunRecord(next) != nil ||
		!next.UpdatedAt.After(current.UpdatedAt) || !backupRunImmutableEqual(current, next) ||
		!validBackupRunStateTransition(current.State, next.State) {
		return errs.New(errs.KindValidationFailed, "backup run transition is invalid")
	}
	changed, hasChanged := ChangedBackupSourceOrdinal(current, next)
	if !hasChanged {
		for index := range current.Sources {
			if !backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
				return errs.New(
					errs.KindValidationFailed,
					"backup run transition changes multiple sources",
				)
			}
		}
		if current.State == next.State {
			return errs.New(errs.KindValidationFailed, "backup run transition is a no-op")
		}
		return nil
	}
	from := current.Sources[changed]
	to := next.Sources[changed]
	if !validBackupSourceTransition(from, to, mode) {
		return errs.New(errs.KindValidationFailed, "backup source transition is invalid")
	}
	return nil
}

func validBackupRunStateTransition(current BackupRunState, next BackupRunState) bool {
	if current == next {
		return current == BackupRunQueued || current == BackupRunRunning
	}
	switch current {
	case BackupRunQueued:
		return next == BackupRunRunning || next == BackupRunFailed || next == BackupRunAborted ||
			next == BackupRunTimedOut
	case BackupRunRunning:
		return next == BackupRunFailed || next == BackupRunCompleted || next == BackupRunAborted ||
			next == BackupRunTimedOut
	default:
		return false
	}
}

func validBackupSourceTransition(
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
	mode backupRunTransitionMode,
) bool {
	if current.ConfigArchive != next.ConfigArchive &&
		!(current.Kind == BackupRuntimeSourceConfig && current.ConfigArchive == (BackupConfigArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture &&
			(current.State == BackupSourceAttemptPending || current.State == BackupSourceAttemptCapturing) &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging &&
			validateSelectedConfigArchive(next.Kind, next.ConfigArchive, next.Evidence) == nil) {
		return false
	}
	if current.VolumeArchive != next.VolumeArchive &&
		!(current.Kind == BackupRuntimeSourceVolume && current.VolumeArchive == (BackupVolumeArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture &&
			(current.State == BackupSourceAttemptPending || current.State == BackupSourceAttemptCapturing) &&
			next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload &&
			validateSelectedVolumeArchive(next.Kind, next.VolumeArchive, next.Evidence) == nil) {
		return false
	}
	if current.Evidence != (BackupArtifactEvidence{}) && current.Evidence != next.Evidence {
		return false
	}
	if current.Upload != next.Upload && current.Upload != (BackupUploadOutcome{}) &&
		!(current.Upload.Kind == BackupUploadPrepared && current.Phase == BackupSourcePhaseUpload &&
			next.Phase == BackupSourcePhaseHeadVerification && current.Upload.Target == next.Upload.Target &&
			(next.Upload.Kind == BackupUploadReturned || next.Upload.Kind == BackupUploadUnknown)) {
		return false
	}
	if current.Object != next.Object &&
		!(current.Object == (BackupObjectIdentity{}) && current.Phase == BackupSourcePhaseHeadVerification &&
			next.Phase == BackupSourcePhasePointCommit) {
		return false
	}
	if mode == BackupRunTransitionOrphanCreate {
		return current.State == BackupSourceAttemptStaged &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	if mode == BackupRunTransitionOrphanDelete {
		return current.State == BackupSourceAttemptOrphaned &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptFailed && next.Phase == current.Phase
	}
	if mode == BackupRunTransitionOrphanTerminal {
		return current.State == BackupSourceAttemptOrphaned &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase &&
			current.FailureCode == "" && next.FailureCode != ""
	}
	if mode == BackupRunTransitionPointCommit {
		return ((current.State == BackupSourceAttemptStaged && current.Phase == BackupSourcePhasePointCommit) ||
			(current.State == BackupSourceAttemptOrphaned && current.Phase == BackupSourcePhasePointCommit)) &&
			next.State == BackupSourceAttemptPointCommitted &&
			next.Phase == BackupSourcePhaseRetention
	}
	if mode == BackupRunTransitionRetentionComplete {
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention &&
			next.State == BackupSourceAttemptCleanupPending &&
			next.Phase == BackupSourcePhaseCleanup
	}
	if mode == BackupRunTransitionCaptureComplete {
		// Capture does not stop consumers or modify live source state.
		// Its source-cleanup receipt already proved staging removal;
		// only the native retention sweep remains before source completion.
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention && next.State == BackupSourceAttemptSucceeded &&
			next.Phase == BackupSourcePhaseCleanup && current.FailureCode == "" && next.FailureCode == ""
	}
	if next.State == BackupSourceAttemptFailed {
		return ActiveBackupSourceAttemptState(current.State) &&
			current.State != BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	switch current.State {
	case BackupSourceAttemptPending:
		return current.Phase == BackupSourcePhaseCapture &&
			((next.State == BackupSourceAttemptCapturing && next.Phase == current.Phase) ||
				(next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging) ||
				(current.Kind == BackupRuntimeSourceVolume && next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload))
	case BackupSourceAttemptCapturing:
		return current.Phase == BackupSourcePhaseCapture &&
			(next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging ||
				current.Kind == BackupRuntimeSourceVolume && next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload)
	case BackupSourceAttemptReady:
		return current.Phase == BackupSourcePhaseStaging &&
			next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload
	case BackupSourceAttemptStaged:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload && next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification && next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptOrphaned:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload &&
				next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification &&
					next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptPointCommitted:
		return next.State == current.State && next.Phase == current.Phase &&
			next.FailureCode == BackupFailureRetention
	case BackupSourceAttemptCleanupPending:
		return next.Phase == current.Phase && (next.State == BackupSourceAttemptSucceeded ||
			(next.State == current.State && next.FailureCode == BackupFailureCleanup))
	default:
		return false
	}
}

func backupRunImmutableEqual(current BackupRunRecord, next BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	normalized := next
	normalized.State = current.State
	normalized.UpdatedAt = current.UpdatedAt
	normalized.Sources = append([]BackupRunSourceAttemptRecord(nil), next.Sources...)
	for index := range normalized.Sources {
		normalized.Sources[index].State = current.Sources[index].State
		normalized.Sources[index].Phase = current.Sources[index].Phase
		normalized.Sources[index].Evidence = current.Sources[index].Evidence
		normalized.Sources[index].ConfigArchive = current.Sources[index].ConfigArchive
		normalized.Sources[index].VolumeArchive = current.Sources[index].VolumeArchive
		normalized.Sources[index].Upload = current.Sources[index].Upload
		normalized.Sources[index].Object = current.Sources[index].Object
		normalized.Sources[index].FailureCode = current.Sources[index].FailureCode
	}
	return BackupRunRecordsEqual(current, normalized)
}

func backupRunSourceMutableEqual(
	left BackupRunSourceAttemptRecord,
	right BackupRunSourceAttemptRecord,
) bool {
	return left.State == right.State && left.Phase == right.Phase &&
		left.ConfigArchive == right.ConfigArchive && left.VolumeArchive == right.VolumeArchive &&
		left.Evidence == right.Evidence && left.Upload == right.Upload && left.Object == right.Object &&
		left.FailureCode == right.FailureCode
}

func BackupRunRecordsEqual(left BackupRunRecord, right BackupRunRecord) bool {
	leftValue, leftErr := EncodeBackupRunRecord(left)
	rightValue, rightErr := EncodeBackupRunRecord(right)
	defer clear(leftValue)
	defer clear(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func TerminalBackupRunState(state BackupRunState) bool {
	return state == BackupRunFailed || state == BackupRunCompleted || state == BackupRunAborted ||
		state == BackupRunTimedOut
}
