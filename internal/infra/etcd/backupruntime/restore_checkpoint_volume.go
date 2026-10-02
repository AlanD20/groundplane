package backupruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupVolumeOldManifestProof struct {
	EntryCount            uint64
	ContentManifestSHA256 [sha256.Size]byte
	FullTreeSHA256        [sha256.Size]byte
}

func PrepareVolumeRestoreCheckpoint(current BackupRestoreRecord, request *agentpb.BackupCheckpointRequest,
	old *BackupVolumeOldManifestProof, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceVolume ||
		request == nil || request.TaskId != current.TaskID || !ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore checkpoint identity is invalid")
	}
	if _, err := executionplan.ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err != nil {
		return BackupRestoreRecord{}, err
	}
	next := CloneBackupRestoreRecord(current)
	next.UpdatedAt = at
	if validated := request.GetRestoreArtifactValidated(); validated != nil {
		archive, err := current.Point.VolumeArchive.Wire()
		if err != nil || current.State != BackupRestoreDownloading && current.State != BackupRestoreQueued ||
			validated.PointId != current.Point.ID || !backupObjectMatchesWire(current.Point.Object, validated.Object) ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, validated.Evidence) ||
			!proto.Equal(archive, validated.GetVolume()) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
				"Volume Restore artifact differs from selected Point",
			)
		}
		artifact := current.Point.Evidence
		next.Artifact, next.VolumeProgress, next.State = &artifact, &BackupRestoreVolumeProgress{}, BackupRestoreArtifactVerified
	} else {
		volume := request.GetVolume()
		if volume == nil || next.VolumeProgress == nil {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore checkpoint lacks artifact proof")
		}
		progress := next.VolumeProgress
		switch event := volume.Checkpoint.(type) {
		case *agentpb.BackupVolumeCheckpoint_ServiceProgress:
			value := event.ServiceProgress
			if value == nil || value.ServiceCursor > current.ServiceCount ||
				value.ServiceCursor < progress.ServiceCursor {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore Service cursor changed")
			}
			if !progress.OldRemoved && value.Phase >= agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore Service recovery precedes old-tree cleanup")
			}
			if value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT {
				if progress.ConsumersStopped || progress.OldRemoved ||
					(current.State != BackupRestoreArtifactVerified && current.State != BackupRestoreConsumersStopped) {
					return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore stop intent has no live authority")
				}
				progress.ServiceMutationStarted, next.MutationStarted = true, true
				next.State = BackupRestoreConsumersStopped
			}
			progress.ServiceCursor = value.ServiceCursor
			if progress.OldRemoved && value.ServiceCursor == current.ServiceCount &&
				(value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
					value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
					value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING) {
				progress.ServicesRecovered = true
				next.State, next.Verification = BackupRestoreVerified, BackupVerificationPassed
			}
		case *agentpb.BackupVolumeCheckpoint_ConsumersStopped:
			value := event.ConsumersStopped
			if current.State != BackupRestoreArtifactVerified && current.State != BackupRestoreConsumersStopped ||
				progress.ConsumersStopped || old == nil ||
				old.EntryCount < 1 || old.EntryCount > backupvolume.MaxEntries ||
				old.ContentManifestSHA256 == ([sha256.Size]byte{}) || old.FullTreeSHA256 == ([sha256.Size]byte{}) ||
				value == nil || value.StoppedCount > current.ServiceCount {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore stopped-tree proof is unavailable")
			}
			progress.OldEntryCount = old.EntryCount
			progress.OldContentManifestSHA256 = hex.EncodeToString(old.ContentManifestSHA256[:])
			progress.OldFullTreeSHA256 = hex.EncodeToString(old.FullTreeSHA256[:])
			progress.ConsumersStopped, progress.ServiceMutationStarted, next.MutationStarted = true, true, true
			progress.ServiceCursor = 0
			next.State = BackupRestoreConsumersStopped
		case *agentpb.BackupVolumeCheckpoint_ConstructionIntent:
			value := event.ConstructionIntent
			if !progress.ConsumersStopped || progress.PendingConstruction != 0 || progress.PendingFinalization != 0 ||
				value == nil || value.ConstructionCursorBefore != progress.ConstructionCursor ||
				value.ArchiveOrdinal != progress.ConstructionCursor+1 ||
				value.ArchiveOrdinal > current.Point.VolumeArchive.EntryCount ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.NewTreeContentManifestSha256) != current.Point.VolumeArchive.ContentManifestSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore construction intent is out of order")
			}
			progress.PendingConstruction = value.ArchiveOrdinal
			next.State = BackupRestoreRestoring
		case *agentpb.BackupVolumeCheckpoint_ConstructionCompleted:
			value := event.ConstructionCompleted
			if value == nil || value.Intent == nil || progress.PendingConstruction == 0 ||
				value.Intent.ArchiveOrdinal != progress.PendingConstruction ||
				value.ConstructionCursorAfter != progress.PendingConstruction {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore construction completion changed")
			}
			progress.ConstructionCursor, progress.PendingConstruction = progress.PendingConstruction, 0
		case *agentpb.BackupVolumeCheckpoint_FinalizationIntent:
			value := event.FinalizationIntent
			if progress.ConstructionCursor != current.Point.VolumeArchive.EntryCount ||
				progress.PendingFinalization != 0 || value == nil ||
				value.FinalizationCursorBefore != progress.FinalizationCursor ||
				value.FinalizationOrdinal != progress.FinalizationCursor+1 ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.NewTreeContentManifestSha256) != current.Point.VolumeArchive.ContentManifestSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore finalization intent is out of order")
			}
			progress.PendingFinalization = value.FinalizationOrdinal
		case *agentpb.BackupVolumeCheckpoint_FinalizationCompleted:
			value := event.FinalizationCompleted
			if value == nil || value.Intent == nil || progress.PendingFinalization == 0 ||
				value.Intent.FinalizationOrdinal != progress.PendingFinalization ||
				value.FinalizationCursorAfter != progress.PendingFinalization {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore finalization completion changed")
			}
			progress.FinalizationCursor, progress.PendingFinalization = progress.PendingFinalization, 0
			if progress.FinalizationCursor == current.Point.VolumeArchive.EntryCount {
				next.StagedTreeManifestSHA256 = current.Point.VolumeArchive.ContentManifestSHA256
				next.State = BackupRestoreTreeValidated
			}
		case *agentpb.BackupVolumeCheckpoint_ExchangeIntent:
			value := event.ExchangeIntent
			if current.State != BackupRestoreTreeValidated || progress.ExchangeIntent || value == nil ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.OldFullTreeSha256) != progress.OldFullTreeSHA256 ||
				hex.EncodeToString(value.NewFullTreeSha256) != current.Point.VolumeArchive.FullTreeSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore exchange intent differs from both trees")
			}
			progress.ExchangeIntent, next.MutationStarted, next.State = true, true, BackupRestoreExchangeReady
		case *agentpb.BackupVolumeCheckpoint_TreeExchanged:
			value := event.TreeExchanged
			if current.State != BackupRestoreExchangeReady || !progress.ExchangeIntent || value == nil ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.OldFullTreeSha256) != progress.OldFullTreeSHA256 ||
				hex.EncodeToString(value.NewFullTreeSha256) != current.Point.VolumeArchive.FullTreeSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore exchange completion differs from intent")
			}
			progress.Exchanged, next.State = true, BackupRestoreExchanged
		case *agentpb.BackupVolumeCheckpoint_ReplacedPathDeleteIntent:
			value := event.ReplacedPathDeleteIntent
			if !progress.Exchanged || progress.PendingDeletion != 0 || value == nil ||
				value.DeletionCursorBefore != progress.DeletionCursor ||
				value.DeletionOrdinal != progress.DeletionCursor+1 ||
				value.DeletionOrdinal > progress.OldEntryCount ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.OldFullTreeSha256) != progress.OldFullTreeSHA256 ||
				hex.EncodeToString(value.NewFullTreeSha256) != current.Point.VolumeArchive.FullTreeSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore deletion intent is out of order")
			}
			progress.PendingDeletion = value.DeletionOrdinal
		case *agentpb.BackupVolumeCheckpoint_VolumeReplacedPathCleaned:
			value := event.VolumeReplacedPathCleaned
			if value == nil || progress.PendingDeletion == 0 || value.DeletionOrdinal != progress.PendingDeletion ||
				value.DeletionCursorBefore+1 != value.DeletionOrdinal ||
				!volumeRestoreMutationIdentity(current, value.PointId, value.RestoreGenerationId) ||
				hex.EncodeToString(value.OldFullTreeSha256) != progress.OldFullTreeSHA256 ||
				hex.EncodeToString(value.NewFullTreeSha256) != current.Point.VolumeArchive.FullTreeSHA256 {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore deletion completion changed")
			}
			progress.DeletionCursor, progress.PendingDeletion = progress.PendingDeletion, 0
			if progress.DeletionCursor == progress.OldEntryCount {
				if !bytes.Equal(value.LogicalPath, []byte(".")) {
					return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore root deletion proof is missing")
				}
				progress.OldRemoved = true
				if current.ServiceCount == 0 {
					progress.ServicesRecovered = true
					next.State, next.Verification = BackupRestoreVerified, BackupVerificationPassed
				} else {
					next.State = BackupRestoreConsumersRestored
				}
			}
		default:
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore checkpoint phase is unsupported")
		}
	}
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func volumeRestoreMutationIdentity(current BackupRestoreRecord, pointID, generationID string) bool {
	return pointID == current.Point.ID && generationID == current.RestoreGenerationID
}
