package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

var volumeCheckpointMarshal = proto.MarshalOptions{Deterministic: true}

// A Volume restore projects only accepted typed receipts. An intent remains
// visible until its exact completion; a restart must resolve that intent
// instead of deriving a fresh mutation from the current filesystem.
func replayBackupVolumeRestore(replay *BackupStepResumeReplay,
	checkpoint *agentpb.BackupVolumeCheckpoint,
) error {
	step, resume := replay.step, replay.restore
	if resume.GetArtifactValidated() == nil && resume.GetVolumeProgress() == nil {
		return backupResumeHistoryInvalid()
	}
	progress := proto.CloneOf(resume.GetVolumeProgress())
	if progress == nil {
		progress = &agentpb.BackupVolumeProgress{}
	}
	archive := step.GetRestore().GetVolume().Archive
	pointID, generationID := step.GetRestore().PointId, step.ExecutionId
	validIdentity := func(point, generation string) bool { return point == pointID && generation == generationID }
	switch value := checkpoint.Checkpoint.(type) {
	case *agentpb.BackupVolumeCheckpoint_ServiceProgress:
		service := value.ServiceProgress
		if !validVolumeServiceTransition(service, progress, resume.Phase, step.ConsumerServiceIds) {
			return backupResumeHistoryInvalid()
		}
		progress.ServiceCursor, progress.ServicePhase = service.ServiceCursor, service.Phase
		if service.Phase >= agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT {
			resume.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		}
	case *agentpb.BackupVolumeCheckpoint_ConsumersStopped:
		stopped := value.ConsumersStopped
		if stopped == nil || resume.GetVolumeProgress() == nil && resume.GetArtifactValidated() == nil ||
			progress.ConstructionCursor != 0 || progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
			progress.ServiceCursor != uint32(len(step.ConsumerServiceIds)) ||
			stopped.StoppedCount != volumeRunningConsumerCount(replay.authority, step.ConsumerServiceIds) {
			return backupResumeHistoryInvalid()
		}
		progress.ServicePhase = agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED
		progress.ServiceCursor = 0
	case *agentpb.BackupVolumeCheckpoint_ConstructionIntent:
		intent := value.ConstructionIntent
		if intent == nil || progress.ServicePhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED ||
			progress.PendingConstruction != nil || progress.PendingFinalization != nil ||
			progress.ConstructionCursor+1 != intent.ConstructionCursorBefore+1 ||
			intent.ArchiveOrdinal != progress.ConstructionCursor+1 || intent.ArchiveOrdinal > archive.EntryCount ||
			!validIdentity(intent.PointId, intent.RestoreGenerationId) ||
			!bytes.Equal(intent.NewTreeContentManifestSha256, archive.ContentManifestSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.PendingConstruction = proto.CloneOf(intent)
	case *agentpb.BackupVolumeCheckpoint_ConstructionCompleted:
		complete := value.ConstructionCompleted
		if complete == nil || !proto.Equal(progress.PendingConstruction, complete.Intent) ||
			complete.ConstructionCursorAfter != progress.ConstructionCursor+1 {
			return backupResumeHistoryInvalid()
		}
		progress.ConstructionCursor = complete.ConstructionCursorAfter
		progress.ConstructionChainSha256 = volumeResumeChain(progress.ConstructionChainSha256, complete)
		progress.PendingConstruction = nil
		resume.Cursor.VolumeCursor++
	case *agentpb.BackupVolumeCheckpoint_FinalizationIntent:
		intent := value.FinalizationIntent
		if intent == nil || progress.ConstructionCursor != archive.EntryCount ||
			progress.PendingConstruction != nil || progress.PendingFinalization != nil ||
			intent.FinalizationCursorBefore != progress.FinalizationCursor ||
			intent.FinalizationOrdinal != progress.FinalizationCursor+1 ||
			intent.FinalizationOrdinal > archive.EntryCount || !validIdentity(intent.PointId, intent.RestoreGenerationId) ||
			!bytes.Equal(intent.NewTreeContentManifestSha256, archive.ContentManifestSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.PendingFinalization = proto.CloneOf(intent)
	case *agentpb.BackupVolumeCheckpoint_FinalizationCompleted:
		complete := value.FinalizationCompleted
		if complete == nil || !proto.Equal(progress.PendingFinalization, complete.Intent) ||
			complete.FinalizationCursorAfter != progress.FinalizationCursor+1 {
			return backupResumeHistoryInvalid()
		}
		progress.FinalizationCursor = complete.FinalizationCursorAfter
		progress.FinalizationChainSha256 = volumeResumeChain(progress.FinalizationChainSha256, complete)
		progress.PendingFinalization = nil
		resume.Cursor.VolumeCursor++
	case *agentpb.BackupVolumeCheckpoint_ExchangeIntent:
		intent := value.ExchangeIntent
		if intent == nil || progress.FinalizationCursor != archive.EntryCount ||
			progress.PendingFinalization != nil || progress.PendingExchange != nil ||
			len(progress.OldFullTreeSha256) != 0 || !validIdentity(intent.PointId, intent.RestoreGenerationId) ||
			!bytes.Equal(intent.NewFullTreeSha256, archive.FullTreeSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.PendingExchange = proto.CloneOf(intent)
		progress.OldFullTreeSha256 = append([]byte(nil), intent.OldFullTreeSha256...)
		progress.NewFullTreeSha256 = append([]byte(nil), intent.NewFullTreeSha256...)
		resume.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
	case *agentpb.BackupVolumeCheckpoint_TreeExchanged:
		complete := value.TreeExchanged
		if complete == nil || progress.PendingExchange == nil ||
			!validIdentity(complete.PointId, complete.RestoreGenerationId) ||
			!bytes.Equal(complete.OldFullTreeSha256, progress.OldFullTreeSha256) ||
			!bytes.Equal(complete.NewFullTreeSha256, progress.NewFullTreeSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.PendingExchange = nil
		resume.Cursor.VolumeCursor++
		resume.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_CLEANUP
	case *agentpb.BackupVolumeCheckpoint_ReplacedPathDeleteIntent:
		intent := value.ReplacedPathDeleteIntent
		if intent == nil || progress.PendingExchange != nil || len(progress.OldFullTreeSha256) != sha256.Size ||
			progress.PendingDelete != nil || intent.DeletionCursorBefore != progress.DeletionCursor ||
			intent.DeletionOrdinal != progress.DeletionCursor+1 || intent.DeletionOrdinal > backupvolume.MaxEntries ||
			!validIdentity(intent.PointId, intent.RestoreGenerationId) ||
			!bytes.Equal(intent.OldFullTreeSha256, progress.OldFullTreeSha256) ||
			!bytes.Equal(intent.NewFullTreeSha256, progress.NewFullTreeSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.PendingDelete = proto.CloneOf(intent)
	case *agentpb.BackupVolumeCheckpoint_VolumeReplacedPathCleaned:
		complete := value.VolumeReplacedPathCleaned
		pending := progress.PendingDelete
		if complete == nil || pending == nil || complete.DeletionCursorBefore != progress.DeletionCursor ||
			complete.DeletionOrdinal != progress.DeletionCursor+1 ||
			!validIdentity(complete.PointId, complete.RestoreGenerationId) ||
			!bytes.Equal(complete.LogicalPath, pending.LogicalPath) || complete.Kind != pending.Kind ||
			!bytes.Equal(complete.ParentLogicalPath, pending.ParentLogicalPath) ||
			!bytes.Equal(complete.OldFullTreeSha256, pending.OldFullTreeSha256) ||
			!bytes.Equal(complete.NewFullTreeSha256, pending.NewFullTreeSha256) {
			return backupResumeHistoryInvalid()
		}
		progress.DeletionCursor++
		progress.PendingDelete = nil
		resume.Cursor.VolumeCursor++
		if bytes.Equal(complete.LogicalPath, []byte(".")) {
			resume.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		}
	default:
		return backupResumeHistoryInvalid()
	}
	resume.Checkpoint = &agentpb.BackupRestoreResume_VolumeProgress{VolumeProgress: progress}
	return nil
}

func validBackupVolumeRestoreResume(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step.GetRestore().GetVolume() == nil || state.Cursor == nil ||
		state.Cursor.ObjectAttempt != 1 || state.Cursor.ConfigRecordSequence != 0 ||
		state.Cursor.ConfigValueOrdinal != 0 || len(state.Cursor.CumulativeChainSha256) != 0 {
		return false
	}
	if state.CheckpointSequence == 0 {
		return state.PrecedingCheckpoint == nil && state.Checkpoint == nil && state.Cursor.VolumeCursor == 0 &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION
	}
	if !backupCheckpointFence(state.PrecedingCheckpoint, step.StepDigest) {
		return false
	}
	if artifact := state.GetArtifactValidated(); artifact != nil {
		return state.Cursor.VolumeCursor == 0 &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
			backupResumeRestoreArtifactMatches(artifact, step.GetRestore())
	}
	progress := state.GetVolumeProgress()
	if progress == nil || progress.ConstructionCursor > step.GetRestore().GetVolume().Archive.EntryCount ||
		progress.FinalizationCursor > progress.ConstructionCursor ||
		(progress.ConstructionCursor != 0 && len(progress.ConstructionChainSha256) != sha256.Size) ||
		(progress.FinalizationCursor != 0 && len(progress.FinalizationChainSha256) != sha256.Size) ||
		(progress.PendingExchange != nil && state.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION) {
		return false
	}
	return state.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
		state.Phase <= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TERMINAL
}

func volumeResumeChain(previous []byte, message proto.Message) []byte {
	encoded, err := volumeCheckpointMarshal.Marshal(message)
	if err != nil {
		return nil
	}
	hasher := sha256.New()
	_, _ = hasher.Write(previous)
	_, _ = hasher.Write(encoded)
	return hasher.Sum(nil)
}

func validVolumeServiceTransition(service *agentpb.BackupVolumeServiceProgress,
	progress *agentpb.BackupVolumeProgress, phase agentpb.BackupRestorePhase,
	consumers []string,
) bool {
	if service == nil || len(consumers) == 0 || service.ServiceCursor > uint32(len(consumers)) {
		return false
	}
	recovering := phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
	if !recovering && (phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION ||
		progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED &&
			progress.ServiceCursor == 0) {
		return false
	}
	switch service.Phase {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT:
		return !recovering && service.ServiceCursor == progress.ServiceCursor &&
			int(service.ServiceCursor) < len(consumers) && consumers[service.ServiceCursor] == service.ServiceId &&
			progress.ServicePhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT:
		return recovering && service.ServiceCursor == progress.ServiceCursor &&
			int(service.ServiceCursor) < len(consumers) && consumers[service.ServiceCursor] == service.ServiceId &&
			progress.ServicePhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED:
		return !recovering && service.ServiceCursor == progress.ServiceCursor+1 &&
			consumers[progress.ServiceCursor] == service.ServiceId &&
			progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED:
		return recovering && service.ServiceCursor == progress.ServiceCursor+1 &&
			consumers[progress.ServiceCursor] == service.ServiceId &&
			progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING:
		return service.ServiceCursor == progress.ServiceCursor+1 &&
			consumers[progress.ServiceCursor] == service.ServiceId &&
			progress.ServicePhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
			progress.ServicePhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT
	default:
		return false
	}
}

func volumeRunningConsumerCount(authority *agentpb.BackupTaskAuthority, consumers []string) uint32 {
	var count uint32
	for _, serviceID := range consumers {
		for _, fact := range authority.GetServices() {
			if fact.ServiceId == serviceID && fact.GetPriorRuntimeIntent().GetKind() ==
				agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
				count++
				break
			}
		}
	}
	return count
}
