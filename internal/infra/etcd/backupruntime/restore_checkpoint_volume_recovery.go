package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareVolumeRestoreRecoveryCheckpoint continues the captured tree exchange
// and consumer sequence. Failed outcome remains visible until the complete
// recovery proof settles; pending filesystem intents are never cleared here.
func PrepareVolumeRestoreRecoveryCheckpoint(current BackupRestoreRecord, input *agentpb.BackupWorkInput,
	request *agentpb.BackupWorkCheckpointRequest, old *BackupVolumeOldManifestProof, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.State != BackupRestoreRecoveryRequired ||
		current.Point.SourceKind != BackupRuntimeSourceVolume || current.VolumeProgress == nil ||
		current.VolumeProgress.ServicesRecovered || request == nil || request.GetVolume() == nil {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
			"Volume recovery requires its unfinished captured mutation",
		)
	}
	position := CloneBackupRestoreRecord(current)
	position.Verification = BackupVerificationPending
	progress := position.VolumeProgress
	switch {
	case progress.OldRemoved:
		position.State = BackupRestoreConsumersRestored
	case progress.Exchanged:
		position.State = BackupRestoreExchanged
	case progress.ExchangeIntent:
		position.State = BackupRestoreExchangeReady
	case progress.FinalizationCursor == current.Point.VolumeArchive.EntryCount:
		position.State = BackupRestoreTreeValidated
	case progress.PendingConstruction != 0 || progress.ConstructionCursor != 0:
		position.State = BackupRestoreRestoring
	case progress.ServiceMutationStarted:
		position.State = BackupRestoreConsumersStopped
	default:
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume recovery has no recorded mutation position")
	}
	next, err := PrepareVolumeRestoreCheckpoint(position, input, request, old, at)
	if err != nil {
		return BackupRestoreRecord{}, err
	}
	next.State, next.Verification = BackupRestoreRecoveryRequired, BackupVerificationFailed
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
