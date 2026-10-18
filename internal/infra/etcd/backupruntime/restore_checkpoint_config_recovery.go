package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareConfigRestoreRecoveryCheckpoint records file and cleanup proofs for
// the already-published generation, without discarding the failed outcome.
func PrepareConfigRestoreRecoveryCheckpoint(current BackupRestoreRecord, input *agentpb.BackupWorkInput,
	request *agentpb.BackupWorkCheckpointRequest, publication BackupRestoreConfigProgress, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.State != BackupRestoreRecoveryRequired ||
		current.Point.SourceKind != BackupRuntimeSourceConfig || current.ConfigProgress == nil ||
		current.ConfigProgress.SourceCleanupCompleted || request == nil ||
		(request.GetConfig().GetMaterializationVerified() == nil && request.GetSourceCleanupCompleted() == nil &&
			request.GetConfig().GetTransferCompleted() == nil) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Config recovery requires its published generation and remaining file or cleanup proof")
	}
	progress := current.ConfigProgress
	if progress.PublishedHeadRevision <= 0 || progress.DeleteEntryOrdinal != progress.DeleteEntryCount ||
		progress.UpsertEntryOrdinal != current.Point.ConfigArchive.EntryCount {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Config recovery must finish publishing its captured generation before file verification")
	}
	if completed := request.GetConfig().GetTransferCompleted(); completed != nil {
		archive, err := current.Point.ConfigArchive.Wire()
		if err != nil || executionplan.RejectUnknown(request) != nil ||
			request.GetAttempt().GetTaskId() != current.TaskID || ValidateRestoreWorkInput(current, input) != nil ||
			!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) ||
			completed.RestoreGenerationId != current.RestoreGenerationID ||
			completed.RenderGeneration != current.CurrentTarget.Config.RenderGeneration ||
			!proto.Equal(completed.Content, archive.Content) || progress.MaterializationSHA256 != "" ||
			publication.RevisionRootRevision != progress.RevisionRootRevision ||
			publication.ExpectedMaterializationSHA256 != progress.ExpectedMaterializationSHA256 {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Config recovery transfer receipt differs from the published generation")
		}
		// Publication is already durable. Restore only its missing source receipt,
		// never rewind the Entry mutation cursors or clear the original failure.
		next := CloneBackupRestoreRecord(current)
		next.UpdatedAt = at
		return next, nil
	}
	position := CloneBackupRestoreRecord(current)
	position.State, position.Verification = BackupRestoreCanonicalComplete, BackupVerificationPending
	if progress.MaterializationSHA256 != "" {
		position.State, position.Verification = BackupRestoreVerified, BackupVerificationPassed
	}
	next, err := PrepareConfigRestoreCheckpoint(position, input, request, publication, at)
	if err != nil {
		return BackupRestoreRecord{}, err
	}
	next.State, next.Verification = BackupRestoreRecoveryRequired, BackupVerificationFailed
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
