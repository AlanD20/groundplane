package backupruntime

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareConfigRestoreCheckpoint changes only native progress. Its writer
// must atomically fence the selected plan, receipt and full staged generation.
func PrepareConfigRestoreCheckpoint(current BackupRestoreRecord, request *agentpb.BackupCheckpointRequest,
	publication BackupRestoreConfigProgress, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceConfig ||
		request == nil || request.TaskId != current.TaskID || !ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("config restore checkpoint identity is invalid")
	}
	if _, err := executionplan.ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err != nil {
		return BackupRestoreRecord{}, err
	}
	next := CloneBackupRestoreRecord(current)
	next.UpdatedAt = at
	if validated := request.GetRestoreArtifactValidated(); validated != nil {
		archive, err := current.Point.ConfigArchive.Wire()
		if err != nil || (current.State != BackupRestoreQueued && current.State != BackupRestoreDownloading) ||
			publication != (BackupRestoreConfigProgress{}) || validated.PointId != current.Point.ID ||
			!backupObjectMatchesWire(current.Point.Object, validated.Object) ||
			!BackupArtifactEvidenceMatchesWire(
				current.Point.Evidence,
				validated.Evidence,
			) || !proto.Equal(archive, validated.GetConfig()) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
				"config restore validated artifact differs from selected authority",
			)
		}
		artifact := current.Point.Evidence
		next.Artifact, next.State = &artifact, BackupRestoreArtifactVerified
	} else if completed := request.GetConfig().GetTransferCompleted(); completed != nil {
		archive, err := current.Point.ConfigArchive.Wire()
		if err != nil || (current.State != BackupRestoreArtifactVerified && current.State != BackupRestoreReceiving) ||
			completed.RestoreGenerationId != current.RestoreGenerationID || completed.RenderGeneration != current.CurrentTarget.Config.RenderGeneration ||
			!proto.Equal(completed.Content, archive.Content) || publication.RevisionRootRevision <= 0 ||
			publication.DeleteEntryOrdinal != 0 || publication.UpsertEntryOrdinal != 0 ||
			publication.PublishedHeadRevision != 0 || publication.MaterializationSHA256 != "" {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("config restore completed transfer differs from staged authority")
		}
		progress := publication
		next.ConfigProgress, next.State = &progress, BackupRestoreStaged
	} else if materialized := request.GetConfig().GetMaterializationVerified(); materialized != nil {
		if (current.State != BackupRestoreCanonicalComplete && current.State != BackupRestoreMaterializing) ||
			publication != (BackupRestoreConfigProgress{}) || materialized.RestoreGenerationId != current.RestoreGenerationID ||
			materialized.MaterializedEntryCount != current.Point.ConfigArchive.EntryCount ||
			hex.EncodeToString(materialized.MaterializationSha256) != current.ConfigProgress.ExpectedMaterializationSHA256 {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("config restore file proof differs from selected generation")
		}
		next.State, next.Verification = BackupRestoreVerified, BackupVerificationPassed
		next.ConfigProgress.MaterializationSHA256 = current.ConfigProgress.ExpectedMaterializationSHA256
	} else if cleanup := request.GetSourceCleanupCompleted(); cleanup != nil {
		if current.State != BackupRestoreVerified || current.ConfigProgress.SourceCleanupCompleted ||
			publication != (BackupRestoreConfigProgress{}) || cleanup.PointId != current.Point.ID ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, cleanup.Evidence) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("config restore cleanup differs from verified authority")
		}
		next.ConfigProgress.SourceCleanupCompleted = true
	} else {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("config restore checkpoint has an invalid phase")
	}
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
