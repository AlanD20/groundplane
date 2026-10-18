package backupconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
)

// PrepareRetainedRestore revalidates the owned plaintext archive before file
// replay. It neither downloads a different object nor opens current Entry values.
func PrepareRetainedRestore(ctx context.Context, authority *backupartifact.RestoreAuthority,
	stage *backupstage.Stage, source *backupstage.Artifact, evidence backupstage.ArtifactEvidence,
) (*RestoreArtifact, error) {
	sealed := authority.Sealed()
	if ctx == nil || stage == nil || source == nil || sealed.GetConfig() == nil ||
		evidence.Size != sealed.ExpectedEvidence.SourceSizeBytes ||
		evidence.Name != executionplan.BackupSourceStagingFinal {
		return nil, invalidRestoreArtifact()
	}
	return validateRestoreSource(ctx, sealed, stage, backupartifact.DownloadResult{
		Source: source, SourceEvidence: evidence,
		StoredEvidence: backupstage.ArtifactEvidence{Name: executionplan.BackupStoredStagingFinal},
	})
}
