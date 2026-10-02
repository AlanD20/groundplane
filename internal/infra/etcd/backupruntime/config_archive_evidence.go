package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Config archive authority survives Task pruning with the selected object.
// These fields are validated capture evidence, not a recipe to discover a
// replacement archive or reconstruct missing authority from current Entries.
type BackupConfigArchiveEvidence struct {
	ManifestSHA256          string `json:"manifest_sha256"`
	MetadataSnapshotSHA256  string `json:"metadata_snapshot_sha256"`
	EntryCount              uint32 `json:"entry_count"`
	TotalSelectedValueBytes uint64 `json:"total_selected_value_bytes"`
	ManifestSizeBytes       uint64 `json:"manifest_size_bytes"`
	SourceSizeBytes         uint64 `json:"source_size_bytes"`
	CaptureTranscriptSHA256 string `json:"capture_transcript_sha256"`
}

func ConfigArchiveEvidenceFromCapture(
	completed *agentpb.BackupConfigTransferCompleted,
) (BackupConfigArchiveEvidence, error) {
	var result BackupConfigArchiveEvidence
	if completed == nil || completed.RestoreGenerationId != "" || completed.RenderGeneration != 0 ||
		completed.Content == nil || executionplan.RejectUnknown(completed) != nil ||
		len(completed.TransferTranscriptSha256) != 32 {
		return result, invalidBackupRuntimeRecord("Config capture archive evidence is invalid")
	}
	content := completed.Content
	result = BackupConfigArchiveEvidence{ManifestSHA256: hex.EncodeToString(content.ManifestSha256),
		MetadataSnapshotSHA256: hex.EncodeToString(content.MetadataSnapshotSha256), EntryCount: content.EntryCount,
		TotalSelectedValueBytes: content.TotalSelectedValueBytes, ManifestSizeBytes: content.ManifestSizeBytes,
		SourceSizeBytes: content.SourceSizeBytes, CaptureTranscriptSHA256: hex.EncodeToString(completed.TransferTranscriptSha256)}
	if _, err := result.Wire(); err != nil {
		return BackupConfigArchiveEvidence{}, err
	}
	return result, nil
}

func (evidence BackupConfigArchiveEvidence) Wire() (*agentpb.BackupConfigArchiveEvidence, error) {
	if !recordcodec.ValidSHA256(evidence.ManifestSHA256) || !recordcodec.ValidSHA256(evidence.MetadataSnapshotSHA256) ||
		!recordcodec.ValidSHA256(evidence.CaptureTranscriptSHA256) {
		return nil, invalidBackupRuntimeRecord("Config archive evidence is incomplete")
	}
	manifest, err := hex.DecodeString(evidence.ManifestSHA256)
	if err != nil {
		return nil, invalidBackupRuntimeRecord("Config archive manifest digest is invalid")
	}
	metadata, err := hex.DecodeString(evidence.MetadataSnapshotSHA256)
	if err != nil {
		return nil, invalidBackupRuntimeRecord("Config archive metadata digest is invalid")
	}
	transcript, err := hex.DecodeString(evidence.CaptureTranscriptSHA256)
	if err != nil {
		return nil, invalidBackupRuntimeRecord("Config archive transcript digest is invalid")
	}
	content := &agentpb.BackupConfigContentAuthority{ManifestSha256: manifest, MetadataSnapshotSha256: metadata,
		EntryCount: evidence.EntryCount, TotalSelectedValueBytes: evidence.TotalSelectedValueBytes,
		ManifestSizeBytes: evidence.ManifestSizeBytes, SourceSizeBytes: evidence.SourceSizeBytes}
	if _, err := backupconfigtransfer.ContentSHA256(content); err != nil {
		return nil, err
	}
	return &agentpb.BackupConfigArchiveEvidence{Content: content, CaptureTranscriptSha256: transcript}, nil
}

func validateSelectedConfigArchive(
	kind BackupRuntimeSourceKind,
	archive BackupConfigArchiveEvidence,
	artifact BackupArtifactEvidence,
) error {
	if kind != BackupRuntimeSourceConfig {
		if archive != (BackupConfigArchiveEvidence{}) {
			return invalidBackupRuntimeRecord("non-Config source carries Config archive evidence")
		}
		return nil
	}
	if _, err := archive.Wire(); err != nil {
		return err
	}
	if artifact != (BackupArtifactEvidence{}) && artifact.SourceSizeBytes != archive.SourceSizeBytes {
		return invalidBackupRuntimeRecord("Config archive size differs from selected source evidence")
	}
	return nil
}
