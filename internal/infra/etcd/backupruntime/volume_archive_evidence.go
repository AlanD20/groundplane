package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupVolumeArchiveEvidence is selected capture authority retained with a
// recovery point after the producing Task and its live transfer are pruned.
type BackupVolumeArchiveEvidence struct {
	EntryCount            uint64                        `json:"entry_count"`
	ContentManifestSHA256 string                        `json:"content_manifest_sha256"`
	FullTreeSHA256        string                        `json:"full_tree_sha256"`
	SourceSizeBytes       uint64                        `json:"source_size_bytes"`
	Manifest              BackupVolumeManifestReference `json:"manifest"`
}

type BackupVolumeManifestReference struct {
	Binding        backupvolumetransfer.Binding `json:"binding"`
	CursorRevision int64                        `json:"cursor_revision"`
}

func (reference BackupVolumeManifestReference) Valid() bool {
	return reference.Binding.Validate() == nil && reference.CursorRevision > 0 &&
		reference.Binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE
}

func (reference BackupVolumeManifestReference) Owner() (backupvolumemanifest.Owner, error) {
	if !reference.Valid() {
		return backupvolumemanifest.Owner{}, invalidBackupRuntimeRecord("Volume manifest reference is invalid")
	}
	owner := backupvolumemanifest.Owner{Binding: reference.Binding}
	return owner, owner.Validate()
}

func VolumeArchiveEvidenceFromWire(wire *agentpb.BackupVolumeArchiveEvidence) (BackupVolumeArchiveEvidence, error) {
	if _, err := backupvolumetransfer.ArchiveEvidenceFromWire(wire); err != nil {
		return BackupVolumeArchiveEvidence{}, err
	}
	return BackupVolumeArchiveEvidence{
		EntryCount: wire.EntryCount, ContentManifestSHA256: hex.EncodeToString(wire.ContentManifestSha256),
		FullTreeSHA256: hex.EncodeToString(wire.FullTreeSha256), SourceSizeBytes: wire.SourceSizeBytes,
	}, nil
}

func (evidence BackupVolumeArchiveEvidence) Wire() (*agentpb.BackupVolumeArchiveEvidence, error) {
	if evidence.EntryCount == 0 || !recordcodec.ValidSHA256(evidence.ContentManifestSHA256) ||
		!recordcodec.ValidSHA256(evidence.FullTreeSHA256) {
		return nil, invalidBackupRuntimeRecord("Volume archive evidence is incomplete")
	}
	manifest, err := hex.DecodeString(evidence.ContentManifestSHA256)
	if err != nil {
		return nil, invalidBackupRuntimeRecord("Volume manifest digest is invalid")
	}
	tree, err := hex.DecodeString(evidence.FullTreeSHA256)
	if err != nil {
		return nil, invalidBackupRuntimeRecord("Volume tree digest is invalid")
	}
	wire := &agentpb.BackupVolumeArchiveEvidence{EntryCount: evidence.EntryCount,
		ContentManifestSha256: manifest, FullTreeSha256: tree, SourceSizeBytes: evidence.SourceSizeBytes}
	if _, err := backupvolumetransfer.ArchiveEvidenceFromWire(wire); err != nil {
		return nil, err
	}
	return wire, nil
}

func validateSelectedVolumeArchive(
	kind BackupRuntimeSourceKind,
	archive BackupVolumeArchiveEvidence,
	artifact BackupArtifactEvidence,
) error {
	if kind != BackupRuntimeSourceVolume {
		if archive != (BackupVolumeArchiveEvidence{}) {
			return invalidBackupRuntimeRecord("non-Volume source carries Volume archive evidence")
		}
		return nil
	}
	if _, err := archive.Wire(); err != nil {
		return err
	}
	if !archive.Manifest.Valid() {
		return invalidBackupRuntimeRecord("Volume archive has no complete retained manifest reference")
	}
	if artifact != (BackupArtifactEvidence{}) && artifact.SourceSizeBytes != archive.SourceSizeBytes {
		return invalidBackupRuntimeRecord("Volume archive size differs from selected source evidence")
	}
	return nil
}
