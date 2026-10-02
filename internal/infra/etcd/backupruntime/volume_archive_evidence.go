package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
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
	TaskID               string `json:"task_id"`
	AssignmentID         string `json:"assignment_id"`
	StepID               string `json:"step_id"`
	TransferID           string `json:"transfer_id"`
	AuthoritySHA256      string `json:"authority_sha256"`
	AgentID              string `json:"agent_id"`
	AgentGeneration      uint64 `json:"agent_generation"`
	AssignmentGeneration uint64 `json:"assignment_generation"`
	CursorRevision       int64  `json:"cursor_revision"`
}

func (reference BackupVolumeManifestReference) Valid() bool {
	_, transferErr := ulid.ParseStrict(reference.TransferID)
	return ids.Validate(ids.KindTask, reference.TaskID) == nil &&
		ids.Validate(ids.KindAssignment, reference.AssignmentID) == nil &&
		ids.Validate(ids.KindStep, reference.StepID) == nil && transferErr == nil &&
		recordcodec.ValidSHA256(reference.AuthoritySHA256) && ids.Validate(ids.KindAgent, reference.AgentID) == nil &&
		reference.AgentGeneration > 0 && reference.AssignmentGeneration > 0 && reference.CursorRevision > 0
}

func (reference BackupVolumeManifestReference) Owner() (backupvolumemanifest.Owner, error) {
	if !reference.Valid() {
		return backupvolumemanifest.Owner{}, invalidBackupRuntimeRecord("Volume manifest reference is invalid")
	}
	digest, err := hex.DecodeString(reference.AuthoritySHA256)
	if err != nil {
		return backupvolumemanifest.Owner{}, err
	}
	binding := backupvolumetransfer.Binding{TaskID: reference.TaskID,
		AssignmentID: reference.AssignmentID, StepID: reference.StepID, TransferID: reference.TransferID,
		Direction: agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE}
	copy(binding.AuthorityDigest[:], digest)
	owner := backupvolumemanifest.Owner{Binding: binding, AgentID: reference.AgentID,
		AgentGeneration: reference.AgentGeneration, AssignmentGeneration: reference.AssignmentGeneration}
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
