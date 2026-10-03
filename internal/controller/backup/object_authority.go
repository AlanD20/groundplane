package backup

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupObjectAuthority(point backupruntime.BackupRecoveryPointSnapshot,
	identity backupruntime.BackupObjectIdentity,
) (backupobject.Artifact, backupobject.PruneAuthority, error) {
	if identity.Target != point.ObjectTarget() ||
		point.Object != (backupruntime.BackupObjectIdentity{}) && identity != point.Object {
		return backupobject.Artifact{}, backupobject.PruneAuthority{},
			errs.New(errs.KindStateConflict, "backup selected object changed")
	}
	artifact, err := backupObjectArtifact(point)
	if err != nil {
		return backupobject.Artifact{}, backupobject.PruneAuthority{}, err
	}
	count, digest := artifact.MetadataEvidence()
	authority := backupobject.PruneAuthority{Key: artifact.Key, EnvironmentID: artifact.EnvironmentID,
		SourceID: artifact.SourceID, RecoveryPointID: artifact.RecoveryPointID,
		Evidence: artifact.Evidence,
		Discriminator: backupobject.Discriminator{
			Kind:  identity.Discriminator.Kind,
			Value: identity.Discriminator.Value,
		},
		MetadataCount: count, MetadataSHA256: digest}
	if err := authority.Validate(point.ConnectorPrefix); err != nil {
		return backupobject.Artifact{}, backupobject.PruneAuthority{}, err
	}
	return artifact, authority, nil
}

func backupObjectArtifact(point backupruntime.BackupRecoveryPointSnapshot) (backupobject.Artifact, error) {
	sourceDigest, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil || len(sourceDigest) != 32 {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	storedDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil || len(storedDigest) != 32 {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	artifact := backupobject.Artifact{Key: point.ObjectKey, EnvironmentID: point.EnvironmentID,
		SourceID: point.SourceID, RecoveryPointID: point.ID,
		SourceFormat: backupobject.SourceFormat(
			point.SourceFormat,
		), Encryption: backupobject.Encryption(point.Encryption),
		Evidence: backupobject.Evidence{SourceSizeBytes: point.Evidence.SourceSizeBytes,
			StoredSizeBytes: point.Evidence.StoredSizeBytes}}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if point.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		era := uint64(point.KeyEra)
		artifact.KeyEra = &era
	}
	if err := artifact.Validate(); err != nil || artifact.ExpectedKey(point.ConnectorPrefix) != point.ObjectKey {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	return artifact, nil
}
