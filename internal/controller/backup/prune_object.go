package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func buildBackupPruneObject(
	evidence backupplanning.PruneExecutionEvidence,
	ordinal uint32,
) (*agentpb.BackupPruneObject, error) {
	point := evidence.Prune.Point
	if err := backupruntime.ValidateBackupRecoveryPointSnapshot(point); err != nil {
		return nil, err
	}
	policyDigest, err := hex.DecodeString(evidence.Prune.PolicySHA256)
	if err != nil || len(policyDigest) != sha256.Size || evidence.Prune.PolicyRevision <= 0 ||
		evidence.RetentionPolicy == nil || evidence.RetentionPolicy.ModRevision != evidence.Prune.PolicyRevision ||
		!bytes.Equal(evidence.RetentionPolicy.Sha256, policyDigest) || evidence.ConnectorAuthority == nil ||
		evidence.PointRevision != evidence.Prune.PointRevision || evidence.PointRevision <= 0 || len(evidence.PointSHA256) != sha256.Size {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup prune retained policy or point authority is incomplete or changed",
		)
	}
	sourceDigest, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil || len(sourceDigest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backup prune source digest is invalid")
	}
	storedDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil || len(storedDigest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backup prune stored digest is invalid")
	}
	artifact := backupobject.Artifact{
		Key: point.ObjectKey, EnvironmentID: point.EnvironmentID, SourceID: point.SourceID, RecoveryPointID: point.ID,
		SourceFormat: backupobject.SourceFormat(
			point.SourceFormat,
		), Encryption: backupobject.Encryption(point.Encryption),
		Evidence: backupobject.Evidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes,
			StoredSizeBytes: point.Evidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if point.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		era := uint64(point.KeyEra)
		artifact.KeyEra = &era
	}
	if err := artifact.Validate(); err != nil {
		return nil, err
	}
	metadataCount, metadataDigest := artifact.MetadataEvidence()
	object := &agentpb.BackupObjectIdentity{
		Connector: proto.CloneOf(evidence.ConnectorAuthority), Bucket: point.Object.Target.ConnectorBucket,
		ObjectKey: point.Object.Target.ObjectKey,
	}
	switch point.Object.Discriminator.Kind {
	case backupobject.DiscriminatorVersionID:
		object.Discriminator = &agentpb.BackupObjectIdentity_VersionId{
			VersionId: &agentpb.BackupS3VersionId{Value: point.Object.Discriminator.Value},
		}
	case backupobject.DiscriminatorETag:
		object.Discriminator = &agentpb.BackupObjectIdentity_Etag{
			Etag: &agentpb.BackupS3ETag{Value: point.Object.Discriminator.Value},
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup prune selected immutable discriminator is missing")
	}
	return &agentpb.BackupPruneObject{
		Ordinal: ordinal, PointId: point.ID,
		Point: &agentpb.RevisionDigest{
			ModRevision: evidence.PointRevision,
			Sha256:      append([]byte(nil), evidence.PointSHA256...),
		},
		Evidence: &agentpb.BackupArtifactEvidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes,
			SourceSha256:    sourceDigest,
			StoredSizeBytes: point.Evidence.StoredSizeBytes,
			StoredSha256:    storedDigest,
		},
		Object: object, MetadataCount: metadataCount, MetadataSha256: append([]byte(nil), metadataDigest[:]...),
	}, nil
}
