package etcd

import (
	hex "encoding/hex"
	testing "testing"

	backupobject "github.com/AlanD20/groundplane/internal/common/backupobject"
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

func backupRuntimePruneObject(
	t *testing.T,
	evidence testbackupplanning.PruneExecutionEvidence,
	ordinal uint32,
) *agentpb.BackupPruneObject {
	t.Helper()
	point := evidence.Prune.Point
	sourceDigest, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil {
		t.Fatal(err)
	}
	storedDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil {
		t.Fatal(err)
	}
	artifact := backupobject.Artifact{
		Key: point.ObjectKey, EnvironmentID: point.EnvironmentID, SourceID: point.SourceID,
		RecoveryPointID: point.ID, SourceFormat: backupobject.SourceFormat(point.SourceFormat),
		Encryption: backupobject.Encryption(point.Encryption),
		Evidence: backupobject.Evidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes, StoredSizeBytes: point.Evidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if point.Encryption == testbackupruntime.BackupRuntimeEncryptionAge {
		era := uint64(point.KeyEra)
		artifact.KeyEra = &era
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
		t.Fatalf("unsupported backup object discriminator %q", point.Object.Discriminator.Kind)
	}
	return &agentpb.BackupPruneObject{
		Ordinal: ordinal, PointId: point.ID,
		Point: &agentpb.RevisionDigest{ModRevision: evidence.PointRevision, Sha256: evidence.PointSHA256},
		Evidence: &agentpb.BackupArtifactEvidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes, SourceSha256: sourceDigest,
			StoredSizeBytes: point.Evidence.StoredSizeBytes, StoredSha256: storedDigest,
		},
		Object: object, MetadataCount: metadataCount, MetadataSha256: metadataDigest[:],
	}
}

// Rationale: stable-id validation must fail before malformed identifiers can
// select arbitrary durable key suffixes.
