package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func BackupArtifactEvidenceMatchesWire(native BackupArtifactEvidence, wire *agentpb.BackupArtifactEvidence) bool {
	return wire != nil && native.SourceSizeBytes == wire.SourceSizeBytes &&
		native.StoredSizeBytes == wire.StoredSizeBytes &&
		native.SourceSHA256 == hex.EncodeToString(wire.SourceSha256) &&
		native.StoredSHA256 == hex.EncodeToString(wire.StoredSha256)
}

func backupTargetMatchesWire(native BackupObjectTarget, wire *agentpb.BackupObjectTarget) bool {
	return wire != nil && wire.Connector != nil && wire.Connector.PathStyle != nil &&
		native.ConnectorID == wire.Connector.ConnectorId && native.ConnectorPrefix == wire.Connector.Prefix &&
		native.ConnectorEndpoint == wire.Connector.CanonicalEndpointUrl && native.ConnectorRegion == wire.Connector.Region &&
		native.ConnectorPathStyle == wire.Connector.GetPathStyle() && native.ConnectorBucket == wire.Bucket && native.ObjectKey == wire.ObjectKey
}

func backupObjectMatchesWire(native BackupObjectIdentity, wire *agentpb.BackupObjectIdentity) bool {
	if wire == nil || !backupTargetMatchesWire(native.Target, &agentpb.BackupObjectTarget{
		Connector: wire.Connector, Bucket: wire.Bucket, ObjectKey: wire.ObjectKey,
	}) {
		return false
	}
	switch discriminator := wire.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		return discriminator != nil && discriminator.VersionId != nil &&
			native.Discriminator.Kind == backupobject.DiscriminatorVersionID && native.Discriminator.Value == discriminator.VersionId.Value
	case *agentpb.BackupObjectIdentity_Etag:
		return discriminator != nil && discriminator.Etag != nil &&
			native.Discriminator.Kind == backupobject.DiscriminatorETag && native.Discriminator.Value == discriminator.Etag.Value
	default:
		return false
	}
}
