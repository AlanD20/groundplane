package executionplan

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validBackupCheckpointEvidence(value *agentpb.BackupArtifactEvidence) bool {
	if value == nil || value.SourceSizeBytes == 0 || value.StoredSizeBytes == 0 ||
		value.SourceSizeBytes > backupformat.MaxStoredBytes || value.StoredSizeBytes > backupformat.MaxStoredBytes ||
		!backupCheckpointDigest(value.SourceSha256) || !backupCheckpointDigest(value.StoredSha256) {
		return false
	}
	if value.SourceSizeBytes == value.StoredSizeBytes {
		return bytes.Equal(value.SourceSha256, value.StoredSha256)
	}
	storedSize, err := backupformat.AgeStoredSize(value.SourceSizeBytes)
	return err == nil && value.StoredSizeBytes == storedSize
}

func validBackupCheckpointFinals(value *agentpb.BackupStagingFinals, evidence *agentpb.BackupArtifactEvidence) bool {
	if value == nil || value.SameInode == nil || !validBackupCheckpointEvidence(evidence) ||
		!backupCheckpointFinalName(value.SourceRelativeName) || !backupCheckpointFinalName(value.StoredRelativeName) {
		return false
	}
	if value.GetSameInode() {
		return value.SourceRelativeName == BackupSourceStagingFinal &&
			value.StoredRelativeName == BackupSourceStagingFinal &&
			evidence.SourceSizeBytes == evidence.StoredSizeBytes &&
			bytes.Equal(evidence.SourceSha256, evidence.StoredSha256)
	}
	return value.SourceRelativeName == BackupSourceStagingFinal && value.StoredRelativeName == BackupStoredStagingFinal
}

func backupCheckpointFinalName(value string) bool {
	return len(value) > 0 && len(value) <= 255 && value != "." && value != ".." &&
		utf8.ValidString(value) && !strings.ContainsAny(value, "/\\\x00")
}

func validBackupCheckpointMetadata(count uint32, digest []byte) bool {
	// MySQL adds exact observed server and tool versions to the complete set.
	return (count >= 10 && count <= 13) && backupCheckpointDigest(digest)
}

func backupCheckpointRevision(value *agentpb.RevisionDigest) bool {
	return value != nil && value.ModRevision > 0 && backupCheckpointDigest(value.Sha256)
}

func validBackupCheckpointConnector(value *agentpb.BackupConnectorAuthority) bool {
	return value != nil && ids.Validate(ids.KindConnector, value.ConnectorId) == nil &&
		backupCheckpointRevision(value.Connector) && s3connector.ValidEndpoint(value.CanonicalEndpointUrl) &&
		s3connector.ValidRegion(value.Region) && s3connector.ValidPrefix(value.Prefix) && value.PathStyle != nil &&
		backupCheckpointFinalName(value.AccessKeySlotId) && backupCheckpointFinalName(value.SecretKeySlotId) &&
		value.AccessKeySlotId != value.SecretKeySlotId && backupCheckpointRevision(value.AccessKeySlot) &&
		backupCheckpointRevision(value.SecretKeySlot)
}

func validBackupCheckpointTarget(value *agentpb.BackupObjectTarget, pointID string) bool {
	return value != nil && validBackupCheckpointConnector(value.Connector) && s3connector.ValidBucket(value.Bucket) &&
		backupCheckpointObjectKey(value.ObjectKey, value.Connector.Prefix, pointID)
}

func backupCheckpointObjectKey(key, prefix, pointID string) bool {
	if !backupCheckpointPoint(pointID) || !utf8.ValidString(key) || len(key) > 1024 ||
		!strings.HasPrefix(key, prefix) || strings.ContainsAny(key, "\\\x00") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	return len(parts) == 4 && ids.Validate(ids.KindEnvironment, parts[0]) == nil &&
		ids.Validate(ids.KindBackupSource, parts[1]) == nil && parts[2] == pointID && parts[3] == "artifact.bin"
}

func validBackupCheckpointObject(value *agentpb.BackupObjectIdentity, pointID string) bool {
	if value == nil || !validBackupCheckpointConnector(value.Connector) || !s3connector.ValidBucket(value.Bucket) ||
		!backupCheckpointObjectKey(value.ObjectKey, value.Connector.Prefix, pointID) {
		return false
	}
	var discriminator backupobject.Discriminator
	switch kind := value.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		if kind == nil || kind.VersionId == nil {
			return false
		}
		discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorVersionID, Value: kind.VersionId.Value}
	case *agentpb.BackupObjectIdentity_Etag:
		if kind == nil || kind.Etag == nil {
			return false
		}
		discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: kind.Etag.Value}
	default:
		return false
	}
	return discriminator.Validate() == nil
}

func validBackupUploadCompleted(value *agentpb.BackupUploadCompleted) bool {
	if value == nil || !backupCheckpointPoint(value.PointId) || !validBackupCheckpointEvidence(value.Evidence) ||
		!validBackupCheckpointTarget(value.Target, value.PointId) ||
		!validBackupCheckpointMetadata(value.MetadataCount, value.MetadataSha256) {
		return false
	}
	switch outcome := value.Outcome.(type) {
	case *agentpb.BackupUploadCompleted_ReturnedObject:
		return outcome != nil && validBackupCheckpointObject(outcome.ReturnedObject, value.PointId) &&
			outcome.ReturnedObject.Bucket == value.Target.Bucket && outcome.ReturnedObject.ObjectKey == value.Target.ObjectKey &&
			proto.Equal(outcome.ReturnedObject.Connector, value.Target.Connector)
	case *agentpb.BackupUploadCompleted_Unknown:
		return outcome != nil && outcome.Unknown != nil
	default:
		return false
	}
}

func validBackupArtifactPrepared(value *agentpb.BackupArtifactPrepared) bool {
	if value == nil || !backupCheckpointPoint(value.PointId) ||
		!validBackupCheckpointFinals(value.Finals, value.Evidence) {
		return false
	}
	switch archive := value.Archive.(type) {
	case *agentpb.BackupArtifactPrepared_Postgres:
		return archive != nil && validBackupCheckpointPostgresArchive(archive.Postgres)
	case *agentpb.BackupArtifactPrepared_Mysql:
		return archive != nil && validBackupCheckpointMySQLArchive(archive.Mysql)
	case *agentpb.BackupArtifactPrepared_Config:
		return archive != nil && validBackupCheckpointConfigArchive(archive.Config, value.Evidence)
	case *agentpb.BackupArtifactPrepared_Volume:
		return archive != nil && validBackupCheckpointVolumeArchive(archive.Volume, value.Evidence)
	default:
		return false
	}
}

func validBackupRestoreArtifact(value *agentpb.BackupRestoreArtifactValidated) bool {
	if value == nil || !backupCheckpointPoint(value.PointId) ||
		!validBackupCheckpointFinals(value.Finals, value.Evidence) ||
		!validBackupCheckpointObject(value.Object, value.PointId) {
		return false
	}
	switch archive := value.Archive.(type) {
	case *agentpb.BackupRestoreArtifactValidated_Postgres:
		return archive != nil && validBackupCheckpointPostgresArchive(archive.Postgres)
	case *agentpb.BackupRestoreArtifactValidated_Mysql:
		return archive != nil && validBackupCheckpointMySQLArchive(archive.Mysql)
	case *agentpb.BackupRestoreArtifactValidated_Config:
		return archive != nil && validBackupCheckpointConfigArchive(archive.Config, value.Evidence)
	case *agentpb.BackupRestoreArtifactValidated_Volume:
		return archive != nil && validBackupCheckpointVolumeArchive(archive.Volume, value.Evidence)
	default:
		return false
	}
}

func validBackupCheckpointMySQLArchive(value *agentpb.BackupMySQLArchiveEvidence) bool {
	_, err := backupmysql.FromWire(value)
	return err == nil
}

func validBackupCheckpointPostgresArchive(value *agentpb.BackupPostgresArchiveEvidence) bool {
	_, err := backuppostgres.FromWire(value)
	return err == nil
}

func validBackupCheckpointConfigContent(value *agentpb.BackupConfigContentAuthority) bool {
	return value != nil && backupCheckpointDigest(value.ManifestSha256) &&
		backupCheckpointDigest(value.MetadataSnapshotSha256) &&
		value.EntryCount <= backupconfig.MaxEntries &&
		value.TotalSelectedValueBytes <= backupconfig.MaxTotalSelectedValueBytes &&
		value.ManifestSizeBytes > 0 &&
		value.ManifestSizeBytes <= backupconfig.MaxManifestBytes &&
		value.SourceSizeBytes > 0 &&
		value.SourceSizeBytes <= backupconfig.MaxSourceBytes &&
		value.SourceSizeBytes%backupconfig.TarBlockBytes == 0
}

func validBackupCheckpointConfigArchive(
	value *agentpb.BackupConfigArchiveEvidence,
	evidence *agentpb.BackupArtifactEvidence,
) bool {
	if value == nil || !validBackupCheckpointConfigContent(value.Content) ||
		!backupCheckpointDigest(value.CaptureTranscriptSha256) ||
		value.Content.SourceSizeBytes != evidence.SourceSizeBytes {
		return false
	}
	storedSize, err := backupconfig.AgeStoredSize(evidence.SourceSizeBytes)
	return err == nil && evidence.StoredSizeBytes == storedSize
}

func validBackupCheckpointVolumeArchive(
	value *agentpb.BackupVolumeArchiveEvidence,
	evidence *agentpb.BackupArtifactEvidence,
) bool {
	if value == nil || !backupCheckpointDigest(value.ContentManifestSha256) ||
		!backupCheckpointDigest(value.FullTreeSha256) {
		return false
	}
	artifact := backupvolume.ArtifactEvidence{
		Source:  backupformat.Evidence{SizeBytes: evidence.SourceSizeBytes},
		Archive: backupvolume.ArchiveEvidence{EntryCount: value.EntryCount, SourceSizeBytes: value.SourceSizeBytes},
	}
	copy(artifact.Source.SHA256[:], evidence.SourceSha256)
	copy(artifact.Archive.ContentManifestSHA256[:], value.ContentManifestSha256)
	copy(artifact.Archive.FullTreeSHA256[:], value.FullTreeSha256)
	return artifact.Validate() == nil
}
