package executionplan

import (
	"bytes"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validBackupResourceAuthority(value *agentpb.BackupResourceIdentity) bool {
	if value == nil || !backupCheckpointRevision(value.Resource) {
		return false
	}
	switch value.Kind {
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH:
		return ids.Validate(ids.KindAttach, value.ResourceId) == nil
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT:
		return ids.Validate(ids.KindEnvironment, value.ResourceId) == nil
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME:
		return ids.Validate(ids.KindVolume, value.ResourceId) == nil
	default:
		return false
	}
}

func validBackupEncryptionAuthority(value *agentpb.BackupEncryptionAuthority) bool {
	if value == nil {
		return false
	}
	switch value.Kind {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		return value.SecretSlotId == "" && value.SecretSlot == nil && len(value.RecipientSha256) == 0 &&
			value.KeyEra == nil
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		if !backupCheckpointDigest(value.RecipientSha256) || value.KeyEra == nil || value.GetKeyEra() == 0 {
			return false
		}
		switch value.SecretSlotId {
		case backupsecret.CurrentAgeIdentitySlotID:
			return backupCheckpointRevision(value.SecretSlot)
		case backupsecret.OperatorOldAgeIdentitySlotID:
			// Request-only private material has no durable Secret revision.
			return value.SecretSlot == nil
		default:
			return false
		}
	default:
		return false
	}
}

func validBackupCaptureAuthority(value *agentpb.BackupCaptureAuthority) bool {
	if value == nil || !backupCheckpointPoint(value.PointId) || !validBackupResourceAuthority(value.Resource) ||
		!validBackupCheckpointTarget(value.Target, value.PointId) || !validBackupEncryptionAuthority(value.Encryption) {
		return false
	}
	if value.Encryption.SecretSlotId == backupsecret.OperatorOldAgeIdentitySlotID {
		return false
	}
	switch source := value.Source.(type) {
	case *agentpb.BackupCaptureAuthority_Postgres:
		if source == nil || source.Postgres == nil || value.Resource.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH {
			return false
		}
		postgres := source.Postgres
		maximum := backupformat.MaxStoredBytes
		if value.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			maximum = backupformat.MaxAgeSourceBytes
		}
		_, releaseErr := postgres16protocol.DecodeManagedReleaseIndex(postgres.ManagedReleaseIndex)
		return releaseErr == nil && postgres.AdapterContractVersion == postgres16protocol.AdapterContractVersion &&
			backupCheckpointServiceID(postgres.DatabaseServiceId) && postgresidentity.ValidGenerated(postgres.DatabaseName) &&
			postgresidentity.ValidGenerated(postgres.RoleName) && postgres.MaxPlaintextBytes == maximum
	case *agentpb.BackupCaptureAuthority_Mysql:
		if source == nil || source.Mysql == nil || value.Resource.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH {
			return false
		}
		mysql := source.Mysql
		maximum := backupformat.MaxStoredBytes
		if value.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			maximum = backupformat.MaxAgeSourceBytes
		}
		return mysql.AdapterContractVersion == mysql84protocol.AdapterContractVersion &&
			mysql.RequiredServerMajor == mysql84protocol.ServerMajor && mysql.RequiredServerMinor == mysql84protocol.ServerMinor &&
			backupCheckpointServiceID(mysql.DatabaseServiceId) && mysql84protocol.ValidGeneratedIdentity(mysql.DatabaseName) &&
			mysql84protocol.ValidGeneratedIdentity(mysql.RoleName) && mysql.MaxPlaintextBytes == maximum &&
			backupCheckpointDigest(mysql.DatabaseImageReferenceSha256)
	case *agentpb.BackupCaptureAuthority_Config:
		if source == nil || source.Config == nil || value.Resource.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT ||
			value.Encryption.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return false
		}
		config := source.Config
		return config.EnvironmentId == value.Resource.ResourceId && validBackupCheckpointConfigContent(config.Content) &&
			config.MetadataSnapshotRevision > 0 && config.MetadataEntryCount == config.Content.EntryCount &&
			config.MetadataProtoBytes <= backupconfig.MaxDurableMetadataBytes
	case *agentpb.BackupCaptureAuthority_Volume:
		if source == nil || source.Volume == nil || value.Resource.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME {
			return false
		}
		volume := source.Volume
		return volume.VolumeId == value.Resource.ResourceId && proto.Equal(volume.Volume, value.Resource.Resource) &&
			volume.SourceSizeUpperBound > 0 && volume.SourceSizeUpperBound <= backupformat.MaxStoredBytes && validBackupVolumeProjection(volume.Projection)
	default:
		return false
	}
}

func validBackupVolumeProjection(value *agentpb.BackupVolumeProjectionAuthority) bool {
	return value != nil && ids.Validate(ids.KindConfig, value.ArtifactId) == nil &&
		backupCheckpointDigest(value.ArtifactSha256) &&
		value.ArtifactRevision > 0 &&
		value.ProjectionRoot > 0 &&
		value.RenderGeneration > 0 &&
		value.ComposeVolumeKey != "" &&
		value.DockerVolumeName != "" &&
		filepath.IsAbs(value.AuthorizedVolumeDir) &&
		filepath.Clean(value.AuthorizedVolumeDir) == value.AuthorizedVolumeDir
}

func validBackupPruneAuthority(value *agentpb.BackupPruneAuthority) bool {
	if value == nil || !backupCheckpointRevision(value.RetentionPolicy) || len(value.Objects) == 0 ||
		len(value.Objects) > MaximumBackupPrunePoints {
		return false
	}
	points := make(map[string]struct{}, len(value.Objects))
	var ordinal uint32
	for _, object := range value.Objects {
		if object == nil || object.Ordinal <= ordinal || object.Ordinal > MaximumBackupPrunePoints ||
			!backupCheckpointPoint(
				object.PointId,
			) || !backupCheckpointRevision(object.Point) || !validBackupCheckpointEvidence(object.Evidence) ||
			!validBackupCheckpointObject(
				object.Object,
				object.PointId,
			) || !validBackupCheckpointMetadata(object.MetadataCount, object.MetadataSha256) {
			return false
		}
		if _, exists := points[object.PointId]; exists {
			return false
		}
		points[object.PointId] = struct{}{}
		ordinal = object.Ordinal
	}
	return true
}

func validBackupRestoreAuthority(value *agentpb.BackupRestoreAuthority) bool {
	if value == nil || !backupCheckpointPoint(value.PointId) || !validBackupResourceAuthority(value.Destination) ||
		!validBackupCheckpointObject(
			value.SourceObject,
			value.PointId,
		) || !validBackupCheckpointEvidence(value.ExpectedEvidence) ||
		!validBackupEncryptionAuthority(value.Encryption) || !validBackupRestoreIdentity(value) {
		return false
	}
	switch source := value.Source.(type) {
	case *agentpb.BackupRestoreAuthority_Postgres:
		if source == nil || source.Postgres == nil || value.Destination.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH {
			return false
		}
		postgres := source.Postgres
		_, releaseErr := postgres16protocol.DecodeManagedReleaseIndex(postgres.ManagedReleaseIndex)
		_, archiveErr := backuppostgres.FromWire(postgres.ExpectedArchive)
		versions, versionErr := databaseversion.FromWire(postgres.ExpectedTargetVersions)
		return releaseErr == nil && archiveErr == nil && versionErr == nil && versions.Family == "postgres" &&
			postgres.AdapterContractVersion == postgres16protocol.AdapterContractVersion &&
			backupCheckpointServiceID(postgres.DatabaseServiceId) && postgresidentity.ValidGenerated(postgres.DatabaseName) && postgresidentity.ValidGenerated(postgres.RoleName)
	case *agentpb.BackupRestoreAuthority_Mysql:
		if source == nil || source.Mysql == nil || value.Destination.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH {
			return false
		}
		mysql := source.Mysql
		_, archiveErr := backupmysql.FromWire(mysql.ExpectedArchive)
		versions, versionErr := databaseversion.FromWire(mysql.ExpectedTargetVersions)
		return archiveErr == nil && versionErr == nil && versions.Family == "mysql" && mysql.AdapterContractVersion == mysql84protocol.AdapterContractVersion &&
			mysql.RequiredServerMajor == mysql84protocol.ServerMajor && mysql.RequiredServerMinor == mysql84protocol.ServerMinor &&
			backupCheckpointServiceID(mysql.DatabaseServiceId) && mysql84protocol.ValidGeneratedIdentity(mysql.DatabaseName) &&
			mysql84protocol.ValidGeneratedIdentity(mysql.RoleName) && backupCheckpointDigest(mysql.DatabaseImageReferenceSha256)
	case *agentpb.BackupRestoreAuthority_Config:
		if source == nil || source.Config == nil || value.Destination.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT ||
			value.Encryption.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return false
		}
		return source.Config.DestinationEnvironmentId == value.Destination.ResourceId &&
			backupconfigmaterialization.ValidateContext(source.Config.DestinationEnvironmentId, source.Config.Files) == nil &&
			ids.Validate(ids.KindConfig, source.Config.RestoreGenerationId) == nil && source.Config.RenderGeneration > 0 &&
			ids.Validate(ids.KindTask, source.Config.BaselineRevisionId) == nil && source.Config.BaselineHeadRevision > 0 &&
			validBackupCheckpointConfigArchive(source.Config.ExpectedArchive, value.ExpectedEvidence)
	case *agentpb.BackupRestoreAuthority_Volume:
		if source == nil || source.Volume == nil || value.Destination.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME {
			return false
		}
		volume := source.Volume
		return volume.VolumeId == value.Destination.ResourceId && proto.Equal(volume.Volume, value.Destination.Resource) &&
			validBackupVolumeProjection(volume.Projection) && validBackupCheckpointVolumeArchive(volume.Archive, value.ExpectedEvidence)
	default:
		return false
	}
}

func validBackupRestoreIdentity(value *agentpb.BackupRestoreAuthority) bool {
	switch identity := value.Identity.(type) {
	case *agentpb.BackupRestoreAuthority_OriginalIdentity:
		return identity != nil && identity.OriginalIdentity != nil && validRawULID(identity.OriginalIdentity.OriginalExecutionId) &&
			ids.Validate(ids.KindAssignment, identity.OriginalIdentity.OriginalAssignmentId) == nil && proto.Equal(identity.OriginalIdentity.Destination, value.Destination)
	case *agentpb.BackupRestoreAuthority_AdoptedIdentity:
		if identity == nil || identity.AdoptedIdentity == nil {
			return false
		}
		adopted := identity.AdoptedIdentity
		return validRawULID(adopted.AdoptionId) && validRawULID(adopted.PredecessorExecutionId) && adopted.AdoptionModRevision > 0 &&
			adopted.PredecessorFence != nil && adopted.PredecessorFence.DedupeKeyModRevision > 0 && backupCheckpointDigest(adopted.PredecessorFence.AuthorityDigest) &&
			adopted.AdoptionModRevision > adopted.PredecessorFence.DedupeKeyModRevision &&
			ids.Validate(ids.KindAssignment, adopted.PredecessorAssignmentId) == nil && ids.Validate(ids.KindAssignment, adopted.AdoptedAssignmentId) == nil &&
			!bytes.Equal([]byte(adopted.PredecessorAssignmentId), []byte(adopted.AdoptedAssignmentId))
	default:
		return false
	}
}
