package backupconfigtransfer

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CaptureEntry converts validated schema-one capture metadata to the archive
// model. Revisions and source stable IDs are validated before the archive model
// intentionally omits them.
func CaptureEntry(value *agentpb.BackupConfigEntry) (backupconfig.Entry, error) {
	if value == nil || rejectUnknown(value) != nil || value.Secret == nil ||
		ids.Validate(ids.KindEnvEntry, value.EntryId) != nil || !validRevision(value.Entry) ||
		value.Ordinal == 0 || value.Ordinal > backupconfig.MaxEntries ||
		value.SelectedValueSizeBytes > backupconfig.MaxSelectedValueBytes ||
		len(value.SelectedValueSha256) != sha256.Size || proto.Size(value) > backupconfig.MaxCanonicalEntryBytes {
		return backupconfig.Entry{}, invalid("capture Config Entry metadata is invalid")
	}
	entry := backupconfig.Entry{ID: value.EntryId, Secret: value.GetSecret()}
	entry.Value.Path = "values/" + value.EntryId
	entry.Value.SizeBytes = value.SelectedValueSizeBytes
	copy(entry.Value.SHA256[:], value.SelectedValueSha256)
	if err := captureMetadata(value, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := captureExposure(value, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := captureSource(value, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := backupconfig.ValidateEntry(entry); err != nil {
		return backupconfig.Entry{}, err
	}
	return entry, nil
}

// RestoreEntry converts validated schema-one restore metadata to the archive
// model used to verify and materialize the selected artifact.
func RestoreEntry(value *agentpb.BackupConfigRestoreEntry) (backupconfig.Entry, error) {
	if value == nil || rejectUnknown(value) != nil || value.Secret == nil || value.SelectedValue == nil ||
		ids.Validate(ids.KindEnvEntry, value.EntryId) != nil ||
		value.SelectedValue.SizeBytes > backupconfig.MaxSelectedValueBytes ||
		len(value.SelectedValue.Sha256) != sha256.Size || proto.Size(value) > backupconfig.MaxCanonicalEntryBytes {
		return backupconfig.Entry{}, invalid("restore Config Entry metadata is invalid")
	}
	entry := backupconfig.Entry{ID: value.EntryId, Secret: value.GetSecret()}
	entry.Value.Path = "values/" + value.EntryId
	entry.Value.SizeBytes = value.SelectedValue.SizeBytes
	copy(entry.Value.SHA256[:], value.SelectedValue.Sha256)
	if err := restoreMetadata(value.Metadata, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := restoreExposure(value.Exposure, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := restoreSource(value.Source, &entry); err != nil {
		return backupconfig.Entry{}, err
	}
	if err := backupconfig.ValidateEntry(entry); err != nil {
		return backupconfig.Entry{}, err
	}
	return entry, nil
}

func entryFromHeader(
	direction agentpb.BackupConfigDirection,
	header *agentpb.BackupConfigEntryHeader,
) (backupconfig.Entry, error) {
	switch direction {
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
		if header.GetCaptureEntry() == nil || header.GetRestoreEntry() != nil {
			return backupconfig.Entry{}, invalid("capture transfer carries the wrong Config Entry variant")
		}
		return CaptureEntry(header.GetCaptureEntry())
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
		if header.GetRestoreEntry() == nil || header.GetCaptureEntry() != nil {
			return backupconfig.Entry{}, invalid("restore transfer carries the wrong Config Entry variant")
		}
		return RestoreEntry(header.GetRestoreEntry())
	default:
		return backupconfig.Entry{}, invalid("config transfer direction is invalid")
	}
}

func captureMetadata(value *agentpb.BackupConfigEntry, entry *backupconfig.Entry) error {
	switch metadata := value.Metadata.(type) {
	case *agentpb.BackupConfigEntry_Environment:
		if metadata == nil || metadata.Environment == nil {
			return invalid("capture Config environment metadata is invalid")
		}
		entry.Metadata.Kind = backupconfig.MetadataEnvironment
		entry.Metadata.Environment.Key = metadata.Environment.Name
	case *agentpb.BackupConfigEntry_File:
		if metadata == nil || metadata.File == nil {
			return invalid("capture Config file metadata is invalid")
		}
		entry.Metadata.Kind = backupconfig.MetadataFile
		entry.Metadata.File = backupconfig.FileMetadata{
			Path: metadata.File.Path, Mode: metadata.File.Mode, UID: metadata.File.Uid, GID: metadata.File.Gid,
		}
	default:
		return invalid("capture Config metadata kind is invalid")
	}
	return nil
}

func captureExposure(value *agentpb.BackupConfigEntry, entry *backupconfig.Entry) error {
	switch exposure := value.Exposure.(type) {
	case *agentpb.BackupConfigEntry_ExposureAll:
		if exposure == nil || exposure.ExposureAll == nil {
			return invalid("capture Config all-services exposure is invalid")
		}
		entry.Exposure.Kind = backupconfig.ExposureAll
	case *agentpb.BackupConfigEntry_ExposureServices:
		if exposure == nil || exposure.ExposureServices == nil {
			return invalid("capture Config services exposure is invalid")
		}
		entry.Exposure.Kind = backupconfig.ExposureServices
		entry.Exposure.ServiceIDs = append([]string(nil), exposure.ExposureServices.ServiceIds...)
	default:
		return invalid("capture Config exposure kind is invalid")
	}
	return nil
}

func captureSource(value *agentpb.BackupConfigEntry, entry *backupconfig.Entry) error {
	switch source := value.Source.(type) {
	case *agentpb.BackupConfigEntry_Literal:
		if source == nil || source.Literal == nil {
			return invalid("capture Config literal source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceLiteral
	case *agentpb.BackupConfigEntry_SecretRef:
		if source == nil || source.SecretRef == nil || ids.Validate(ids.KindSecret, source.SecretRef.SecretId) != nil ||
			!validRevision(source.SecretRef.Secret) {
			return invalid("capture Config secret-reference source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceSecretReference
		entry.Source.SecretReference.AuthoredKey = source.SecretRef.AuthoredKey
	case *agentpb.BackupConfigEntry_FactRef:
		if source == nil || source.FactRef == nil || ids.Validate(ids.KindAttach, source.FactRef.AttachId) != nil ||
			!validRevision(source.FactRef.Attach) {
			return invalid("capture Config fact source is invalid")
		}
		fact := source.FactRef
		if fact.GrantAttachId == nil {
			if fact.GrantAttach != nil {
				return invalid("capture Config fact grant revision lacks its stable ID")
			}
		} else if ids.Validate(ids.KindAttach, fact.GetGrantAttachId()) != nil || !validRevision(fact.GrantAttach) {
			return invalid("capture Config fact grant source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceFact
		entry.Source.Fact = backupconfig.FactReference{
			AttachID: fact.AttachId, Fact: fact.Fact, GrantAttachID: fact.GetGrantAttachId(),
		}
	default:
		return invalid("capture Config source kind is invalid")
	}
	return nil
}

func restoreMetadata(value *agentpb.BackupConfigEntryMetadata, entry *backupconfig.Entry) error {
	if value == nil {
		return invalid("restore Config Entry metadata is required")
	}
	switch metadata := value.Metadata.(type) {
	case *agentpb.BackupConfigEntryMetadata_Environment:
		if metadata == nil || metadata.Environment == nil {
			return invalid("restore Config environment metadata is invalid")
		}
		entry.Metadata.Kind = backupconfig.MetadataEnvironment
		entry.Metadata.Environment.Key = metadata.Environment.Name
	case *agentpb.BackupConfigEntryMetadata_File:
		if metadata == nil || metadata.File == nil {
			return invalid("restore Config file metadata is invalid")
		}
		entry.Metadata.Kind = backupconfig.MetadataFile
		entry.Metadata.File = backupconfig.FileMetadata{
			Path: metadata.File.Path, Mode: metadata.File.Mode, UID: metadata.File.Uid, GID: metadata.File.Gid,
		}
	default:
		return invalid("restore Config metadata kind is invalid")
	}
	return nil
}

func restoreExposure(value *agentpb.BackupConfigEntryExposure, entry *backupconfig.Entry) error {
	if value == nil {
		return invalid("restore Config Entry exposure is required")
	}
	switch exposure := value.Exposure.(type) {
	case *agentpb.BackupConfigEntryExposure_All:
		if exposure == nil || exposure.All == nil {
			return invalid("restore Config all-services exposure is invalid")
		}
		entry.Exposure.Kind = backupconfig.ExposureAll
	case *agentpb.BackupConfigEntryExposure_Services:
		if exposure == nil || exposure.Services == nil {
			return invalid("restore Config services exposure is invalid")
		}
		entry.Exposure.Kind = backupconfig.ExposureServices
		entry.Exposure.ServiceIDs = append([]string(nil), exposure.Services.ServiceIds...)
	default:
		return invalid("restore Config exposure kind is invalid")
	}
	return nil
}

func restoreSource(value *agentpb.BackupConfigArtifactDesiredSource, entry *backupconfig.Entry) error {
	if value == nil {
		return invalid("restore Config Entry source is required")
	}
	switch source := value.Source.(type) {
	case *agentpb.BackupConfigArtifactDesiredSource_Literal:
		if source == nil || source.Literal == nil {
			return invalid("restore Config literal source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceLiteral
	case *agentpb.BackupConfigArtifactDesiredSource_SecretRef:
		if source == nil || source.SecretRef == nil {
			return invalid("restore Config secret-reference source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceSecretReference
		entry.Source.SecretReference.AuthoredKey = source.SecretRef.AuthoredKey
	case *agentpb.BackupConfigArtifactDesiredSource_FactRef:
		if source == nil || source.FactRef == nil || ids.Validate(ids.KindAttach, source.FactRef.AttachId) != nil ||
			(source.FactRef.GrantAttachId != nil && ids.Validate(ids.KindAttach, source.FactRef.GetGrantAttachId()) != nil) {
			return invalid("restore Config fact source is invalid")
		}
		entry.Source.Kind = backupconfig.SourceFact
		entry.Source.Fact = backupconfig.FactReference{
			AttachID: source.FactRef.AttachId, Fact: source.FactRef.Fact,
			GrantAttachID: source.FactRef.GetGrantAttachId(),
		}
	default:
		return invalid("restore Config source kind is invalid")
	}
	return nil
}

func validRevision(value *agentpb.RevisionDigest) bool {
	return value != nil && value.ModRevision > 0 && len(value.Sha256) == sha256.Size
}
