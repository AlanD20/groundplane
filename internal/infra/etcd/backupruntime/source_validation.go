package backupruntime

import (
	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/oklog/ulid/v2"
	"path"
	"strings"
	"time"
	"unicode/utf8"
)

func validateBackupPostgresSnapshot(snapshot BackupPostgresSourceSnapshot) error {
	if recordcodec.ValidateID(ids.KindEnvironment, snapshot.ConsumerEnvironmentID) != nil ||
		recordcodec.ValidateID(
			ids.KindAttach,
			snapshot.AttachID,
		) != nil || snapshot.AttachRevision <= 0 ||
		recordcodec.ValidateID(ids.KindProject, snapshot.BackingProjectID) != nil ||
		snapshot.BackingProjectRevision <= 0 ||
		recordcodec.ValidateID(ids.KindEnvironment, snapshot.BackingEnvironmentID) != nil ||
		snapshot.BackingEnvironmentRevision <= 0 ||
		recordcodec.ValidateID(ids.KindService, snapshot.BackingServiceID) != nil ||
		snapshot.BackingServiceRevision <= 0 || snapshot.AttachFactsRevision <= 0 ||
		!validBackupPostgresIdentity(snapshot.Database) || !validBackupPostgresIdentity(snapshot.Role) {
		return invalidBackupRuntimeRecord("postgres source snapshot is invalid")
	}
	return nil
}

func validBackupPostgresIdentity(value string) bool {
	if len(value) == 0 || len(value) > 63 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validateBackupVolumeSnapshot(snapshot BackupVolumeSourceSnapshot) error {
	if recordcodec.ValidateID(ids.KindEnvironment, snapshot.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindVolume, snapshot.VolumeID) != nil ||
		recordcodec.ValidateID(ids.KindTask, snapshot.DesiredRevisionID) != nil ||
		snapshot.EnvironmentRevision <= 0 || snapshot.ProjectionRoot <= 0 ||
		!recordcodec.ValidSHA256(snapshot.DependencyDigest) || snapshot.RenderGeneration == 0 ||
		snapshot.ComposeVolumeKey == "" || snapshot.DockerVolumeName != "gp_vol_"+snapshot.VolumeID ||
		snapshot.AuthorizedVolumeDir == "" {
		return invalidBackupRuntimeRecord("volume source snapshot is invalid")
	}
	hasArtifactAuthority := snapshot.ArtifactID != "" || snapshot.ArtifactDigest != "" || snapshot.ArtifactRevision != 0
	if hasArtifactAuthority && (recordcodec.ValidateID(ids.KindConfig, snapshot.ArtifactID) != nil ||
		!recordcodec.ValidSHA256(snapshot.ArtifactDigest) || snapshot.ArtifactRevision <= 0) {
		return invalidBackupRuntimeRecord("volume artifact authority is incomplete")
	}
	previousID := ""
	for _, service := range snapshot.Services {
		if recordcodec.ValidateID(ids.KindService, service.ServiceID) != nil ||
			service.ServiceRevision < 0 ||
			!validBackupServiceRuntimeIntent(service.PriorIntent) ||
			(previousID != "" && service.ServiceID <= previousID) {
			return invalidBackupRuntimeRecord("volume service snapshots are invalid")
		}
		if service.ComposeKey == "" || len(service.MountPaths) == 0 {
			return invalidBackupRuntimeRecord("volume service projection is incomplete")
		}
		previousID = service.ServiceID
	}
	return nil
}

func validateBackupSourceIdentity(
	kind BackupRuntimeSourceKind,
	targetID string,
	format BackupRuntimeFormat,
) error {
	switch kind {
	case BackupRuntimeSourceAttach:
		if recordcodec.ValidateID(ids.KindAttach, targetID) != nil ||
			format != BackupRuntimeFormatPostgres {
			return invalidBackupRuntimeRecord("postgres backup source identity is invalid")
		}
	case BackupRuntimeSourceVolume:
		if recordcodec.ValidateID(ids.KindVolume, targetID) != nil ||
			format != BackupRuntimeFormatVolume {
			return invalidBackupRuntimeRecord("volume backup source identity is invalid")
		}
	case BackupRuntimeSourceConfig:
		if recordcodec.ValidateID(ids.KindEnvironment, targetID) != nil ||
			format != BackupRuntimeFormatConfig {
			return invalidBackupRuntimeRecord("config backup source identity is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup source kind is invalid")
	}
	return nil
}

func validateBackupRuntimeEncryption(
	encryption BackupRuntimeEncryption,
	keyRecordRevision int64,
	keyValueRevision int64,
	keyEra int,
	recipient string,
) error {
	switch encryption {
	case BackupRuntimeEncryptionAge:
		if keyRecordRevision <= 0 || keyValueRevision <= 0 || keyEra <= 0 ||
			!validBackupRecipient(recipient) {
			return invalidBackupRuntimeRecord("age backup encryption evidence is invalid")
		}
	case BackupRuntimeEncryptionNone:
		if keyRecordRevision != 0 || keyValueRevision != 0 || keyEra != 0 || recipient != "" {
			return invalidBackupRuntimeRecord("unencrypted backup cannot carry age key evidence")
		}
	default:
		return invalidBackupRuntimeRecord("backup encryption is invalid")
	}
	return nil
}

func validBackupRecipient(value string) bool {
	recipient, err := age.ParseX25519Recipient(value)
	return err == nil && recipient.String() == value
}

func validBackupObjectKey(
	value string,
	environmentID string,
	sourceID string,
	connectorPrefix string,
	recoveryPointID string,
) bool {
	if value == "" || len(value) > maximumBackupObjectKeyBytes || !utf8.ValidString(value) ||
		strings.ContainsRune(
			value,
			'\x00',
		) || strings.Contains(value, `\`) || strings.HasPrefix(value, "/") ||
		path.Clean(value) != value {
		return false
	}
	if connectorPrefix != "" && (!utf8.ValidString(connectorPrefix) ||
		strings.HasPrefix(connectorPrefix, "/") || !strings.HasSuffix(connectorPrefix, "/") ||
		strings.ContainsRune(connectorPrefix, '\x00') || strings.Contains(connectorPrefix, `\`) ||
		path.Clean(strings.TrimSuffix(connectorPrefix, "/")) != strings.TrimSuffix(connectorPrefix, "/")) {
		return false
	}
	suffix := environmentID + "/" + sourceID + "/" + recoveryPointID + "/artifact.bin"
	return value == connectorPrefix+suffix
}

func recoveryPointIDMatchesInstant(recoveryPointID string, instant time.Time) bool {
	if recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID) != nil ||
		!ValidBackupRuntimeInstant(instant) || instant.Nanosecond()%int(time.Millisecond) != 0 {
		return false
	}
	parsed, err := ulid.ParseStrict(strings.TrimPrefix(recoveryPointID, string(ids.KindRecoveryPoint)+"_"))
	if err != nil {
		return false
	}
	return time.UnixMilli(int64(parsed.Time())).UTC().Equal(instant)
}
