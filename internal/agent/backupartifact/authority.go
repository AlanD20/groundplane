// Package backupartifact transfers exact sealed Backup objects. It does not
// publish Recovery Points, mutate restore targets or clean staging state.
package backupartifact

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Authority owns the captured resource and Environment used to derive the exact
// provider-neutral object expectation. Execution authentication belongs to its caller.
type Authority struct {
	environment string
	capture     *agentpb.BackupCaptureAuthority
}

// NewAuthority binds a capture to its Environment. The returned value
// owns copies, so callers cannot change upload authority after validation.
func NewAuthority(
	environment string,
	capture *agentpb.BackupCaptureAuthority,
) (*Authority, error) {
	if ids.Validate(ids.KindEnvironment, environment) != nil {
		return nil, invalidAuthority()
	}
	owned, err := executionplan.CaptureBackupArtifactAuthority(capture)
	if err != nil {
		return nil, err
	}
	return &Authority{
		environment: environment,
		capture:     owned,
	}, nil
}

func (authority *Authority) artifact(prepared *agentpb.BackupArtifactPrepared) (backupobject.Artifact, error) {
	if authority == nil || authority.environment == "" || authority.capture == nil ||
		prepared == nil {
		return backupobject.Artifact{}, invalidAuthority()
	}
	if err := executionplan.ValidateBackupPreparedArtifact(prepared); err != nil {
		return backupobject.Artifact{}, err
	}
	if prepared.PointId != authority.capture.PointId || !preparedSourceMatches(prepared, authority.capture) {
		return backupobject.Artifact{}, invalidAuthority()
	}
	target := authority.capture.Target
	connector := target.Connector
	parts := strings.Split(strings.TrimPrefix(target.ObjectKey, connector.Prefix), "/")
	if len(parts) != 4 || parts[0] != authority.environment {
		return backupobject.Artifact{}, invalidAuthority()
	}

	artifact := backupobject.Artifact{
		Key:             target.ObjectKey,
		EnvironmentID:   authority.environment,
		SourceID:        parts[1],
		RecoveryPointID: authority.capture.PointId,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: prepared.Evidence.SourceSizeBytes,
			StoredSizeBytes: prepared.Evidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], prepared.Evidence.SourceSha256)
	copy(artifact.Evidence.StoredSHA256[:], prepared.Evidence.StoredSha256)
	switch {
	case authority.capture.GetPostgres() != nil:
		archive := prepared.GetPostgres()
		if archive == nil {
			return backupobject.Artifact{}, invalidAuthority()
		}
		artifact.SourceFormat = backupobject.SourceFormatPostgresCustom
		artifact.SourceServerVersion = archive.SourceServerVersion
		artifact.BackupToolVersion = archive.BackupToolVersion
	case authority.capture.GetMysql() != nil:
		archive := prepared.GetMysql()
		if archive == nil {
			return backupobject.Artifact{}, invalidAuthority()
		}
		artifact.SourceFormat = backupobject.SourceFormatMySQLLogical
		artifact.SourceServerVersion = archive.SourceServerVersion
		artifact.BackupToolVersion = archive.BackupToolVersion
	case authority.capture.GetConfig() != nil:
		artifact.SourceFormat = backupobject.SourceFormatEnvironmentConfig
	case authority.capture.GetVolume() != nil:
		artifact.SourceFormat = backupobject.SourceFormatVolumeTar
	default:
		return backupobject.Artifact{}, invalidAuthority()
	}
	switch authority.capture.Encryption.Kind {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if authority.capture.Encryption.KeyEra != nil {
			return backupobject.Artifact{}, invalidAuthority()
		}
		artifact.Encryption = backupobject.EncryptionNone
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		if authority.capture.Encryption.KeyEra == nil || authority.capture.Encryption.GetKeyEra() == 0 {
			return backupobject.Artifact{}, invalidAuthority()
		}
		era := authority.capture.Encryption.GetKeyEra()
		artifact.Encryption, artifact.KeyEra = backupobject.EncryptionAge, &era
	default:
		return backupobject.Artifact{}, invalidAuthority()
	}
	if artifact.ExpectedKey(connector.Prefix) != artifact.Key {
		return backupobject.Artifact{}, invalidAuthority()
	}
	if err := artifact.Validate(); err != nil {
		return backupobject.Artifact{}, err
	}
	return artifact, nil
}

func preparedSourceMatches(prepared *agentpb.BackupArtifactPrepared, capture *agentpb.BackupCaptureAuthority) bool {
	return prepared.GetPostgres() != nil && capture.GetPostgres() != nil ||
		prepared.GetMysql() != nil && capture.GetMysql() != nil ||
		prepared.GetConfig() != nil && capture.GetConfig() != nil ||
		prepared.GetVolume() != nil && capture.GetVolume() != nil
}

func invalidAuthority() error {
	return errs.New(errs.KindStateConflict, "backup artifact upload differs from sealed capture authority")
}
