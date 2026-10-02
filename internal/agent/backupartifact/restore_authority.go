package backupartifact

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RestoreAuthority owns the selected object and evidence from the sealed
// Restore step. Neither download nor reconnect may select a newer object.
type RestoreAuthority struct {
	task    *agentpb.BackupTaskAuthority
	step    *agentpb.BackupStepAuthority
	restore *agentpb.BackupRestoreAuthority
	object  backupobject.Object
}

func NewRestoreAuthority(task *agentpb.BackupTaskAuthority, step *agentpb.BackupStepAuthority) (
	*RestoreAuthority, error,
) {
	sealedTask, sealedStep, err := bindStep(task, step)
	if err != nil {
		return nil, err
	}
	restore := sealedStep.GetRestore()
	if restore == nil || restore.Destination == nil || restore.Destination.GetResourceId() == "" {
		return nil, invalidRestore()
	}
	selected := restore.SourceObject
	parts := strings.Split(strings.TrimPrefix(selected.ObjectKey, selected.Connector.Prefix), "/")
	if len(parts) != 4 || parts[0] != sealedTask.EnvironmentId {
		return nil, invalidRestore()
	}
	artifact := backupobject.Artifact{
		Key: selected.ObjectKey, EnvironmentID: parts[0], SourceID: parts[1], RecoveryPointID: restore.PointId,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: restore.ExpectedEvidence.SourceSizeBytes,
			StoredSizeBytes: restore.ExpectedEvidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], restore.ExpectedEvidence.SourceSha256)
	copy(artifact.Evidence.StoredSHA256[:], restore.ExpectedEvidence.StoredSha256)
	switch {
	case restore.GetConfig() != nil:
		artifact.SourceFormat = backupobject.SourceFormatEnvironmentConfig
	case restore.GetVolume() != nil:
		artifact.SourceFormat = backupobject.SourceFormatVolumeTar
	case restore.GetPostgres() != nil:
		artifact.SourceFormat = backupobject.SourceFormatPostgresCustom
	default:
		return nil, invalidRestore()
	}
	switch restore.Encryption.Kind {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		artifact.Encryption = backupobject.EncryptionNone
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		era := restore.Encryption.GetKeyEra()
		artifact.Encryption, artifact.KeyEra = backupobject.EncryptionAge, &era
	default:
		return nil, invalidRestore()
	}
	if artifact.ExpectedKey(selected.Connector.Prefix) != artifact.Key {
		return nil, invalidRestore()
	}
	if err := artifact.Validate(); err != nil {
		return nil, err
	}
	discriminator, err := discriminatorFromWire(selected)
	if err != nil {
		return nil, err
	}
	return &RestoreAuthority{
		task: sealedTask, step: sealedStep, restore: restore,
		object: backupobject.Object{Artifact: artifact, Discriminator: discriminator},
	}, nil
}

// Sealed returns a copy for source-specific format validation and checkpoints.
func (authority *RestoreAuthority) Sealed() *agentpb.BackupRestoreAuthority {
	if authority == nil || authority.restore == nil {
		return nil
	}
	return proto.Clone(authority.restore).(*agentpb.BackupRestoreAuthority)
}

func invalidRestore() error {
	return errs.New(errs.KindStateConflict, "backup restore artifact differs from sealed object authority")
}
