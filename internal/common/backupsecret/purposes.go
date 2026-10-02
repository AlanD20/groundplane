package backupsecret

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	AccessKeySlotID              = "access-key"
	SecretKeySlotID              = "secret-key"
	CurrentAgeIdentitySlotID     = "current-age-identity"
	OperatorOldAgeIdentitySlotID = "operator-old-age-identity"
)

// Purposes is shared by slot delivery and worker reservation. The encryption
// authority selects which identity is required; the receiver never guesses an
// old-era restore identity from the Environment's current key.
func Purposes(step *agentpb.ExecutionStep) ([]agentpb.BackupSecretSlotPurpose, error) {
	if step == nil || step.GetBackupStep() == nil {
		return nil, errs.New(errs.KindValidationFailed, "backup secret step authority is required")
	}
	purposes := []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
	}
	var encryption *agentpb.BackupEncryptionAuthority
	if capture := step.GetBackupStep().GetCapture(); capture != nil {
		encryption = capture.Encryption
	}
	if restore := step.GetBackupStep().GetRestore(); restore != nil {
		encryption = restore.Encryption
	}
	if encryption == nil {
		if step.GetBackupStep().GetPrune() != nil {
			return purposes, nil
		}
		return nil, errs.New(errs.KindValidationFailed, "backup encryption authority is required")
	}
	switch encryption.Kind {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		return purposes, nil
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		switch encryption.SecretSlotId {
		case CurrentAgeIdentitySlotID:
			purposes = append(purposes, agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY)
		case OperatorOldAgeIdentitySlotID:
			if step.GetBackupStep().GetRestore() == nil {
				return nil, errs.New(errs.KindValidationFailed, "old age identity is restricted to Restore")
			}
			purposes = append(
				purposes,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
			)
		default:
			return nil, errs.New(errs.KindValidationFailed, "backup age identity slot is invalid")
		}
		return purposes, nil
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup encryption authority is invalid")
	}
}
