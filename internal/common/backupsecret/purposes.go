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

// SourcePurposes selects slots directly from captured Backup input, independent
// of the superseded generic execution-step envelope.
func SourcePurposes(step *agentpb.BackupStepAuthority) ([]agentpb.BackupSecretSlotPurpose, error) {
	if step == nil {
		return nil, errs.New(errs.KindValidationFailed, "backup secret source authority is required")
	}
	purposes := []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
	}
	var encryption *agentpb.BackupEncryptionAuthority
	if capture := step.GetCapture(); capture != nil {
		encryption = capture.Encryption
	}
	if restore := step.GetRestore(); restore != nil {
		encryption = restore.Encryption
	}
	if encryption == nil {
		if step.GetPrune() != nil {
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
			if step.GetRestore() == nil {
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
