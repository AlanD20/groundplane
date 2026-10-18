package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	MaximumBackupSecretCredentialBytes = uint64(256 << 10)
	MaximumBackupSecretIdentityBytes   = uint64(4 << 10)
	MaximumBackupSecretChunkBytes      = uint64(entrymaterialization.MaximumChunkBytes)
)

func backupSecretSlotMaximum(purpose agentpb.BackupSecretSlotPurpose) uint64 {
	switch purpose {
	case agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY:
		return MaximumBackupSecretCredentialBytes
	case agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY:
		return MaximumBackupSecretIdentityBytes
	default:
		return 0
	}
}

func backupSecretSlotChunkCount(total uint64) uint64 {
	return ((total - 1) / MaximumBackupSecretChunkBytes) + 1
}
