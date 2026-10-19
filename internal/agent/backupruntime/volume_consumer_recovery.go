package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) recoverVolumeServiceSet(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, publisher *checkpointdelivery.BackupSource,
) (*agentpb.BackupStepResume, error) {
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return nil, err
	}
	if cursor.GetRestore().GetPhase() < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return nil, invalidDatabaseStaging()
	}
	host, err := NewBackupConsumerHost(executor.journal, executor.helper, identity, executor.ownership)
	if err != nil {
		return nil, err
	}
	commands := volumeRecoveryCommands{
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(
				ctx,
				assignment,
				0,
				serviceID,
				agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED,
				true,
			)
		},
		recover: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(
				ctx,
				assignment,
				0,
				serviceID,
				agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER,
				false,
			)
		},
		resumeIntent: func(ctx context.Context, cursor uint32, serviceID string, digest [sha256.Size]byte) error {
			return publishVolumeServiceProgress(ctx, publisher, cursor, serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT, digest)
		},
	}
	if err := recoverVolumeConsumerSet(ctx, assignment.Input, 0, cursor.GetRestore(), publisher, commands); err != nil {
		return nil, err
	}
	return publisher.Current(ctx)
}
