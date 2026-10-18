package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// finishVolumeConsumerStop resumes only the captured stop sequence. The caller
// must publish the stopped-tree manifest before authorizing filesystem changes.
func (executor *Executor) finishVolumeConsumerStop(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, publisher *checkpointdelivery.BackupSource,
) (uint32, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return 0, zero, err
	}
	if cursor.GetRestore().GetPhase() != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION {
		return 0, zero, invalidDatabaseStaging()
	}
	host, err := NewBackupConsumerHost(executor.journal, executor.helper, identity, executor.ownership)
	if err != nil {
		return 0, zero, err
	}
	return stopVolumeConsumerSet(ctx, assignment.Input, 0, cursor.GetRestore(), publisher, volumeStopCommands{
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(ctx, assignment, 0, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED, true)
		},
		stop: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(ctx, assignment, 0, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP, false)
		},
		resumeIntent: func(ctx context.Context, ordinal uint32, serviceID string, digest [sha256.Size]byte) error {
			return publishVolumeServiceProgress(ctx, publisher, ordinal, serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT, digest)
		},
	})
}
