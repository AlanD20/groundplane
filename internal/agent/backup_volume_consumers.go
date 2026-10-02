package agent

import (
	"context"
	"crypto/sha256"
	"hash"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (pool *WorkerPool) stopBackupVolumeConsumers(ctx context.Context,
	assignment taskassignment.Assignment, execution *agentpb.ExecutionStep,
	resume *agentpb.BackupRestoreResume, publisher *backupStepCheckpoint,
) (uint32, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if pool.compose == nil || execution.GetBackupStep() == nil || publisher == nil {
		return 0, zero, errs.New(errs.KindInternal, "Backup Volume consumer runtime is unavailable")
	}
	hasher := sha256.New()
	stopped := uint32(0)
	for index, serviceID := range execution.GetBackupStep().ConsumerServiceIds {
		fact := volumeServiceFact(assignment, serviceID)
		if fact == nil {
			return 0, zero, invalidAgentStaging()
		}
		observed, digest, err := pool.compose.ObserveBackupVolumeConsumer(ctx, assignment, execution, serviceID)
		if err != nil {
			return 0, zero, err
		}
		if fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			stopped++
			if resume.GetVolumeProgress().GetServiceCursor() < uint32(index+1) {
				pendingStop := resume.GetVolumeProgress().GetServicePhase() ==
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
					resume.GetVolumeProgress().GetServiceCursor() == uint32(index)
				if !pendingStop {
					if !volumeAllRunning(observed, serviceID) {
						return 0, zero, invalidAgentStaging()
					}
					if err := publishVolumeServiceProgress(ctx, publisher, uint32(index), serviceID,
						agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT, digest); err != nil {
						return 0, zero, err
					}
				}
				if volumeObservedRunning(observed, serviceID) {
					_, digest, err = pool.compose.ExecuteBackupVolumeConsumer(ctx, assignment, execution, serviceID,
						agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_STOP)
					if err != nil {
						return 0, zero, err
					}
				}
				if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED, digest); err != nil {
					return 0, zero, err
				}
			} else if volumeObservedRunning(observed, serviceID) {
				return 0, zero, invalidAgentStaging()
			}
		} else {
			if volumeObservedRunning(observed, serviceID) {
				return 0, zero, invalidAgentStaging()
			}
			if resume.GetVolumeProgress().GetServiceCursor() < uint32(index+1) {
				if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING, digest); err != nil {
					return 0, zero, err
				}
			}
		}
		writeVolumeServiceDigest(hasher, digest)
	}
	copy(zero[:], hasher.Sum(nil))
	return stopped, zero, nil
}

func (pool *WorkerPool) recoverBackupVolumeConsumers(ctx context.Context,
	assignment taskassignment.Assignment, execution *agentpb.ExecutionStep,
	resume *agentpb.BackupRestoreResume, publisher *backupStepCheckpoint,
) error {
	if pool.compose == nil || execution.GetBackupStep() == nil || publisher == nil {
		return errs.New(errs.KindInternal, "Backup Volume consumer runtime is unavailable")
	}
	for index, serviceID := range execution.GetBackupStep().ConsumerServiceIds {
		fact := volumeServiceFact(assignment, serviceID)
		if fact == nil {
			return invalidAgentStaging()
		}
		observed, digest, err := pool.compose.ObserveBackupVolumeConsumer(ctx, assignment, execution, serviceID)
		if err != nil {
			return err
		}
		if resume.GetVolumeProgress().GetServiceCursor() >= uint32(index+1) {
			if fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
				!volumeAllRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			continue
		}
		if fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			pendingRestart := resume.GetVolumeProgress().GetServicePhase() ==
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
				resume.GetVolumeProgress().GetServiceCursor() == uint32(index)
			if !pendingRestart {
				if volumeObservedRunning(observed, serviceID) {
					return invalidAgentStaging()
				}
				if err := publishVolumeServiceProgress(ctx, publisher, uint32(index), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT, digest); err != nil {
					return err
				}
			}
			if !volumeAllRunning(observed, serviceID) {
				_, digest, err = pool.compose.ExecuteBackupVolumeConsumer(ctx, assignment, execution, serviceID,
					agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_RECOVER)
				if err != nil {
					return err
				}
			}
			if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY, digest); err != nil {
				return err
			}
		} else {
			if volumeObservedRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING, digest); err != nil {
				return err
			}
		}
	}
	return nil
}

func publishVolumeServiceProgress(ctx context.Context, publisher *backupStepCheckpoint,
	cursor uint32, serviceID string, phase agentpb.BackupServicePhase, digest [sha256.Size]byte,
) error {
	return publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Volume{
		Volume: &agentpb.BackupVolumeCheckpoint{Checkpoint: &agentpb.BackupVolumeCheckpoint_ServiceProgress{
			ServiceProgress: &agentpb.BackupVolumeServiceProgress{ServiceCursor: cursor,
				ServiceId: serviceID, Phase: phase, ObservationSha256: append([]byte(nil), digest[:]...)}}}}})
}

func writeVolumeServiceDigest(hasher hash.Hash, digest [sha256.Size]byte) {
	_, _ = hasher.Write(digest[:])
}

func volumeServiceFact(assignment taskassignment.Assignment, serviceID string) *agentpb.BackupServiceFact {
	for _, fact := range assignment.BackupAuthority.GetServices() {
		if fact.ServiceId == serviceID {
			return fact
		}
	}
	return nil
}

func volumeObservedRunning(observed *agentpb.ObservedProject, serviceID string) bool {
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() == serviceID &&
			container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return true
		}
	}
	return false
}

func volumeAllRunning(observed *agentpb.ObservedProject, serviceID string) bool {
	count := 0
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() != serviceID {
			continue
		}
		count++
		if container.GetState() != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return false
		}
	}
	return count != 0
}
