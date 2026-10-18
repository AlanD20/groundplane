package backupruntime

import (
	"context"
	"crypto/sha256"
	"hash"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupConsumers operates only on workload facts captured in the native input.
// The implementation authenticates the attempt and source before host access.
type BackupConsumers interface {
	Observe(context.Context, *agentpb.BackupWorkAssignment, uint32, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	Execute(context.Context, *agentpb.BackupWorkAssignment, uint32, string, agentpb.BackupConsumerOperation) (*agentpb.ObservedProject, [sha256.Size]byte, error)
}

func stopVolumeConsumers(ctx context.Context, consumers BackupConsumers,
	assignment *agentpb.BackupWorkAssignment, ordinal uint32,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource,
) (uint32, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if consumers == nil || assignment == nil || assignment.Input == nil || uint64(ordinal) >= uint64(len(assignment.Input.Steps)) || publisher == nil {
		return 0, zero, errs.New(errs.KindInternal, "Backup Volume consumer runtime is unavailable")
	}
	return stopVolumeConsumerSet(ctx, assignment.Input, ordinal, resume, publisher, volumeStopCommands{
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Observe(ctx, assignment, ordinal, serviceID)
		},
		stop: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Execute(ctx, assignment, ordinal, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP)
		},
	})
}

type volumeStopCommands struct {
	observe      func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	stop         func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	resumeIntent func(context.Context, uint32, string, [sha256.Size]byte) error
}

func stopVolumeConsumerSet(ctx context.Context, input *agentpb.BackupWorkInput, ordinal uint32,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource, commands volumeStopCommands,
) (uint32, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if input == nil || uint64(ordinal) >= uint64(len(input.Steps)) || publisher == nil || commands.observe == nil || commands.stop == nil {
		return 0, zero, errs.New(errs.KindInternal, "Volume stop commands are unavailable")
	}
	hasher := sha256.New()
	stopped := uint32(0)
	for index, serviceID := range input.Steps[ordinal].ConsumerServiceIds {
		fact := backupServiceFact(input, serviceID)
		if fact == nil || fact.PriorRuntimeIntent == nil {
			return 0, zero, errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
		}
		observed, digest, err := commands.observe(ctx, serviceID)
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
						return 0, zero, errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
					}
					if err := publishVolumeServiceProgress(ctx, publisher, uint32(index), serviceID,
						agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT, digest); err != nil {
						return 0, zero, err
					}
				}
				if volumeObservedRunning(observed, serviceID) {
					if pendingStop && commands.resumeIntent != nil {
						if err := commands.resumeIntent(ctx, uint32(index), serviceID, digest); err != nil {
							return 0, zero, err
						}
					}
					_, digest, err = commands.stop(ctx, serviceID)
					if err != nil {
						return 0, zero, err
					}
				}
				if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED, digest); err != nil {
					return 0, zero, err
				}
			} else if volumeObservedRunning(observed, serviceID) {
				return 0, zero, errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
			}
		} else {
			if volumeObservedRunning(observed, serviceID) {
				return 0, zero, errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
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

func recoverVolumeConsumers(ctx context.Context, consumers BackupConsumers,
	assignment *agentpb.BackupWorkAssignment, ordinal uint32,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource,
) error {
	if consumers == nil || assignment == nil || assignment.Input == nil || uint64(ordinal) >= uint64(len(assignment.Input.Steps)) || publisher == nil {
		return errs.New(errs.KindInternal, "Backup Volume consumer runtime is unavailable")
	}
	return recoverVolumeConsumerSet(ctx, assignment.Input, ordinal, resume, publisher, volumeRecoveryCommands{
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Observe(ctx, assignment, ordinal, serviceID)
		},
		recover: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Execute(ctx, assignment, ordinal, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER)
		},
	})
}

type volumeRecoveryCommands struct {
	observe      func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	recover      func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	resumeIntent func(context.Context, uint32, string, [sha256.Size]byte) error
}

func recoverVolumeConsumerSet(ctx context.Context, input *agentpb.BackupWorkInput, ordinal uint32,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource, commands volumeRecoveryCommands,
) error {
	if input == nil || uint64(ordinal) >= uint64(len(input.Steps)) || publisher == nil || commands.observe == nil || commands.recover == nil {
		return errs.New(errs.KindInternal, "Volume recovery consumer commands are unavailable")
	}
	for index, serviceID := range input.Steps[ordinal].ConsumerServiceIds {
		fact := backupServiceFact(input, serviceID)
		if fact == nil || fact.PriorRuntimeIntent == nil {
			return errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
		}
		observed, digest, err := commands.observe(ctx, serviceID)
		if err != nil {
			return err
		}
		if resume.GetVolumeProgress().GetServiceCursor() >= uint32(index+1) {
			if fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
				!volumeAllRunning(observed, serviceID) ||
				fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
					volumeObservedRunning(observed, serviceID) {
				return errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
			}
			continue
		}
		if fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			pendingRestart := resume.GetVolumeProgress().GetServicePhase() ==
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
				resume.GetVolumeProgress().GetServiceCursor() == uint32(index)
			if !pendingRestart {
				if volumeObservedRunning(observed, serviceID) {
					return errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
				}
				if err := publishVolumeServiceProgress(ctx, publisher, uint32(index), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT, digest); err != nil {
					return err
				}
			}
			if !volumeAllRunning(observed, serviceID) {
				if pendingRestart && commands.resumeIntent != nil {
					if err := commands.resumeIntent(ctx, uint32(index), serviceID, digest); err != nil {
						return err
					}
				}
				_, digest, err = commands.recover(ctx, serviceID)
				if err != nil {
					return err
				}
			}
			_, digest, err = waitDatabaseConsumerObservation(ctx, input, ordinal, serviceID, commands.observe)
			if err != nil {
				return err
			}
			if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY, digest); err != nil {
				return err
			}
		} else {
			if volumeObservedRunning(observed, serviceID) {
				return errs.New(errs.KindStateConflict, "Volume consumer state differs from captured execution")
			}
			if err := publishVolumeServiceProgress(ctx, publisher, uint32(index+1), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING, digest); err != nil {
				return err
			}
		}
	}
	return nil
}

func publishVolumeServiceProgress(ctx context.Context, publisher *checkpointdelivery.BackupSource,
	cursor uint32, serviceID string, phase agentpb.BackupServicePhase, digest [sha256.Size]byte,
) error {
	return publisher.Volume(ctx, &agentpb.BackupVolumeCheckpoint{Checkpoint: &agentpb.BackupVolumeCheckpoint_ServiceProgress{
		ServiceProgress: &agentpb.BackupVolumeServiceProgress{ServiceCursor: cursor,
			ServiceId: serviceID, Phase: phase, ObservationSha256: append([]byte(nil), digest[:]...)}}})
}

func writeVolumeServiceDigest(hasher hash.Hash, digest [sha256.Size]byte) {
	_, _ = hasher.Write(digest[:])
}

func volumeServiceFact(assignment *agentpb.BackupWorkAssignment, serviceID string) *agentpb.BackupServiceFact {
	return backupServiceFact(assignment.GetInput(), serviceID)
}

func backupServiceFact(input *agentpb.BackupWorkInput, serviceID string) *agentpb.BackupServiceFact {
	for _, fact := range input.GetServices() {
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
