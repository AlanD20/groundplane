package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

func advanceDatabaseConsumer(progress *agentpb.BackupDatabaseRestoreProgress,
	step *agentpb.BackupStepAuthority, services []*agentpb.BackupServiceFact,
	cursor uint32, serviceID string, phase agentpb.BackupServicePhase, observation []byte, recovering bool,
) error {
	const idle = agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED
	index := int(progress.StoppedConsumers)
	if recovering {
		index = len(step.ConsumerServiceIds) - 1 - int(progress.RecoveredConsumers)
	}
	if !backupCheckpointServicePhase(phase) || !backupCheckpointDigest(observation) ||
		index < 0 || index >= len(step.ConsumerServiceIds) || cursor != uint32(index) ||
		serviceID != step.ConsumerServiceIds[index] {
		return invalidBackupCheckpoint("database restore consumer report is out of order")
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range services {
		if candidate.ServiceId == serviceID {
			fact = candidate
			break
		}
	}
	if fact == nil || fact.PriorRuntimeIntent == nil {
		return invalidBackupCheckpoint("database restore consumer has no captured runtime intent")
	}
	finished := false
	if fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		if progress.ConsumerPhase != idle || phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING {
			return invalidBackupCheckpoint("database restore must preserve the stopped consumer")
		}
		finished = true
	} else {
		phases := []agentpb.BackupServicePhase{
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED,
		}
		if recovering {
			phases = []agentpb.BackupServicePhase{
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY,
			}
		}
		previous, accepted := idle, false
		for index, candidate := range phases {
			if progress.ConsumerPhase == previous && phase == candidate {
				accepted, finished = true, index == len(phases)-1
				break
			}
			previous = candidate
		}
		if !accepted {
			return invalidBackupCheckpoint("database restore consumer report skips a required transition")
		}
	}
	progress.ConsumerPhase = phase
	if finished {
		progress.ConsumerPhase = idle
		if recovering {
			progress.RecoveredConsumers++
		} else {
			progress.StoppedConsumers++
		}
	}
	return nil
}
