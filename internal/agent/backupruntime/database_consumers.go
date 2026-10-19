package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func databaseStopConsumers(ctx context.Context, consumers BackupConsumers, assignment *agentpb.BackupWorkAssignment,
	sourceOrdinal uint32, resume *agentpb.BackupRestoreResume,
	publisher *checkpointdelivery.BackupSource, attempted *bool,
) error {
	if consumers == nil || publisher == nil || attempted == nil {
		return errs.New(errs.KindInternal, "database consumer runtime is unavailable")
	}
	for ordinal, serviceID := range assignment.Input.Steps[sourceOrdinal].ConsumerServiceIds {
		_, _, err := executionplan.BackupConsumer(assignment.Input.EnvironmentId, assignment.Input.Services,
			assignment.Input.Artifacts, assignment.Input.Steps[sourceOrdinal], serviceID)
		fact := volumeServiceFact(assignment, serviceID)
		if fact == nil || fact.PriorRuntimeIntent == nil {
			return invalidDatabaseStaging()
		}
		intent := fact.PriorRuntimeIntent.Kind
		if err != nil {
			return err
		}
		observed, digest, err := consumers.Observe(ctx, assignment, sourceOrdinal, serviceID)
		if err != nil {
			return err
		}
		progress := databaseServiceProgress(resume)
		if progress != nil && resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
			(uint32(ordinal) < progress.ServiceCursor || uint32(ordinal) == progress.ServiceCursor &&
				(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED ||
					progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)) {
			if volumeObservedRunning(observed, serviceID) {
				return invalidDatabaseStaging()
			}
			continue
		}
		if intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			if volumeObservedRunning(observed, serviceID) {
				return invalidDatabaseStaging()
			}
			if err := publishDatabaseService(ctx, assignment.Input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING, digest); err != nil {
				return err
			}
			continue
		}
		pending := progress != nil &&
			resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
			progress.ServiceCursor == uint32(ordinal) &&
			progress.ServiceId == serviceID &&
			progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT
		if !pending {
			if !volumeAllRunning(observed, serviceID) {
				return invalidDatabaseStaging()
			}
			if err := publishDatabaseService(ctx, assignment.Input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT, digest); err != nil {
				return err
			}
		}
		if volumeObservedRunning(observed, serviceID) {
			*attempted = true
			_, digest, err = consumers.Execute(
				ctx,
				assignment,
				sourceOrdinal,
				serviceID,
				agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP,
			)
			if err != nil {
				return err
			}
		}
		if err := publishDatabaseService(ctx, assignment.Input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED, digest); err != nil {
			return err
		}
	}
	return nil
}

func databaseRecoverConsumers(ctx context.Context, consumers BackupConsumers, assignment *agentpb.BackupWorkAssignment,
	sourceOrdinal uint32, resume *agentpb.BackupRestoreResume,
	publisher *checkpointdelivery.BackupSource,
) error {
	if consumers == nil || assignment == nil {
		return invalidDatabaseStaging()
	}
	return recoverDatabaseConsumerSet(ctx, assignment.Input, sourceOrdinal, resume, publisher, databaseRecoveryCommands{
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Observe(ctx, assignment, sourceOrdinal, serviceID)
		},
		recover: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return consumers.Execute(
				ctx,
				assignment,
				sourceOrdinal,
				serviceID,
				agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER,
			)
		},
	})
}

type databaseRecoveryCommands struct {
	observe      func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	recover      func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error)
	resumeIntent func(context.Context, *agentpb.BackupPostgresServiceProgress) error
}

func recoverDatabaseConsumerSet(ctx context.Context, input *agentpb.BackupWorkInput,
	sourceOrdinal uint32, resume *agentpb.BackupRestoreResume,
	publisher *checkpointdelivery.BackupSource, commands databaseRecoveryCommands,
) error {
	if commands.observe == nil || commands.recover == nil || publisher == nil {
		return errs.New(errs.KindInternal, "database consumer runtime is unavailable")
	}
	for ordinal := len(input.Steps[sourceOrdinal].ConsumerServiceIds) - 1; ordinal >= 0; ordinal-- {
		serviceID := input.Steps[sourceOrdinal].ConsumerServiceIds[ordinal]
		_, _, err := executionplan.BackupConsumer(input.EnvironmentId, input.Services,
			input.Artifacts, input.Steps[sourceOrdinal], serviceID)
		fact := backupServiceFact(input, serviceID)
		if fact == nil || fact.PriorRuntimeIntent == nil {
			return invalidDatabaseStaging()
		}
		intent := fact.PriorRuntimeIntent.Kind
		if err != nil {
			return err
		}
		observed, digest, err := commands.observe(ctx, serviceID)
		if err != nil {
			return err
		}
		progress := databaseServiceProgress(resume)
		if progress != nil && resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY &&
			(uint32(ordinal) > progress.ServiceCursor || uint32(ordinal) == progress.ServiceCursor &&
				(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
					progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)) {
			if intent == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
				!volumeAllRunning(observed, serviceID) ||
				intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
					volumeObservedRunning(observed, serviceID) {
				return invalidDatabaseStaging()
			}
			continue
		}
		if intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			if volumeObservedRunning(observed, serviceID) {
				return invalidDatabaseStaging()
			}
			if err := publishDatabaseService(ctx, input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING, digest); err != nil {
				return err
			}
			continue
		}
		pending := progress != nil &&
			resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY &&
			progress.ServiceCursor == uint32(ordinal) &&
			progress.ServiceId == serviceID
		if !pending || progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT {
			if !pending {
				if volumeObservedRunning(observed, serviceID) && !resume.GetDatabaseProgress().GetConsumersOnly() {
					return invalidDatabaseStaging()
				}
				if err := publishDatabaseService(ctx, input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT, digest); err != nil {
					return err
				}
			}
			if !volumeAllRunning(observed, serviceID) {
				if pending && commands.resumeIntent != nil {
					if err := commands.resumeIntent(ctx, progress); err != nil {
						return err
					}
				}
				_, digest, err = commands.recover(ctx, serviceID)
				if err != nil {
					return err
				}
			}
			if err := publishDatabaseService(ctx, input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED, digest); err != nil {
				return err
			}
		}
		if !pending || progress.Phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT {
			if err := publishDatabaseService(ctx, input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT, digest); err != nil {
				return err
			}
		}
		_, _, err = waitDatabaseConsumerObservation(ctx, input, sourceOrdinal, serviceID, commands.observe)
		if err != nil {
			return err
		}
		observed, digest, err = commands.observe(ctx, serviceID)
		if err != nil || !volumeAllRunning(observed, serviceID) {
			return invalidDatabaseStaging()
		}
		if err := publishDatabaseService(ctx, input.Steps[sourceOrdinal], publisher, uint32(ordinal), serviceID,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY, digest); err != nil {
			return err
		}
	}
	return nil
}

func publishDatabaseService(ctx context.Context, step *agentpb.BackupStepAuthority,
	publisher *checkpointdelivery.BackupSource, cursor uint32, serviceID string,
	phase agentpb.BackupServicePhase, digest [sha256.Size]byte,
) error {
	if step.GetRestore().GetMysql() != nil {
		return publisher.MysqlServiceProgress(ctx, &agentpb.BackupMySQLServiceProgress{
			ServiceCursor: cursor, ServiceId: serviceID, Phase: phase, ObservationSha256: digest[:]})
	}
	return publisher.PostgresServiceProgress(ctx, &agentpb.BackupPostgresServiceProgress{
		ServiceCursor: cursor, ServiceId: serviceID, Phase: phase, ObservationSha256: digest[:]})
}

func databaseServiceProgress(resume *agentpb.BackupRestoreResume) *agentpb.BackupPostgresServiceProgress {
	if resume == nil {
		return nil
	}
	if value := resume.GetPostgresServiceProgress(); value != nil {
		return value
	}
	if value := resume.GetMysqlServiceProgress(); value != nil {
		return &agentpb.BackupPostgresServiceProgress{ServiceCursor: value.ServiceCursor,
			ServiceId: value.ServiceId, Phase: value.Phase, ObservationSha256: value.ObservationSha256}
	}
	return nil
}
