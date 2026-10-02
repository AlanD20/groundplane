package agent

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func postgresConsumerArtifact(assignment taskassignment.Assignment, serviceID string) (string,
	agentpb.BackupServiceRuntimeIntent, error,
) {
	if assignment.Plan == nil || assignment.Plan.BackupScope == nil {
		return "", 0, invalidAgentStaging()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range assignment.Plan.BackupScope.Services {
		if candidate.ServiceId == serviceID {
			if fact != nil {
				return "", 0, invalidAgentStaging()
			}
			fact = candidate
		}
	}
	if fact == nil || fact.PriorRuntimeIntent == nil {
		return "", 0, invalidAgentStaging()
	}
	var artifactID string
	for _, artifact := range assignment.Plan.Artifacts {
		for _, service := range artifact.Services {
			if service.ServiceId != serviceID || service.ComposeName != fact.CurrentName {
				continue
			}
			if artifactID != "" || artifact.OwnerId != assignment.Plan.BackupScope.EnvironmentId ||
				uint32(len(service.ExpectedLabels)) != fact.RequiredLabelCount ||
				service.ImageReference == "" {
				return "", 0, invalidAgentStaging()
			}
			labels, err := backupservicefact.LabelsDigest(service.ExpectedLabels)
			if err != nil || len(fact.LocalImageIdSha256) != sha256.Size ||
				!bytes.Equal(labels, fact.RequiredLabelsSha256) ||
				len(service.ImageConfigDigest) != 0 &&
					!bytes.Equal(service.ImageConfigDigest, fact.LocalImageIdSha256) {
				return "", 0, invalidAgentStaging()
			}
			artifactID = artifact.ArtifactId
		}
	}
	if artifactID == "" {
		return "", 0, invalidAgentStaging()
	}
	return artifactID, fact.PriorRuntimeIntent.Kind, nil
}

func (pool *WorkerPool) postgresStopConsumers(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep, resume *agentpb.BackupRestoreResume,
	publisher *backupStepCheckpoint, attempted *bool,
) error {
	if pool.compose == nil || publisher == nil || attempted == nil {
		return errs.New(errs.KindInternal, "PostgreSQL consumer runtime is unavailable")
	}
	for ordinal, serviceID := range execution.GetBackupStep().ConsumerServiceIds {
		artifactID, intent, err := postgresConsumerArtifact(assignment, serviceID)
		if err != nil {
			return err
		}
		observed, digest, err := pool.compose.ObservePostgresConsumer(ctx, assignment, artifactID, serviceID)
		if err != nil {
			return err
		}
		progress := resume.GetPostgresServiceProgress()
		if progress != nil && resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
			(uint32(ordinal) < progress.ServiceCursor || uint32(ordinal) == progress.ServiceCursor &&
				(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED ||
					progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)) {
			if volumeObservedRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			continue
		}
		if intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			if volumeObservedRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
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
				return invalidAgentStaging()
			}
			if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT, digest); err != nil {
				return err
			}
		}
		if volumeObservedRunning(observed, serviceID) {
			*attempted = true
			_, digest, err = pool.compose.ExecutePostgresConsumer(ctx, assignment, execution,
				artifactID, serviceID, agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_STOP)
			if err != nil {
				return err
			}
		}
		if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED, digest); err != nil {
			return err
		}
	}
	return nil
}

func (pool *WorkerPool) postgresRecoverConsumers(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep, resume *agentpb.BackupRestoreResume,
	publisher *backupStepCheckpoint,
) error {
	if pool.compose == nil || publisher == nil {
		return errs.New(errs.KindInternal, "PostgreSQL consumer runtime is unavailable")
	}
	for ordinal := len(execution.GetBackupStep().ConsumerServiceIds) - 1; ordinal >= 0; ordinal-- {
		serviceID := execution.GetBackupStep().ConsumerServiceIds[ordinal]
		artifactID, intent, err := postgresConsumerArtifact(assignment, serviceID)
		if err != nil {
			return err
		}
		observed, digest, err := pool.compose.ObservePostgresConsumer(ctx, assignment, artifactID, serviceID)
		if err != nil {
			return err
		}
		progress := resume.GetPostgresServiceProgress()
		if progress != nil && resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY &&
			(uint32(ordinal) > progress.ServiceCursor || uint32(ordinal) == progress.ServiceCursor &&
				(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
					progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)) {
			if intent == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
				!volumeAllRunning(observed, serviceID) ||
				intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING &&
					volumeObservedRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			continue
		}
		if intent != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
			if volumeObservedRunning(observed, serviceID) {
				return invalidAgentStaging()
			}
			if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
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
				if volumeObservedRunning(observed, serviceID) {
					return invalidAgentStaging()
				}
				if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
					agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT, digest); err != nil {
					return err
				}
			}
			if !volumeAllRunning(observed, serviceID) {
				_, digest, err = pool.compose.ExecutePostgresConsumer(
					ctx,
					assignment,
					execution,
					artifactID,
					serviceID,
					agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_RECOVER,
				)
				if err != nil {
					return err
				}
			}
			if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED, digest); err != nil {
				return err
			}
		}
		if !pending || progress.Phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT {
			if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT, digest); err != nil {
				return err
			}
		}
		_, err = pool.compose.ExecuteStep(ctx, assignment, &agentpb.ExecutionStep{StepId: execution.StepId,
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: artifactID, ServiceIds: []string{serviceID}}}})
		if err != nil {
			return err
		}
		observed, digest, err = pool.compose.ObservePostgresConsumer(ctx, assignment, artifactID, serviceID)
		if err != nil || !volumeAllRunning(observed, serviceID) {
			return invalidAgentStaging()
		}
		if err := publishPostgresService(ctx, publisher, uint32(ordinal), serviceID,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY, digest); err != nil {
			return err
		}
	}
	return nil
}

func publishPostgresService(ctx context.Context, publisher *backupStepCheckpoint, cursor uint32,
	serviceID string, phase agentpb.BackupServicePhase, digest [sha256.Size]byte,
) error {
	return publisher.publish(
		ctx,
		&agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_PostgresServiceProgress{
				PostgresServiceProgress: &agentpb.BackupPostgresServiceProgress{
					ServiceCursor: cursor, ServiceId: serviceID, Phase: phase, ObservationSha256: digest[:],
				},
			},
		},
	)
}
