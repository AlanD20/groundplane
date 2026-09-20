package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// validatePendingAbortReplay checks resource state at the aborted Task's
// read revision before resuming any remaining unassigned release cleanup.
func (repository *TaskRepository) validatePendingAbortReplay(
	ctx context.Context,
	current etcdstore.Versioned[TaskRecord],
	environmentCreation, environmentRemoval, zoneRemoval bool,
) error {
	if err := repository.validatePendingScriptAbortReplay(ctx, current); err != nil {
		return err
	}
	if environmentCreation {
		if err := repository.validateEnvironmentCreationReplay(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		); err != nil {
			return err
		}
	}
	if environmentRemoval {
		if err := repository.validateEnvironmentRemovalReplay(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		); err != nil {
			return err
		}
	}
	if zoneRemoval {
		if err := repository.validateZoneRemovalReplay(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		); err != nil {
			return err
		}
	}
	if err := repository.validateAttachTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBlueprintAttachTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateSecretTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateConnectorTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateRunnerTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateScriptTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateReleaseGroupTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateRemovalTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBackingZoneTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateComponentTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validatePlatformComponentTaskAcknowledgementReplay(
		ctx, current.Record, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBackupKeyRotationTaskAcknowledgementReplay(
		ctx, current.Record, TaskStatusAborted, current.ReadRevision,
	); err != nil {
		return err
	}
	if err := repository.validateTaskRetentionReplay(
		ctx, current.Record, current.ReadRevision,
	); err != nil {
		return err
	}
	return nil
}
