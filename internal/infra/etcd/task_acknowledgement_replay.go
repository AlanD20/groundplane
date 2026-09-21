package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// validateTaskAcknowledgementReplay checks the resource transitions already
// committed with a terminal Task, using the same snapshot as its assignment.
func (repository *TaskRepository) validateTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
	environmentID string,
	environmentRemoval, zoneRemoval bool,
) error {
	if environmentID != "" {
		if err := repository.validateEnvironmentCreationReplay(
			ctx, task, terminalStatus, readRevision,
		); err != nil {
			return err
		}
	}
	if environmentRemoval {
		if err := repository.validateEnvironmentRemovalReplay(
			ctx, task, terminalStatus, readRevision,
		); err != nil {
			return err
		}
	}
	if zoneRemoval {
		if err := repository.validateZoneRemovalReplay(
			ctx, task, terminalStatus, readRevision,
		); err != nil {
			return err
		}
	}
	if err := repository.validateAttachTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBlueprintAttachTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateSecretTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateConnectorTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateRunnerTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateScriptTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateReleaseGroupTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if task.Params[TaskReleasePublicationParam] != "" && task.Type == taskjournal.TaskUpdate {
		if err := repository.validateBlueprintCandidateTerminalReplay(
			ctx, task, terminalStatus, readRevision,
		); err != nil {
			return err
		}
	} else if task.Params[TaskReleasePublicationParam] != "" {
		headRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				releases.ReleaseOperationKey(task.OperationID),
			},
			Revision: readRevision,
		})
		if err != nil || headRead == nil || len(headRead.Values) != 1 || headRead.Values[0] == nil {
			return releases.CorruptReleaseRecord()
		}
		head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](
			headRead.Values[0].Value,
			"release-operation",
		)
		if err != nil || repository.validateReleaseTerminalMembers(
			ctx, task, head, terminalStatus, readRevision,
		) != nil {
			return releases.CorruptReleaseRecord()
		}
	}
	if err := repository.validateRemovalTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBackingZoneTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateComponentTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validatePlatformComponentTaskAcknowledgementReplay(
		ctx, task, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateBackupKeyRotationTaskAcknowledgementReplay(
		ctx, task, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if err := repository.validateTaskRetentionReplay(
		ctx, task, readRevision,
	); err != nil {
		return err
	}
	return nil
}
