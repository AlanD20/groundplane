package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StreamConfigRestore receives only the archive captured by the native work
// input. Its final credit follows durable generation publication.
func (service *AgentWork) StreamConfigRestore(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	binding backupconfigtransfer.Binding,
	transport backupconfigtransfer.ReceiverTransport,
) error {
	if service == nil || service.configRuntime == nil || service.configRestores == nil ||
		service.configSnapshots == nil || ctx == nil || transport == nil {
		return configSnapshotInvalid()
	}
	if _, err := authorization.Conditions(); err != nil {
		return err
	}
	if binding.Validate() != nil || binding.Attempt.Executor != authorization.Identity() ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		return configSnapshotGuardConflict()
	}
	attempt := binding.Attempt
	view, err := service.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
	if err != nil {
		return err
	}
	assignment, _, err := service.inputs.LoadConfigTransfer(ctx, service.queue, binding, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[binding.SourceOrdinal]
	config := step.GetRestore().GetConfig()
	if config == nil {
		return configSnapshotGuardConflict()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	content := config.GetExpectedArchive().GetContent()
	owner, err := configRestoreTransferOwner(backupconfiguration.ConfigTransferOwner{Binding: binding}, step.GetRestore())
	if err != nil {
		return err
	}
	receiver, credit, err := OpenConfigRestoreReceiver(
		ctx,
		owner,
		content,
		service.configRuntime,
		service.configSnapshots.protector,
	)
	if err != nil {
		return err
	}
	defer receiver.Abort()
	if receiver.Progress().Complete {
		if err := service.sealConfigRestoreCompletion(ctx, owner, step.GetRestore().PointId, receiver); err != nil {
			return err
		}
	}
	if err := transport.SendCredit(ctx, credit); err != nil {
		return err
	}
	for !receiver.Progress().Complete {
		frame, err := transport.ReadFrame(ctx)
		if err != nil {
			return err
		}
		credit, err := receiver.AcceptFrame(ctx, frame)
		backupconfigtransfer.ClearFrame(frame)
		if err != nil {
			return err
		}
		if credit != nil {
			if receiver.Progress().Complete {
				if err := service.sealConfigRestoreCompletion(ctx, owner, step.GetRestore().PointId, receiver); err != nil {
					return err
				}
			}
			if err := transport.SendCredit(ctx, credit); err != nil {
				return err
			}
		}
	}
	return nil
}

// The final grant cannot precede durable complete-generation publication.
// Reconnect repeats only this exact seal before redelivering a lost final grant.
func (service *AgentWork) sealConfigRestoreCompletion(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, pointID string, receiver *ConfigRestoreReceiver,
) error {
	completed, err := receiver.Completed()
	if err != nil {
		return err
	}
	_, err = service.configRuntime.SealConfigRestoreGeneration(ctx, backupconfiguration.ConfigRestoreGenerationRecord{
		Owner: owner, Completed: completed,
	})
	if err != nil {
		return err
	}
	current, err := service.configRuntime.GetBackupRestore(ctx, owner.Transfer.Binding.Attempt.TaskID)
	if err != nil {
		return err
	}
	if current.Record.Point.ID != pointID || current.Record.RestoreGenerationID != owner.GenerationID ||
		current.Record.ConfigProgress == nil {
		return configSnapshotGuardConflict()
	}
	var publication *ConfigRestorePublication
	if current.Record.ConfigProgress.RevisionRootRevision == 0 {
		publication, err = service.configRestores.PreparePublished(ctx, owner, completed.Content, pointID)
	} else {
		// Native staging already has its completion receipt. Reconnection
		// resumes that publication rather than trying to stage against a head
		// the same Restore may already have switched.
		publication, err = service.configRestores.PrepareCandidate(ctx, owner, completed.Content, pointID)
	}
	if err != nil {
		return err
	}
	defer publication.Clear()
	if current.Record.ConfigProgress.RevisionRootRevision > 0 {
		return service.configRuntime.PublishConfigRestore(
			ctx,
			publication.SourceSeal,
			publication.Projection,
			publication.Identities,
		)
	}
	return nil
}
