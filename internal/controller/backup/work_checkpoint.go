package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) Checkpoint(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackupWorkCheckpointRequest,
) (*agentpb.BackupWorkCheckpointAck, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	if request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint is required")
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(view.Attempt.Attempt, attempt) {
		return nil, errs.New(errs.KindStateConflict, "Backup checkpoint belongs to another active operation")
	}
	if attempt.Direction == workexecution.Recovery {
		return owner.restoreRecoveryCheckpoint(ctx, authorization, view, request, guards)
	}
	admitted, err := owner.inputs.InspectCheckpoint(ctx, view, request)
	if err != nil {
		return nil, err
	}
	var candidate *ConfigRestorePublication
	if view.Work.Item.Item.Operation == workexecution.Restore {
		candidate, err = owner.prepareRestorePublication(ctx, view, admitted)
		if err != nil {
			return nil, err
		}
		if candidate != nil {
			defer candidate.Clear()
			// Immutable staging may advance the store. Join all settlement
			// guards at a fresh snapshot rather than mixing read revisions.
			view, err = authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
			if err != nil {
				return nil, err
			}
			admitted, err = owner.inputs.InspectCheckpoint(ctx, view, request)
			if err != nil {
				return nil, err
			}
		}
	}
	if admitted.Replay != nil {
		if err := owner.finishCaptureSource(ctx, authorization, request); err != nil {
			return nil, err
		}
		if candidate != nil && request.GetConfig().GetTransferCompleted() != nil {
			if err := owner.configRuntime.PublishConfigRestore(ctx, candidate.SourceSeal, candidate.Projection, candidate.Identities); err != nil {
				return nil, err
			}
		}
		return admitted.Replay, nil
	}
	at := time.Now().UTC()
	var settlement workqueue.Change
	evidence := backupwork.CheckpointEvidence{}
	switch view.Work.Item.Item.Operation {
	case workexecution.BackupPrune:
		settlement, err = owner.inputs.PreparePruneSettlement(ctx, view, admitted, at)
	case workexecution.Backup:
		if admitted.Request.GetConfig() != nil {
			evidence, err = owner.configCaptureEvidence(ctx, view, admitted)
		} else {
			evidence, err = owner.inputs.CaptureCheckpointEvidence(ctx, view, admitted)
		}
		if err != nil {
			return nil, err
		}
		settlement, err = owner.inputs.PrepareCaptureProgress(ctx, view, admitted, evidence, at)
	case workexecution.Restore:
		restore := admitted.Assignment.Input.Steps[request.SourceOrdinal].GetRestore()
		if restore.GetPostgres() != nil || restore.GetMysql() != nil {
			settlement, err = owner.prepareDatabaseRestoreProgress(ctx, view, admitted, at)
			break
		}
		if admitted.Assignment.Input.Steps[request.SourceOrdinal].GetRestore().GetVolume() != nil {
			settlement, err = owner.prepareVolumeRestoreProgress(ctx, view, admitted, at)
			break
		}
		if admitted.Assignment.Input.Steps[request.SourceOrdinal].GetRestore().GetConfig() == nil {
			return nil, errs.New(errs.KindStateConflict, "Restore source has no native settlement owner")
		}
		if request.GetConfig() != nil {
			evidence, err = owner.configRestoreEvidence(ctx, view, admitted)
			if err != nil {
				return nil, err
			}
		}
		settlement, err = owner.prepareConfigRestoreProgress(ctx, view, admitted, candidate, at)
	default:
		return nil, errs.New(errs.KindStateConflict, "Backup checkpoint operation has no settlement owner")
	}
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(settlement.Mutations)
	checkpoint, replay, err := owner.inputs.PrepareCheckpoint(ctx, view, admitted.Request, evidence, at)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(checkpoint.Mutations)
	if replay != nil {
		return nil, errs.New(errs.KindStateConflict, "Backup checkpoint changed within its snapshot")
	}
	checkpoint.Conditions = append(checkpoint.Conditions, guards...)
	// The queue enforces the combined transaction budget. Never split resource
	// retirement from its receipt, and never acknowledge an uncertain commit.
	result, err := owner.queue.Commit(ctx, checkpoint, settlement)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Backup ownership changed before settlement")
	}
	if candidate != nil && request.GetConfig().GetTransferCompleted() != nil {
		if err := owner.configRuntime.PublishConfigRestore(ctx, candidate.SourceSeal, candidate.Projection, candidate.Identities); err != nil {
			return nil, err
		}
	}
	if err := owner.finishCaptureSource(ctx, authorization, request); err != nil {
		return nil, err
	}
	ack, err := owner.inputs.ReadCheckpointAcknowledgement(
		ctx,
		admitted.Request,
		admitted.Assignment.Input,
		authorization.Identity(),
		result.Revision,
	)
	if err == nil {
		owner.reportCheckpoint(ctx, view, admitted.Assignment.Input, request, ack)
	}
	return ack, err
}
