package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) restoreRecoveryCheckpoint(ctx context.Context,
	authorization agentregistration.WorkAuthorization, view workqueue.ResumeView,
	request *agentpb.BackupWorkCheckpointRequest, sessionGuards []keyvalue.Condition,
) (*agentpb.BackupWorkCheckpointAck, error) {
	admitted, err := owner.inputs.InspectRestoreRecoveryCheckpoint(ctx, view, request)
	if err != nil {
		return nil, err
	}
	var candidate *ConfigRestorePublication
	if admitted.Replay == nil {
		candidate, err = owner.prepareRestorePublication(ctx, view, admitted)
		if err != nil {
			return nil, err
		}
		if candidate != nil {
			defer candidate.Clear()
			attempt := view.Attempt.Attempt
			view, err = owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
			if err != nil {
				return nil, err
			}
			admitted, err = owner.inputs.InspectRestoreRecoveryCheckpoint(ctx, view, request)
			if err != nil {
				return nil, err
			}
		}
	}
	if admitted.Replay != nil {
		return admitted.Replay, nil
	}
	at := time.Now().UTC()
	restore := admitted.Input().Steps[request.SourceOrdinal].GetRestore()
	var settlement workqueue.Change
	evidence := backupwork.CheckpointEvidence{}
	switch {
	case restore.GetPostgres() != nil, restore.GetMysql() != nil:
		settlement, err = owner.prepareDatabaseRestoreProgress(ctx, view, admitted, at)
	case restore.GetVolume() != nil:
		settlement, err = owner.prepareVolumeRestoreProgress(ctx, view, admitted, at)
	case restore.GetConfig() != nil:
		if request.GetConfig() != nil {
			evidence, err = owner.configRestoreEvidence(ctx, view, admitted)
			if err != nil {
				return nil, err
			}
		}
		settlement, err = owner.prepareConfigRestoreProgress(ctx, view, admitted, candidate, at)
	default:
		return nil, errs.New(errs.KindStateConflict, "Restore recovery source has no settlement owner")
	}
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(settlement.Mutations)
	checkpoint, replay, err := owner.inputs.PrepareRestoreRecoveryCheckpoint(ctx, view, request, evidence, at)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(checkpoint.Mutations)
	if replay != nil {
		return nil, errs.New(errs.KindStateConflict, "Restore recovery checkpoint changed within its snapshot")
	}
	checkpoint.Conditions = append(checkpoint.Conditions, sessionGuards...)
	result, err := owner.queue.Commit(ctx, checkpoint, settlement)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Restore recovery ownership changed before settlement")
	}
	return owner.inputs.ReadRestoreRecoveryCheckpointAcknowledgement(ctx, request, admitted.Recovery,
		authorization.Identity(), result.Revision)
}
