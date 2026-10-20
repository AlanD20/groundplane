package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (owner *AgentWork) Fail(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackupWorkFailureRequest,
) (*agentpb.BackupWorkFailureAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		!executionplan.BackupFailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Backup failure reason is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return nil, err
	}
	operation := view.Work.Item.Item.Operation
	if !workexecution.SameAssignment(view.Attempt.Attempt, attempt) ||
		(operation != workexecution.BackupPrune && operation != workexecution.Restore && operation != workexecution.Backup) || view.Attempt.Attempt.StartedAt == nil {
		return nil, errs.New(errs.KindStateConflict, "Backup failure differs from its started attempt")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		var proof workqueue.Change
		var result workexecution.Completion
		if attempt.Direction == workexecution.Recovery {
			proof, result, err = owner.inputs.PrepareRecoveryFailure(
				ctx,
				view,
				executionplan.BackupFailureReason(request.Reason),
				at,
				false,
			)
		} else {
			proof, result, err = owner.inputs.PrepareStoppedFailure(ctx, view, executionplan.BackupFailureReason(request.Reason), at)
		}
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := owner.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		committed, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Backup failure ownership changed; resend the saved report")
		}
		view, err = authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
		if err != nil {
			return nil, err
		}
	}
	raw, err := owner.inputs.LoadFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	saved := view.Attempt.Attempt
	failure, err := executionplan.DecodeBackupWorkFailure(saved, raw, *saved.Result)
	if err != nil || string(failure.Reason) != request.Reason {
		return nil, errs.New(errs.KindStateConflict, "Backup failure replay differs from the saved result")
	}
	digest, err := hex.DecodeString(saved.Result.SHA256)
	if err != nil {
		return nil, err
	}
	owner.reportTerminal(ctx, saved)
	return &agentpb.BackupWorkFailureAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: saved.Result.ID, Sha256: digest}, Failure: raw,
		Outcome: string(
			saved.Outcome,
		), StartedAt: timestamppb.New(*saved.StartedAt), FinishedAt: timestamppb.New(*saved.FinishedAt)}, nil
}
