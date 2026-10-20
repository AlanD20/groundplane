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

func (owner *AgentWork) Complete(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackupWorkCompletionRequest,
) (*agentpb.BackupWorkCompletionAck, error) {
	if request == nil || request.Evidence == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup completion requires its last acknowledged checkpoint")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(view.Attempt.Attempt, attempt) ||
		(view.Work.Item.Item.Operation != workexecution.BackupPrune && view.Work.Item.Item.Operation != workexecution.Backup && view.Work.Item.Item.Operation != workexecution.Restore) ||
		view.Attempt.Attempt.StartedAt == nil ||
		view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Backup completion differs from its saved checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		var proof workqueue.Change
		if view.Work.Item.Item.Operation == workexecution.Backup {
			proof, err = owner.inputs.PrepareCaptureCompletion(ctx, view, at)
		} else if view.Work.Item.Item.Operation == workexecution.Restore {
			proof, err = owner.inputs.PrepareRestoreCompletion(ctx, view, at)
		} else {
			proof, err = owner.inputs.PreparePruneCompletion(ctx, view)
		}
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := owner.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		outcome := workexecution.Succeeded
		if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		}
		completion.Result = workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: true}
		completion.At = at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return nil, err
		}
		if !result.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Backup completion ownership changed; resend the saved result")
		}
		view, err = authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
		if err != nil {
			return nil, err
		}
	}
	saved := view.Attempt.Attempt
	if saved.FinishedAt == nil || saved.StartedAt == nil || saved.Result == nil || *saved.Result != evidence ||
		(saved.Outcome != workexecution.Succeeded && saved.Outcome != workexecution.Cancelled && saved.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "Backup has no matching settled completion")
	}
	owner.reportTerminal(ctx, saved)
	return &agentpb.BackupWorkCompletionAck{
		Attempt:  proto.CloneOf(request.Attempt),
		Evidence: proto.CloneOf(request.Evidence),
		StartedAt: timestamppb.New(
			*saved.StartedAt,
		),
		FinishedAt: timestamppb.New(*saved.FinishedAt),
		Outcome:    string(saved.Outcome),
	}, nil
}
