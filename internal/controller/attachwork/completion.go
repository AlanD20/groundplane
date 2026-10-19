package attachwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
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

func (service *Reports) CompleteAttachProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachProvisionCompletionRequest,
) (*agentpb.AttachProvisionCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.CheckpointSequence == 0 ||
		request.CheckpointSequence > attachexecution.MaximumProvisionReports || request.CheckpointEvidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach completion requires its final command checkpoint")
	}
	view, guards, err := service.readView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{
		ID:     request.CheckpointEvidence.InputId,
		SHA256: hex.EncodeToString(request.CheckpointEvidence.Sha256),
	}
	if evidence.Validate() != nil || view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != request.CheckpointSequence ||
		view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Attach completion differs from its saved checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		input, err := service.inputs.Forward(ctx, view)
		if err != nil {
			return nil, err
		}
		defer attachexecution.ClearProvision(input.Provision)
		at := time.Now().UTC()
		binding, result, err := service.inputs.repository.PrepareCompletion(ctx, view, input, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(binding.Mutations)
		var receipt workqueue.Change
		if view.Work.Task.Task.Action == workexecution.ActionAttach {
			receipt, err = service.inputs.repository.PrepareDirectCompletionReceipt(ctx, view, result)
		} else {
			receipt, err = service.units.PrepareAttachCompletionReceipt(ctx, view, result)
		}
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(receipt.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		outcome := workexecution.Succeeded
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
		completion.Result, completion.At = workexecution.Completion{
			Outcome:        outcome,
			Evidence:       result,
			EffectsSettled: true,
		}, at
		terminal, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(terminal.Mutations)
		committed, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, binding, receipt, terminal)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(
				errs.KindStateConflict,
				"Attach completion ownership changed; resend the saved completion",
			)
		}
		view, _, err = service.readView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil ||
		(attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "Attach has no successful binding settlement for this execution")
	}
	if _, err := service.inputs.repository.LoadResult(ctx, view); err != nil {
		return nil, err
	}
	service.reportForwardResult(ctx, attempt, true)
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionCompletionAck{
		Attempt:            proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence,
		CheckpointEvidence: proto.CloneOf(
			request.CheckpointEvidence,
		),
		Outcome:       string(attempt.Outcome),
		FinishedAt:    timestamppb.New(*attempt.FinishedAt),
		BindingResult: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
	}, nil
}
