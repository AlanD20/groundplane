package attachwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Reports) FailAttachProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachProvisionFailureRequest,
) (*agentpb.AttachProvisionFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		!attachexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Attach failure request is invalid")
	}
	view, guards, err := service.readView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		input, err := service.inputs.Forward(ctx, view)
		if err != nil {
			return nil, err
		}
		defer attachexecution.ClearProvision(input.Provision)
		at := time.Now().UTC()
		failure, result, err := service.inputs.repository.PrepareFailure(
			ctx,
			view,
			input,
			attachexecution.FailureReason(request.Reason),
			at,
			false,
		)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(failure.Mutations)
		binding, err := service.inputs.repository.PrepareFailedBinding(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(binding.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
		var terminal workqueue.Change
		if result.EffectsSettled {
			terminal, err = workqueue.PrepareCompletion(completion)
		} else {
			terminal, err = workqueue.PrepareCompletionWithRecovery(completion, at.Add(attachexecution.InspectionBudget))
		}
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(terminal.Mutations)
		committed, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, failure, binding, terminal)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Attach failure ownership changed; resend the saved failure")
		}
		view, _, err = service.readView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	raw, err := service.inputs.repository.LoadFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	saved, err := attachexecution.DecodeProvisionFailure(attempt, raw, *attempt.Result)
	if err != nil || saved.Reason != attachexecution.FailureReason(request.Reason) {
		return nil, errs.New(errs.KindStateConflict, "Attach failure replay changed its recorded reason")
	}
	service.reportFailure(ctx, attempt, saved)
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.AttachProvisionFailureAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest}, Failure: raw,
		Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}
