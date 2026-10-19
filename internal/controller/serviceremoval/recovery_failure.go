package serviceremoval

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) FailRemovalRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceRemovalRecoveryFailureRequest,
) (*agentpb.ServiceRemovalRecoveryFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		!lifecycleexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery failure reason is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.recoveryView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	captured, _, err := service.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := service.inputs.PrepareRecoveryFailure(
			ctx,
			view,
			input,
			lifecycleexecution.FailureReason(request.Reason),
			at,
			false,
		)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.At, completion.Result = at, result
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		committed, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service removal recovery ownership changed; resend the saved failure",
			)
		}
		view, err = service.recoveryView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	raw, failure, err := service.inputs.LoadRecoveryFailure(ctx, view, input)
	if err != nil {
		return nil, err
	}
	if string(failure.Reason) != request.Reason {
		clear(raw)
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal recovery failure replay differs from its stored reason",
		)
	}
	attempt := view.Attempt.Attempt
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		clear(raw)
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryFailureAck{Attempt: proto.CloneOf(request.Attempt), Failure: raw,
		Evidence:   &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
		FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
