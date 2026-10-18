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

func (service *WorkReports) FailLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleFailureRequest,
) (*agentpb.ServiceLifecycleFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		!lifecycleexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Service removal failure reason is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, input, err := service.forwardReport(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := service.inputs.PrepareFailure(ctx, view, input, lifecycleexecution.FailureReason(request.Reason), at, false)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
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
			return nil, errs.New(errs.KindStateConflict, "Service removal failure ownership changed; resend the saved report")
		}
		view, input, err = service.forwardReport(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	raw, _, err := service.inputs.LoadFailure(ctx, view, input)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	failure, err := lifecycleexecution.DecodeFailure(attempt, input, raw, *attempt.Result)
	if err != nil || string(failure.Reason) != request.Reason {
		clear(raw)
		return nil, errs.New(errs.KindStateConflict, "Service removal failure replay differs from the saved result")
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		clear(raw)
		return nil, err
	}
	return &agentpb.ServiceLifecycleFailureAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
		Failure:  raw, Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
