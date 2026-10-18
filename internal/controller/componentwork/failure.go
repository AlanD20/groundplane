package componentwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) FailComponent(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ComponentWorkFailureRequest,
) (*agentpb.ComponentWorkFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		!componentexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Component failure reason is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := service.records.PrepareFailure(ctx, view, componentexecution.FailureReason(request.Reason), at, false)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		committed, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Component failure ownership changed; resend the saved report")
		}
		view, err = service.reportView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	raw, err := service.records.LoadFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	input, _, _, err := service.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	failure, err := componentexecution.DecodeFailure(attempt, input, raw, *attempt.Result)
	if err != nil || string(failure.Reason) != request.Reason {
		return nil, errs.New(errs.KindStateConflict, "Component failure replay differs from the saved result")
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkFailureAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
		Failure:  raw, Outcome: string(attempt.Outcome), StartedAt: timestamppb.New(*attempt.StartedAt),
		FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
