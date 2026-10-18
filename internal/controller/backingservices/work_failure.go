package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) FailBackingProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionFailureRequest,
) (*agentpb.BackingProvisionFailureAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation failure request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, false)
	if err != nil {
		return nil, err
	}
	reason := backingprovision.FailureReason(request.Reason)
	if view.Attempt.Attempt.FinishedAt == nil {
		change, _, err := service.records.PrepareFailure(ctx, view, reason, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		change.Conditions = append(change.Conditions, guards...)
		committed, err := service.queue.Commit(ctx, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "backing creation failure ownership changed; resend the saved failure")
		}
		view, err = service.queue.ReadResume(ctx, view.Attempt.Attempt.TaskID, view.Attempt.Attempt.WorkItemID, view.Attempt.Attempt.ID, authorization.Identity())
		if err != nil {
			return nil, err
		}
	}
	failure, encodedFailure, err := service.records.ReadFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	if failure.Reason != reason {
		clear(encodedFailure)
		return nil, errs.New(errs.KindStateConflict, "backing creation failure replay differs from its retained cause")
	}
	evidence, err := encodeBackingReference(*view.Attempt.Attempt.Result)
	if err != nil {
		clear(encodedFailure)
		return nil, err
	}
	ack := &agentpb.BackingProvisionFailureAck{Attempt: proto.CloneOf(request.Attempt), Failure: encodedFailure,
		Evidence: evidence, Outcome: string(view.Attempt.Attempt.Outcome), FinishedAt: timestamppb.New(*view.Attempt.Attempt.FinishedAt)}
	if view.Attempt.Attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*view.Attempt.Attempt.StartedAt)
	}
	service.reportTerminal(ctx, view, false)
	return ack, nil
}
