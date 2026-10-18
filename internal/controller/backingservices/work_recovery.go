package backingservices

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) RecoverBackingProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionRecoveryRequest,
) (*agentpb.BackingProvisionRecoveryAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 || len(request.Report) > backingprovision.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "backing creation cleanup report is missing or oversized")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.Direction != workexecution.Recovery {
		return nil, errs.New(errs.KindStateConflict, "backing creation cleanup report names a forward attempt")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		change, _, err := service.records.PrepareRecoveryResult(ctx, view, request.Report, time.Now().UTC())
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
			return nil, errs.New(errs.KindStateConflict, "backing creation cleanup ownership changed; resend the saved report")
		}
		view, err = service.reportView(ctx, authorization, request.Attempt, true)
		if err != nil {
			return nil, err
		}
	}
	saved, err := service.records.ReadRecoveryResult(ctx, view)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(saved, request.Report) {
		return nil, errs.New(errs.KindStateConflict, "backing creation cleanup replay differs from its retained result")
	}
	attempt := view.Attempt.Attempt
	evidence, err := encodeBackingReference(*attempt.Result)
	if err != nil {
		return nil, err
	}
	service.reportTerminal(ctx, view, true)
	return &agentpb.BackingProvisionRecoveryAck{Attempt: proto.CloneOf(request.Attempt), Evidence: evidence,
		Outcome: string(attempt.Outcome), StartedAt: timestamppb.New(*attempt.StartedAt), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
