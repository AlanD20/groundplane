package environment

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
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

func (owner *Work) ReportEnvironmentProvision(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.EnvironmentProvisionResultRequest,
) (*agentpb.EnvironmentProvisionResultAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "environment provisioning report is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	if !workexecution.SameAssignment(attempt, claimed) || view.Work.Item.Item.Operation != workexecution.Environment ||
		!view.Work.Task.Task.PublicationComplete || view.Work.Task.Task.CollectionStartedAt != nil {
		return nil, errs.New(errs.KindStateConflict, "environment report has no retained captured execution")
	}
	report, err := environmentexecution.DecodeResult(attempt, request)
	if err != nil {
		return nil, err
	}
	if attempt.FinishedAt == nil {
		at := time.Now().UTC()
		if report.ObservedAt.After(at.Add(30 * time.Second)) {
			return nil, errs.New(errs.KindValidationFailed, "environment observation is ahead of the Controller clock")
		}
		if report.ObservedAt.After(at) {
			at = report.ObservedAt
		}
		proof, completionResult, err := owner.records.PrepareResult(ctx, view, report, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := owner.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = completionResult, at
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
			return nil, errs.New(
				errs.KindStateConflict,
				"environment result ownership changed; resend the saved report",
			)
		}
		view, err = authorization.ReadResult(ctx, owner.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return nil, err
		}
		attempt = view.Attempt.Attempt
	}
	receipt, err := owner.records.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	ref, err := report.Reference(attempt)
	if err != nil {
		return nil, err
	}
	if attempt.Result == nil || *attempt.Result != ref || receipt.Record.Report != report ||
		receipt.Record.Attempt.Outcome != attempt.Outcome || attempt.FinishedAt == nil ||
		!receipt.Record.Attempt.FinishedAt.Equal(*attempt.FinishedAt) {
		return nil, errs.New(errs.KindStateConflict, "environment report replay differs from the accepted result")
	}
	evidence, err := environmentexecution.EncodeReference(ref)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.EnvironmentProvisionResultAck{Attempt: proto.CloneOf(request.Attempt), Evidence: evidence,
		Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}
