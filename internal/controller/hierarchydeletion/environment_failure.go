package hierarchydeletion

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Reports) FailEnvironmentRemoval(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.EnvironmentRemovalFailureRequest,
) (*agentpb.EnvironmentRemovalFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || !hierarchyexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal failure request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || view.Work.Item.Item.Operation != workexecution.Hierarchy {
		return nil, errs.New(errs.KindStateConflict, "Environment removal failure belongs to another assignment")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		message, err := service.owner.capturedEnvironmentAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		input, err := hierarchyexecution.DecodeEnvironmentAssignment(message, authorization.Identity())
		if err != nil {
			return nil, err
		}
		input.Attempt = view.Attempt.Attempt
		at := time.Now().UTC()
		proof, result, err := service.owner.records.PrepareHierarchyEnvironmentFailure(ctx, view, input, hierarchyexecution.FailureReason(request.Reason), at)
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
			return nil, errs.New(errs.KindStateConflict, "Environment removal failure ownership changed; resend the saved failure")
		}
		view, err = service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
		if err != nil {
			return nil, err
		}
	}
	terminal := view.Attempt.Attempt
	if terminal.FinishedAt == nil || terminal.Result == nil || terminal.Outcome == workexecution.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Environment removal has no matching saved failure")
	}
	raw, err := service.owner.records.LoadHierarchyEnvironmentFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	failure, err := hierarchyexecution.DecodeEnvironmentFailure(terminal, raw, *terminal.Result)
	if err != nil || string(failure.Reason) != request.Reason {
		return nil, errs.New(errs.KindStateConflict, "Environment removal failure reason differs from its saved result")
	}
	digest, err := hex.DecodeString(terminal.Result.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	ack := &agentpb.EnvironmentRemovalFailureAck{Attempt: proto.CloneOf(request.Attempt), Failure: raw,
		Evidence: &agentpb.WorkInputReference{InputId: terminal.Result.ID, Sha256: digest}, Outcome: string(terminal.Outcome), FinishedAt: timestamppb.New(*terminal.FinishedAt)}
	if terminal.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*terminal.StartedAt)
	}
	return ack, nil
}
