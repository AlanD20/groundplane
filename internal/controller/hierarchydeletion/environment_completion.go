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

func (service *Reports) CompleteEnvironmentRemoval(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.EnvironmentRemovalCompletionRequest,
) (*agentpb.EnvironmentRemovalCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.CheckpointSequence != 4 ||
		request.CheckpointEvidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal completion requires its final checkpoint")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{
		ID:     request.CheckpointEvidence.InputId,
		SHA256: hex.EncodeToString(request.CheckpointEvidence.Sha256),
	}
	if evidence.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal completion evidence is invalid")
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Work.Item.Item.Operation != workexecution.Hierarchy ||
		view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != 4 ||
		view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Environment removal completion differs from its saved checkpoint")
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
		proof, err := service.owner.records.PrepareHierarchyEnvironmentCompletion(ctx, view, input, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result = workexecution.Completion{
			Outcome:        workexecution.Succeeded,
			Evidence:       evidence,
			EffectsSettled: true,
		}
		completion.At = at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return nil, err
		}
		if !result.Succeeded {
			return nil, errs.New(
				errs.KindStateConflict,
				"Environment removal completion ownership changed; resend the saved result",
			)
		}
		view, err = authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != evidence ||
		attempt.Outcome != workexecution.Succeeded {
		return nil, errs.New(
			errs.KindStateConflict,
			"saved Environment removal result does not acknowledge completed absence",
		)
	}
	return &agentpb.EnvironmentRemovalCompletionAck{Attempt: proto.CloneOf(request.Attempt), CheckpointSequence: 4,
		CheckpointEvidence: proto.CloneOf(
			request.CheckpointEvidence,
		), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
