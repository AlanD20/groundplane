package taskcheckpoint

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *ReleaseCheckpointService) CompleteRelease(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ReleaseWorkCompletionRequest,
) (*agentpb.ReleaseWorkCompletionAck, error) {
	if ctx == nil || service == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		request.CheckpointSequence == 0 || request.CheckpointEvidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Release completion request is invalid")
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
		return nil, errs.New(errs.KindValidationFailed, "Release completion requires its exact checkpoint")
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		service.outcomes[view.Work.Item.Item.Operation] == nil ||
		view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != request.CheckpointSequence ||
		view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Release completion differs from the saved attempt checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		if !view.ActiveAttempt || !view.CanSettleResult() {
			return nil, errs.New(errs.KindStateConflict, "Release completion no longer owns the active attempt")
		}
		owner := service.outcomes[view.Work.Item.Item.Operation]
		if owner == nil {
			return nil, errs.New(errs.KindStateConflict, "Release completion owner is unavailable")
		}
		err = owner.SettleCompleted(ctx, view, guards, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		view, err = authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if service.restoring(view) && attempt.Outcome != workexecution.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "saved recovery result does not prove completed restoration")
	}
	if !workexecution.SameAssignment(attempt, claimed) || attempt.FinishedAt == nil || attempt.Result == nil ||
		*attempt.Result != evidence ||
		(attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "saved Release result does not acknowledge this completion")
	}
	return &agentpb.ReleaseWorkCompletionAck{
		Attempt:            proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence,
		CheckpointEvidence: proto.CloneOf(
			request.CheckpointEvidence,
		),
		Outcome:    string(attempt.Outcome),
		FinishedAt: timestamppb.New(*attempt.FinishedAt),
	}, nil
}
