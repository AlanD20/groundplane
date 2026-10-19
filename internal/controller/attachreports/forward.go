package attachreports

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Reports) CheckpointAttachRemoval(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachRemovalCheckpointRequest,
) (*agentpb.AttachRemovalCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 ||
		len(request.Report) > attachexecution.MaximumRemovalReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach checkpoint is missing or oversized")
	}
	view, owner, guards, err := service.readView(ctx, authorization, request.Attempt, workexecution.Forward)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.StartedAt == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach checkpoint has no saved execution start")
	}
	input, err := owner.RemovalInput(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRemoval(input.Removal)
	report, err := attachexecution.DecodeRemovalCheckpointRequest(input, request)
	if err != nil {
		return nil, err
	}
	if report.Progress.UpdatedAt.After(time.Now().UTC()) {
		return nil, errs.New(errs.KindValidationFailed, "Attach report time is in the future")
	}
	change, err := owner.PrepareRemovalCheckpoint(ctx, view, input, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	if err := service.commit(ctx, guards, change); err != nil {
		return nil, err
	}
	return attachexecution.EncodeRemovalCheckpointAck(input, report)
}

func (service *Reports) CompleteAttachRemoval(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachRemovalCompletionRequest,
) (*agentpb.AttachRemovalCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.CheckpointSequence == 0 ||
		request.CheckpointSequence > attachexecution.MaximumRemovalReports || request.CheckpointEvidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach completion requires its final command checkpoint")
	}
	view, owner, guards, err := service.readView(ctx, authorization, request.Attempt, workexecution.Forward)
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{
		ID:     request.CheckpointEvidence.InputId,
		SHA256: hex.EncodeToString(request.CheckpointEvidence.Sha256),
	}
	if evidence.Validate() != nil || view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != request.CheckpointSequence ||
		view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Attach completion differs from its saved checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		input, err := owner.RemovalInput(ctx, view)
		if err != nil {
			return nil, err
		}
		defer attachexecution.ClearRemoval(input.Removal)
		at := time.Now().UTC()
		proof, err := owner.PrepareRemovalCompletion(ctx, view, input, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		if err := service.commitCompletion(ctx, view, guards, proof, workexecution.Completion{
			Outcome: completedOutcome(view.Work), Evidence: evidence, EffectsSettled: true}, at); err != nil {
			return nil, err
		}
		view, _, _, err = service.readView(ctx, authorization, request.Attempt, workexecution.Forward)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != evidence ||
		(attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "Attach has no matching saved completion")
	}
	return &agentpb.AttachRemovalCompletionAck{
		Attempt:            proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence,
		CheckpointEvidence: proto.CloneOf(
			request.CheckpointEvidence,
		),
		FinishedAt: timestamppb.New(*attempt.FinishedAt),
		Outcome:    string(attempt.Outcome),
	}, nil
}

func (service *Reports) FailAttachRemoval(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachRemovalFailureRequest,
) (*agentpb.AttachRemovalFailureAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		!attachexecution.FailureReason(request.Reason).Valid() {
		return nil, errs.New(errs.KindValidationFailed, "Attach failure request is invalid")
	}
	view, owner, guards, err := service.readView(ctx, authorization, request.Attempt, workexecution.Forward)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		input, err := owner.RemovalInput(ctx, view)
		if err != nil {
			return nil, err
		}
		defer attachexecution.ClearRemoval(input.Removal)
		at := time.Now().UTC()
		proof, result, err := owner.PrepareRemovalFailure(
			ctx,
			view,
			input,
			attachexecution.FailureReason(request.Reason),
			at,
		)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		if err := service.commitCompletion(ctx, view, guards, proof, result, at); err != nil {
			return nil, err
		}
		view, _, _, err = service.readView(ctx, authorization, request.Attempt, workexecution.Forward)
		if err != nil {
			return nil, err
		}
	}
	terminal := view.Attempt.Attempt
	if terminal.FinishedAt == nil || terminal.Result == nil || terminal.Outcome == workexecution.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Attach has no matching saved failure")
	}
	raw, err := owner.LoadRemovalFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	failure, err := attachexecution.DecodeRemovalFailure(terminal, raw, *terminal.Result)
	if err != nil || string(failure.Reason) != request.Reason {
		clear(raw)
		return nil, errs.New(errs.KindStateConflict, "Attach failure reason differs from its saved result")
	}
	digest, err := hex.DecodeString(terminal.Result.SHA256)
	if err != nil {
		clear(raw)
		return nil, err
	}
	ack := &agentpb.AttachRemovalFailureAck{Attempt: proto.CloneOf(request.Attempt), Failure: raw,
		Evidence: &agentpb.WorkInputReference{
			InputId: terminal.Result.ID,
			Sha256:  digest,
		}, Outcome: string(terminal.Outcome), FinishedAt: timestamppb.New(*terminal.FinishedAt)}
	if terminal.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*terminal.StartedAt)
	}
	return ack, nil
}
