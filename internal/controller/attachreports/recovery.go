package attachreports

import (
	"bytes"
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

func (service *Reports) CompleteAttachRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachRecoveryCompletionRequest,
) (*agentpb.AttachRecoveryCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil ||
		len(request.Report) == 0 || len(request.Report) > attachexecution.MaximumRemovalRecoveryBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach recovery requires its bounded inspection report")
	}
	view, owner, guards, err := service.readView(ctx, authorization, request.Attempt, workexecution.Recovery)
	if err != nil {
		return nil, err
	}
	input, err := owner.RecoveryInput(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRecoveryInput(&input)
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	report, err := attachexecution.DecodeRemovalInspection(input, request.Report, evidence)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := owner.PrepareRemovalInspection(ctx, view, report, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		if err := service.commitCompletion(ctx, view, guards, proof, result, at); err != nil {
			return nil, err
		}
		view, _, _, err = service.readView(ctx, authorization, request.Attempt, workexecution.Recovery)
		if err != nil {
			return nil, err
		}
	}
	terminal := view.Attempt.Attempt
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	if terminal.FinishedAt == nil || terminal.Result == nil || *terminal.Result != evidence || terminal.Outcome != outcome {
		return nil, errs.New(errs.KindStateConflict, "Attach inspection has no matching saved outcome")
	}
	raw, err := owner.LoadRemovalInspection(ctx, view)
	if err != nil {
		return nil, err
	}
	same := bytes.Equal(raw, request.Report)
	clear(raw)
	if !same {
		return nil, errs.New(errs.KindStateConflict, "Attach inspection differs from its saved evidence")
	}
	ack := &agentpb.AttachRecoveryCompletionAck{Attempt: proto.CloneOf(request.Attempt), Evidence: proto.CloneOf(request.Evidence),
		Outcome: string(terminal.Outcome), FinishedAt: timestamppb.New(*terminal.FinishedAt)}
	if terminal.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*terminal.StartedAt)
	}
	return ack, nil
}
