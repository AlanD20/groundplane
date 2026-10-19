package serviceremoval

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

func (service *WorkReports) CompleteRemovalRecovery(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceRemovalRecoveryCompletionRequest,
) (*agentpb.ServiceRemovalRecoveryCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery completion evidence is missing")
	}
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.recoveryView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Checkpoint.Record == nil || view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal completion differs from its acknowledged restoration proof",
		)
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		captured, _, err := service.inputs.Load(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		input, err := BuildHostInput(captured)
		if err != nil {
			return nil, err
		}
		if err := service.records.SettleRecoveredServiceRemoval(ctx, view, input, guards, time.Now().UTC()); err != nil {
			return nil, err
		}
		view, err = service.recoveryView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.StartedAt == nil || attempt.Result == nil ||
		attempt.Outcome != workexecution.Succeeded || *attempt.Result != evidence {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal recovery completion differs from its stored outcome",
		)
	}
	return &agentpb.ServiceRemovalRecoveryCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: proto.CloneOf(request.Evidence), StartedAt: timestamppb.New(*attempt.StartedAt),
		FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
