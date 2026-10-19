package scriptwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Service) CompleteScript(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ScriptWorkCompletionRequest,
) (*agentpb.ScriptWorkCompletionAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.CheckpointEvidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script completion request is invalid")
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	attempt, err := service.Complete(
		ctx,
		authorization,
		claimed,
		request.CheckpointSequence,
		workexecution.Input{
			ID:     request.CheckpointEvidence.InputId,
			SHA256: hex.EncodeToString(request.CheckpointEvidence.Sha256),
		},
	)
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptWorkCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence, CheckpointEvidence: proto.CloneOf(request.CheckpointEvidence),
		Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}

func (service *Service) CompleteScriptRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ScriptRecoveryCompletionRequest,
) (*agentpb.ScriptRecoveryCompletionAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script recovery completion request is invalid")
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	attempt, err := service.CompleteRecovery(ctx, authorization, claimed, request.Proof,
		workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)})
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptRecoveryCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: proto.CloneOf(request.Evidence), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
