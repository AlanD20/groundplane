package scriptruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RecoveryExchange interface {
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	CompleteScriptRecovery(
		context.Context,
		*agentpb.ScriptRecoveryCompletionRequest,
	) (*agentpb.ScriptRecoveryCompletionAck, error)
}

func (executor *Executor) ExecuteRecovery(
	ctx context.Context,
	input scriptexecution.RecoveryInput,
	exchange RecoveryExchange,
	ownership *agentownership.Lease,
) (*agentpb.ScriptRecoveryCompletionAck, error) {
	if executor == nil || ctx == nil || exchange == nil || input.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Script recovery requires a captured assignment")
	}
	saved, record, err := executor.journal.LoadScriptRecovery(ctx, input.Attempt)
	if err != nil {
		return nil, err
	}
	if record != nil {
		incoming, err := scriptexecution.EncodeRecoveryAssignment(input)
		if err != nil {
			return nil, err
		}
		captured, err := scriptexecution.EncodeRecoveryAssignment(saved)
		if err != nil {
			return nil, err
		}
		if !proto.Equal(incoming, captured) {
			return nil, errs.New(errs.KindStateConflict, "Script recovery delivery differs from its journal")
		}
		input = saved
		if record.Proof != nil {
			if record.Receipt == nil {
				return executor.completeRecovery(ctx, input, *record.Proof, exchange)
			}
			request, err := scriptexecution.EncodeRecoveryCompletion(input, *record.Proof)
			if err != nil {
				return nil, err
			}
			return &agentpb.ScriptRecoveryCompletionAck{Attempt: request.Attempt, Evidence: request.Evidence,
				FinishedAt: timestamppb.New(*record.Receipt.FinishedAt)}, nil
		}
	}
	claimed, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	control, cancel := context.WithTimeout(ctx, 30*time.Second)
	start, err := exchange.Start(control, &agentpb.WorkStartRequest{Attempt: claimed})
	cancel()
	if err != nil {
		return nil, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(start, input.Attempt)
	if err != nil {
		return nil, err
	}
	if start.CheckpointSequence != 0 || input.Attempt.StartedAt != nil && !input.Attempt.StartedAt.Equal(started) {
		return nil, errs.New(errs.KindStateConflict, "Script recovery Start differs from its captured journal")
	}
	input.Attempt.StartedAt = &started
	if _, err := executor.journal.BeginScriptRecovery(ctx, input); err != nil {
		return nil, err
	}
	proof, err := executor.runtime.RecoverStandalone(ctx, input, ownership)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.SaveScriptRecoveryProof(ctx, input.Attempt, proof); err != nil {
		return nil, err
	}
	return executor.completeRecovery(ctx, input, proof, exchange)
}

func (executor *Executor) completeRecovery(
	ctx context.Context,
	input scriptexecution.RecoveryInput,
	proof scriptexecution.RecoveryProof,
	exchange RecoveryExchange,
) (*agentpb.ScriptRecoveryCompletionAck, error) {
	request, err := scriptexecution.EncodeRecoveryCompletion(input, proof)
	if err != nil {
		return nil, err
	}
	settlement, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ack, err := exchange.CompleteScriptRecovery(settlement, request)
	if err != nil {
		return nil, err
	}
	if _, err := executor.journal.AcknowledgeScriptRecovery(settlement, input.Attempt, ack); err != nil {
		return nil, err
	}
	return ack, nil
}
