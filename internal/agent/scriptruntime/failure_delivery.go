package scriptruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FailureExchange interface {
	FailScript(context.Context, *agentpb.ScriptWorkFailureRequest) (*agentpb.ScriptWorkFailureAck, error)
}

type WorkerExchange interface {
	Exchange
	RecoveryExchange
	FailureExchange
}

type ExecutionResult struct {
	Completion *agentpb.ScriptWorkCompletionAck
	Recovery   *agentpb.ScriptRecoveryCompletionAck
	Failure    *agentpb.ScriptWorkFailureAck
}

func (executor *Executor) Run(
	ctx context.Context,
	invocation Invocation,
	exchange WorkerExchange,
) (ExecutionResult, error) {
	defer invocation.Clear()
	if receipt, exists, err := executor.replayFailure(ctx, invocation.Attempt, exchange); exists || err != nil {
		return ExecutionResult{Failure: receipt}, err
	}
	ack, err := executor.Execute(ctx, invocation, exchange)
	if err == nil {
		return ExecutionResult{Completion: ack}, nil
	}
	failure, reportErr := executor.reportFailure(ctx, invocation.Attempt, exchange)
	if reportErr != nil {
		return ExecutionResult{}, errors.Join(err, reportErr)
	}
	return ExecutionResult{Failure: failure}, nil
}

func (executor *Executor) RunRecovery(
	ctx context.Context,
	input scriptexecution.RecoveryInput,
	exchange WorkerExchange,
	ownership *agentownership.Lease,
) (ExecutionResult, error) {
	if receipt, exists, err := executor.replayFailure(ctx, input.Attempt, exchange); exists || err != nil {
		return ExecutionResult{Failure: receipt}, err
	}
	ack, err := executor.ExecuteRecovery(ctx, input, exchange, ownership)
	if err == nil {
		return ExecutionResult{Recovery: ack}, nil
	}
	failure, reportErr := executor.reportFailure(ctx, input.Attempt, exchange)
	if reportErr != nil {
		return ExecutionResult{}, errors.Join(err, reportErr)
	}
	return ExecutionResult{Failure: failure}, nil
}

func (executor *Executor) replayFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	exchange FailureExchange,
) (*agentpb.ScriptWorkFailureAck, bool, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return nil, false, errs.New(errs.KindInternal, "Script failure delivery is not configured")
	}
	saved, err := executor.journal.LoadScriptFailure(ctx, attempt)
	if err != nil || saved == nil {
		return nil, false, err
	}
	if saved.Receipt == nil {
		ack, err := executor.reportFailure(ctx, attempt, exchange)
		return ack, true, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, true, err
	}
	digest, err := hex.DecodeString(saved.Receipt.Result.SHA256)
	if err != nil {
		return nil, true, errs.Wrap(errs.KindInternal, err)
	}
	return &agentpb.ScriptWorkFailureAck{
		Attempt:    identity,
		Evidence:   &agentpb.WorkInputReference{InputId: saved.Receipt.Result.ID, Sha256: digest},
		Outcome:    string(saved.Receipt.Outcome),
		FinishedAt: timestamppb.New(*saved.Receipt.FinishedAt),
	}, true, nil
}

func (executor *Executor) reportFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	exchange FailureExchange,
) (*agentpb.ScriptWorkFailureAck, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	settlement, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := executor.journal.PrepareScriptFailure(settlement, attempt); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	ack, err := exchange.FailScript(settlement, &agentpb.ScriptWorkFailureRequest{Attempt: identity})
	if err != nil {
		return nil, err
	}
	if _, err := executor.journal.AcknowledgeScriptFailure(settlement, attempt, ack); err != nil {
		return nil, err
	}
	return ack, nil
}
