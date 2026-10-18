package releaseruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// HookExchange transfers ownership of response plaintext to the loader.
type HookExchange func(context.Context, *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error)

type ChannelHookLoader struct{ exchange HookExchange }

func NewHookLoader(exchange HookExchange) (*ChannelHookLoader, error) {
	if exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release hook exchange is required")
	}
	return &ChannelHookLoader{exchange: exchange}, nil
}

func (loader *ChannelHookLoader) LoadHook(ctx context.Context, input releaseexecution.Input, hook *agentpb.ReleaseWorkHook) (scriptruntime.Invocation, error) {
	var empty scriptruntime.Invocation
	if ctx == nil || loader == nil || loader.exchange == nil || hook == nil || hook.Input == nil {
		return empty, errs.New(errs.KindValidationFailed, "Release hook delivery input is invalid")
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return empty, err
	}
	delivery, err := loader.exchange(ctx, &agentpb.ReleaseHookRequest{Attempt: identity, ExecutionId: hook.ExecutionId})
	if err != nil {
		releaseexecution.ClearHookDelivery(delivery)
		return empty, err
	}
	invalid := func() (scriptruntime.Invocation, error) {
		releaseexecution.ClearHookDelivery(delivery)
		return empty, errs.New(errs.KindStateConflict, "Controller hook delivery differs from the assigned Release input")
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil {
		return invalid()
	}
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) || delivery.OperationId != input.Release.OperationId ||
		!proto.Equal(delivery.Input, hook.Input) || delivery.Run == nil || delivery.Run.ScriptExecutionId != hook.ExecutionId ||
		delivery.Run.ScriptId != hook.ScriptId || delivery.Checkpoint == nil || delivery.Checkpoint.ScriptExecutionId != hook.ExecutionId || delivery.Cleanup != nil {
		return invalid()
	}
	return scriptruntime.Invocation{Attempt: input.Attempt, OperationID: delivery.OperationId,
		Input: workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)},
		Run:   delivery.Run, Snapshot: delivery.Snapshot, Projection: delivery.Projection, Body: delivery.Body,
		Entries: delivery.Entries, Checkpoint: delivery.Checkpoint}, nil
}
