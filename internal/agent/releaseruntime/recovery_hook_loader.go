package releaseruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type RecoveryHook struct {
	Projection *agentpb.ScriptRunnerProjection
	Artifacts  *agentpb.ScriptCleanupArtifacts
}

func (loader *ChannelHookLoader) LoadRecoveryHook(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	hook *agentpb.ReleaseWorkHook,
) (*RecoveryHook, error) {
	if loader == nil || loader.exchange == nil || ctx == nil || input.Validate() != nil || hook == nil ||
		hook.Input == nil {
		return nil, errs.New(errs.KindStateConflict, "recovery hook ownership request is invalid")
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := loader.exchange(ctx, &agentpb.ReleaseHookRequest{Attempt: identity, ExecutionId: hook.ExecutionId})
	defer releaseexecution.ClearHookDelivery(delivery)
	if err != nil {
		return nil, err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil {
		return nil, errs.New(errs.KindStateConflict, "recovery hook ownership delivery is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, input.Input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Input.Attempt) ||
		delivery.OperationId != input.Input.Release.OperationId || !proto.Equal(delivery.Input, hook.Input) ||
		delivery.Run == nil || delivery.Run.ScriptExecutionId != hook.ExecutionId || delivery.Run.ScriptId != hook.ScriptId ||
		delivery.Run.EnvironmentId != input.Input.Release.EnvironmentId || delivery.Run.ServiceId != input.Input.Release.ServiceId ||
		delivery.Projection == nil || delivery.Body != nil || len(delivery.Entries) != 0 || delivery.Snapshot != nil || delivery.Checkpoint != nil || delivery.InterruptedCleanup != nil ||
		delivery.Cleanup == nil || delivery.Cleanup.BodyMetadata == nil ||
		delivery.Cleanup.BodyMetadata.ScriptExecutionId != hook.ExecutionId || delivery.Cleanup.BodyMetadata.ScriptId != hook.ScriptId {
		return nil, errs.New(errs.KindStateConflict, "recovery hook ownership differs from the captured Release")
	}
	return &RecoveryHook{
		Projection: proto.CloneOf(delivery.Projection),
		Artifacts:  proto.CloneOf(delivery.Cleanup),
	}, nil
}
