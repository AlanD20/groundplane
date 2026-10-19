package releaseruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A captured hook is either executable input or an earlier attempt's proven
// cleanup. An interrupted command never becomes a fresh Script invocation.
type FailureHookDelivery struct {
	Invocation         *scriptruntime.Invocation
	InterruptedCleanup *workexecution.Input
}

func (loader *ChannelHookLoader) LoadFailureHook(
	ctx context.Context,
	input releaseexecution.Input,
	hook *agentpb.ReleaseWorkHook,
) (FailureHookDelivery, error) {
	delivery, err := loader.loadHookDelivery(ctx, input, hook)
	if err != nil {
		return FailureHookDelivery{}, err
	}
	if delivery.InterruptedCleanup == nil {
		invocation, err := decodeHookInvocation(input, hook, delivery)
		if err != nil {
			return FailureHookDelivery{}, err
		}
		return FailureHookDelivery{Invocation: &invocation}, nil
	}
	defer releaseexecution.ClearHookDelivery(delivery)
	proof := delivery.InterruptedCleanup
	reference := workexecution.Input{ID: proof.InputId, SHA256: hex.EncodeToString(proof.Sha256)}
	if reference.Validate() != nil || delivery.Body != nil || len(delivery.Entries) != 0 || delivery.Snapshot != nil ||
		delivery.Checkpoint != nil ||
		delivery.Projection == nil ||
		delivery.Cleanup == nil ||
		delivery.Cleanup.BodyMetadata == nil ||
		delivery.Cleanup.BodyMetadata.ScriptExecutionId != hook.ExecutionId ||
		delivery.Cleanup.BodyMetadata.ScriptId != hook.ScriptId {
		return FailureHookDelivery{}, errs.New(
			errs.KindStateConflict,
			"interrupted failure hook has invalid cleanup-only delivery",
		)
	}
	return FailureHookDelivery{InterruptedCleanup: &reference}, nil
}
