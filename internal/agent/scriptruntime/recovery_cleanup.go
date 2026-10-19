package scriptruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CleanupHook removes only captured artifacts under the caller's exclusive
// recovery ownership. It does not convert a stopped hook into successful work.
func (runtime *Runtime) CleanupHook(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	hook *agentpb.ReleaseWorkHook,
	projection *agentpb.ScriptRunnerProjection,
	artifacts *agentpb.ScriptCleanupArtifacts,
) (scriptexecution.CleanupProof, error) {
	if err := runtime.validateRecoveryHook(ctx, input, report, hook, projection); err != nil {
		return scriptexecution.CleanupProof{}, err
	}
	if artifacts == nil || executionplan.RejectUnknown(artifacts) != nil || artifacts.BodyMetadata == nil ||
		artifacts.BodyMetadata.ScriptExecutionId != hook.ExecutionId || artifacts.BodyMetadata.ScriptId != hook.ScriptId {
		return scriptexecution.CleanupProof{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup metadata does not identify the captured hook",
		)
	}
	if !time.Now().Before(input.Input.Attempt.Deadline) {
		return scriptexecution.CleanupProof{}, context.DeadlineExceeded
	}
	var body *scriptexecution.BodyEvidence
	if artifacts.BodyPrepared != nil {
		value := bodyEvidenceFromCheckpoint(artifacts.BodyPrepared)
		body = &value
	}
	var container *scriptexecution.ContainerEvidence
	if artifacts.ContainerCreated != nil {
		value := containerEvidenceFromCheckpoint(artifacts.ContainerCreated)
		container = &value
	}
	return runtime.engine.Cleanup(scriptexecution.CleanupRequest{
		QuiescenceRequest: scriptexecution.QuiescenceRequest{
			TaskID: input.Forward.TaskID, WorkItemID: input.Forward.WorkItemID,
			OperationID: input.Input.Release.OperationId, ExecutionID: hook.ExecutionId, Projection: projection,
		},
		BodyMetadata: artifacts.BodyMetadata,
	}, body, container)
}
