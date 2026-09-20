package agent

import (
	"bytes"
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeRemovalCheckpoint func(context.Context, *agentpb.VolumeRemovalCheckpointRequest) (*agentpb.VolumeRemovalCheckpointAck, error)

func (runtime *EnvironmentDirectoryRuntime) executeVolumeRemoval(
	ctx context.Context, assignment taskassignment.Assignment, step *agentpb.ExecutionStep, checkpoint volumeRemovalCheckpoint,
) (environmentDirectoryStepResult, error) {
	if checkpoint == nil || runtime == nil || runtime.helper == nil || step.GetManagedVolumeDirectoryRemove() == nil {
		return environmentDirectoryStepResult{}, errs.New(
			errs.KindInternal,
			"agent: Volume removal checkpoint runtime is required",
		)
	}
	request := &agentpb.VolumeRemovalCheckpointRequest{
		RequestId: ids.NewULID(), TaskId: assignment.TaskID, OperationId: assignment.OperationID,
		AssignmentId: assignment.AssignmentID, StepId: step.StepId, PlanHash: assignment.Plan.PlanHash, ConsumersDetached: true,
	}
	var result environmentDirectoryStepResult
	for {
		ack, err := checkpoint(ctx, request)
		if err != nil {
			return result, err
		}
		if err := executionplan.ValidateVolumeRemovalCheckpointAck(ack, request); err != nil {
			return result, err
		}
		if ack.DirectoryAbsent {
			result.Complete, result.NextCursor = true, nil
			return result, nil
		}
		pending, err := removal.DecodePendingPath(ack.PendingPath)
		payload := step.GetManagedVolumeDirectoryRemove()
		if err != nil || pending.OperationID != assignment.OperationID || pending.VolumeID != payload.VolumeId ||
			pending.Key != payload.ComposeKey || !bytes.Equal(pending.IntentSHA256[:], payload.IntentSha256) {
			return result, errs.New(errs.KindStateConflict, "agent: Volume removal pending call changed")
		}
		response, err := runtime.helper.Execute(ctx, &agentpb.EnvironmentDirectoryHelperRequest{
			Schema: environmentDirectoryHelperSchema, TaskId: assignment.TaskID, OperationId: assignment.OperationID,
			AssignmentId: assignment.AssignmentID, StepId: step.StepId, Plan: assignment.Plan,
			TimeoutSeconds: min(
				uint32(30),
				remainingSeconds(ctx, step.TimeoutSeconds),
			), VolumeRemovalPendingPath: ack.PendingPath,
		})
		if err != nil {
			return result, err
		}
		if response == nil || response.Schema != environmentDirectoryHelperSchema || response.ExitCode != 0 {
			result.ExitCode, result.FailedStepID = 1, step.StepId
			return result, errs.New(errs.KindRequestFailed, "agent: Volume removal helper failed")
		}
		completion, err := removal.DecodeCompletion(response.VolumeRemovalCompletion)
		if err != nil || completion.OperationID != pending.OperationID ||
			completion.RequestOrdinal != pending.RequestOrdinal ||
			completion.RequestSHA256 != pending.RequestSHA256 ||
			completion.MutationCount != response.MutationCount ||
			completion.DirectoryAbsent != response.Complete ||
			!bytes.Equal(completion.NextCursor, response.NextCursor) ||
			response.FailedStepId != "" {
			return result, errs.New(errs.KindStateConflict, "agent: Volume removal helper completion changed")
		}
		// Never report completion until the Controller has acknowledged the
		// exact helper result. On failure the durable pending call stays retained.
		result = environmentDirectoryStepResult{MutationCount: completion.MutationCount,
			NextCursor: append(
				[]byte(nil),
				completion.NextCursor...), ResponseSHA256: append([]byte(nil), completion.ResponseSHA256[:]...)}
		request.RequestId, request.ConsumersDetached, request.Completion = ids.NewULID(), false, append(
			[]byte(nil),
			response.VolumeRemovalCompletion...)
	}
}
