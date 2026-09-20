package agent

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (p *WorkerPool) executeBackingHookStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) error {
	procedure := step.GetBackingHookProcedure()
	definition, input, schema, err := executionplan.DecodeBackingHookProcedure(procedure)
	if err != nil {
		return err
	}
	started := newBackingHookCheckpointRequest(assignment, step, procedure.GetEvent())
	started.State = agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_STARTED
	ack, err := p.checkpointBackingHook(ctx, started)
	if err != nil {
		return err
	}
	if ack.GetExisting() {
		return errs.New(
			errs.KindStateConflict,
			"agent: backing hook execution already started; explicit Task retry is required",
		)
	}
	containerID, err := p.adapter.backingContainer(ctx, procedure.GetBackingServiceId())
	if err != nil {
		return err
	}
	output, err := ExecuteBackingHook(ctx, p.runner, containerID, definition, input, schema)
	if err != nil {
		return err
	}
	defer output.Clear()
	result := newBackingHookCheckpointRequest(assignment, step, procedure.GetEvent())
	defer clearBackingHookCheckpointRequest(result)
	result.State = agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_RESULT
	for _, fact := range output.Facts {
		result.Facts = append(result.Facts, &agentpb.BackingHookValue{
			Key: fact.Key, Value: append([]byte(nil), fact.Value...),
		})
	}
	result.ResultSha256, err = executionplan.ComputeBackingHookResultDigest(result.GetEvent(), result.GetFacts())
	if err != nil {
		return err
	}
	_, err = p.checkpointBackingHook(ctx, result)
	return err
}

func clearBackingHookCheckpointRequest(request *agentpb.BackingHookCheckpointRequest) {
	if request == nil {
		return
	}
	for _, fact := range request.GetFacts() {
		if fact != nil {
			clear(fact.Value)
			fact.Value = nil
		}
	}
	request.Facts = nil
}

func newBackingHookCheckpointRequest(
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	event agentpb.BackingHookEvent,
) *agentpb.BackingHookCheckpointRequest {
	return &agentpb.BackingHookCheckpointRequest{
		TaskId: assignment.TaskID, OperationId: assignment.OperationID, AssignmentId: assignment.AssignmentID,
		ExecutionEpoch: assignment.ExecutionEpoch, StepId: step.GetStepId(),
		PlanHash: append([]byte(nil), assignment.Plan.GetPlanHash()...), Event: event,
	}
}
