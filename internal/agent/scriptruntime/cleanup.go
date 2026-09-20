package scriptruntime

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (runtime *Runtime) cleanupAndReturn(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
) (int32, error) {
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
		var body *scriptexecution.BodyEvidence
		if durable.BodyPrepared != nil {
			value := bodyEvidenceFromCheckpoint(durable.BodyPrepared)
			body = &value
		}
		var container *scriptexecution.ContainerEvidence
		if durable.ContainerCreated != nil {
			value := containerEvidenceFromCheckpoint(durable.ContainerCreated)
			container = &value
		}
		proof, err := runtime.engine.Cleanup(request, body, container)
		if err != nil {
			return 0, err
		}
		wire := newScriptCheckpointRequest(
			request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN,
		)
		wire.Evidence = &agentpb.ScriptCheckpointRequest_Cleanup{
			Cleanup: &agentpb.ScriptCleanupCheckpoint{
				ContainerId: proof.ContainerID, BodyDevice: proof.BodyDevice, BodyInode: proof.BodyInode,
				BodyLeaf: proof.BodyLeaf, ContainerAbsent: proof.ContainerAbsent, BodyAbsent: proof.BodyAbsent,
				ExecutionDirectoryAbsent: proof.ExecutionDirectoryAbsent,
			},
		}
		if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
			return 0, err
		}
		durable.State = wire.State
		durable.Cleanup = proto.Clone(wire.GetCleanup()).(*agentpb.ScriptCleanupCheckpoint)
	}
	return scriptDurableResult(durable)
}

func scriptFailureReason(
	ctx context.Context,
	state agentpb.ScriptExecutionState,
	err error,
) agentpb.ScriptOutcomeReason {
	if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		if state == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
			return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT
		}
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START
	}
	if state == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE
	}
	return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE
}

func scriptDurableResult(checkpoint *agentpb.ScriptExecutionCheckpoint) (int32, error) {
	if checkpoint == nil || checkpoint.Outcome == nil {
		return 0, errs.New(errs.KindInternal, "agent: Script outcome is missing")
	}
	outcome := checkpoint.Outcome
	switch outcome.Reason {
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT:
		exitCode := outcome.GetExitCode()
		if exitCode != 0 {
			return exitCode, errs.Newf(errs.KindStateConflict, "Script container exited with status %d", exitCode)
		}
		return 0, nil
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT,
		agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START:
		return 0, context.DeadlineExceeded
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT,
		agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START:
		return 0, context.Canceled
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE:
		return 0, errs.New(errs.KindStateConflict, "Script recovery invariant failed")
	default:
		return 0, errs.New(errs.KindInternal, "Script execution failed")
	}
}
