package scriptruntime

import (
	"context"
	"errors"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

type Runtime struct {
	engine scriptexecution.Engine
}

func New(engine scriptexecution.Engine) (*Runtime, error) {
	if engine == nil {
		return nil, errs.New(errs.KindInternal, "agent: Script execution engine is required")
	}
	return &Runtime{engine: engine}, nil
}

func (runtime *Runtime) ExecuteScript(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (int32, error) {
	request, durable, err := scriptExecutionRequest(ctx, assignment, step, checkpoint)
	if err != nil {
		return 0, err
	}
	defer clearScriptExecutionRequest(&request)
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return scriptDurableResult(durable)
	}

	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED {
		if ctx.Err() != nil || !time.Now().Before(assignment.Deadline) {
			reason := agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START
			if errors.Is(ctx.Err(), context.DeadlineExceeded) || !time.Now().Before(assignment.Deadline) {
				reason = agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START
			}
			if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
				return 0, err
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		if err := checkpointStart(checkpoint, request, durable); err != nil {
			return 0, err
		}
	}

	var body scriptexecution.BodyEvidence
	if durable.BodyPrepared != nil {
		body = bodyEvidenceFromCheckpoint(durable.BodyPrepared)
	}
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED ||
		durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED ||
		durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		prepared, prepareErr := runtime.engine.PrepareBody(ctx, request)
		if durable.BodyPrepared != nil && !sameBodyEvidence(prepared, body) {
			prepareErr = errors.Join(
				prepareErr,
				errs.New(errs.KindStateConflict, "agent: recovered Script body evidence differs"),
			)
		}
		if durable.BodyPrepared == nil && prepared.Device != 0 {
			if err := checkpointBody(checkpoint, request, durable, prepared); err != nil {
				return 0, err
			}
		}
		if prepareErr != nil {
			reason := scriptFailureReason(ctx, durable.State, prepareErr)
			if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
				return 0, errors.Join(prepareErr, err)
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		body = prepared
	}

	var containerEvidence scriptexecution.ContainerEvidence
	if durable.ContainerCreated != nil {
		containerEvidence = containerEvidenceFromCheckpoint(durable.ContainerCreated)
	}
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED {
		recovered, recoverErr := runtime.engine.RecoverContainer(ctx, request, body)
		if recoverErr != nil {
			if err := checkpointOutcome(
				checkpoint, request, durable,
				agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE, nil,
			); err != nil {
				return 0, errors.Join(recoverErr, err)
			}
			return runtime.cleanupAndReturn(checkpoint, request, durable)
		}
		if recovered.Found {
			containerEvidence = recovered.Evidence
		} else {
			containerEvidence, err = runtime.engine.CreateContainer(ctx, request, body)
			if err != nil {
				reason := scriptFailureReason(ctx, durable.State, err)
				if containerEvidence.ID != "" {
					if reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START {
						reason = agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT
					}
					if checkpointErr := checkpointContainer(checkpoint, request, durable, containerEvidence); checkpointErr != nil {
						return 0, errors.Join(err, checkpointErr)
					}
				}
				if checkpointErr := checkpointOutcome(checkpoint, request, durable, reason, nil); checkpointErr != nil {
					return 0, errors.Join(err, checkpointErr)
				}
				exitCode, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
				return exitCode, errors.Join(err, cleanupErr)
			}
		}
		if err := checkpointContainer(checkpoint, request, durable, containerEvidence); err != nil {
			return 0, err
		}
	}

	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED {
		result, runErr := runtime.engine.RunContainer(ctx, request, body, containerEvidence)
		if runErr != nil {
			reason := scriptFailureReason(ctx, durable.State, runErr)
			if checkpointErr := checkpointOutcome(checkpoint, request, durable, reason, nil); checkpointErr != nil {
				return result.ExitCode, errors.Join(runErr, checkpointErr)
			}
			exitCode, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
			return exitCode, errors.Join(runErr, cleanupErr)
		} else if err := checkpointOutcome(
			checkpoint, request, durable,
			agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT, &result.ExitCode,
		); err != nil {
			return result.ExitCode, err
		}
	}
	return runtime.cleanupAndReturn(checkpoint, request, durable)
}

// CompleteScriptWithoutStart records a release hook that cannot safely select
// a serving Release. It proves cleanup without authorizing body preparation or
// container creation.
func (runtime *Runtime) CompleteScriptWithoutStart(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	reason agentpb.ScriptOutcomeReason,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) error {
	if reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE &&
		reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE {
		return errs.New(errs.KindValidationFailed, "agent: Script no-start outcome reason is invalid")
	}
	request, durable, err := scriptExecutionRequest(ctx, assignment, step, checkpoint)
	if err != nil {
		return err
	}
	defer clearScriptExecutionRequest(&request)
	switch durable.State {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		if err := checkpointOutcome(checkpoint, request, durable, reason, nil); err != nil {
			return err
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		if durable.GetOutcome().GetReason() != reason {
			return errs.New(errs.KindStateConflict, "agent: recovered Script no-start outcome differs")
		}
		if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
			return nil
		}
	default:
		return errs.New(errs.KindStateConflict, "agent: Script already crossed its no-start boundary")
	}
	_, cleanupErr := runtime.cleanupAndReturn(checkpoint, request, durable)
	if durable.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return nil
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	return errs.New(errs.KindInternal, "agent: Script no-start cleanup was not proven")
}

func (runtime *Runtime) Close() error {
	if runtime == nil || runtime.engine == nil {
		return nil
	}
	return runtime.engine.Close()
}
