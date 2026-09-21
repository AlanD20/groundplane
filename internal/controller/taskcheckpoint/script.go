package taskcheckpoint

import (
	"context"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCheckpointRepository interface {
	CheckpointScriptExecution(
		context.Context,
		scriptexecutions.ScriptCheckpointInput,
	) (etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord], error)
}

type ScriptCheckpointService struct {
	repository ScriptCheckpointRepository
	now        func() time.Time
}

func NewScriptCheckpointService(repository ScriptCheckpointRepository) (*ScriptCheckpointService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Script checkpoint repository is required")
	}
	return &ScriptCheckpointService{repository: repository, now: time.Now}, nil
}

func (service *ScriptCheckpointService) CheckpointScript(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	request *agentpb.ScriptCheckpointRequest,
) (*agentpb.ScriptCheckpointAck, error) {
	if ctx == nil || service == nil || service.repository == nil || service.now == nil {
		return nil, errs.New(errs.KindInternal, "Script checkpoint service is not configured")
	}
	validated, err := executionplan.ValidateScriptCheckpointRequest(request)
	if err != nil {
		return nil, err
	}
	expected, ok := scriptExecutionState(validated.GetExpectedState())
	if !ok {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint expected state is invalid")
	}
	state, ok := scriptExecutionState(validated.GetState())
	if !ok {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint state is invalid")
	}
	evidence, err := scriptCheckpointEvidence(validated)
	if err != nil {
		return nil, err
	}
	_, err = service.repository.CheckpointScriptExecution(ctx, scriptexecutions.ScriptCheckpointInput{
		TaskID: validated.GetTaskId(), OperationID: validated.GetOperationId(),
		AssignmentID: validated.GetAssignmentId(), AgentID: agentID, AgentGeneration: agentGeneration,
		StepID: validated.GetStepId(), ExecutionID: validated.GetScriptExecutionId(),
		PlanHash: hex.EncodeToString(validated.GetPlanHash()), ExpectedState: expected, State: state,
		PayloadSHA256: hex.EncodeToString(validated.GetControlPayloadSha256()), Evidence: evidence,
		At: service.now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptCheckpointAck{
		TaskId: validated.GetTaskId(), OperationId: validated.GetOperationId(),
		AssignmentId: validated.GetAssignmentId(), StepId: validated.GetStepId(),
		ScriptExecutionId: validated.GetScriptExecutionId(), State: validated.GetState(),
		ControlPayloadSha256: append([]byte(nil), validated.GetControlPayloadSha256()...),
	}, nil
}

func scriptExecutionState(state agentpb.ScriptExecutionState) (scriptexecutions.ScriptExecutionState, bool) {
	switch state {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		return scriptexecutions.ScriptExecutionNotStarted, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
		return scriptexecutions.ScriptExecutionStartAuthorized, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		return scriptexecutions.ScriptExecutionBodyPrepared, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		return scriptexecutions.ScriptExecutionContainerCreated, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
		return scriptexecutions.ScriptExecutionOutcomeRecorded, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		return scriptexecutions.ScriptExecutionCleanupProven, true
	default:
		return "", false
	}
}

func scriptCheckpointEvidence(request *agentpb.ScriptCheckpointRequest) (scriptexecutions.ScriptCheckpointEvidence, error) {
	switch evidence := request.GetEvidence().(type) {
	case *agentpb.ScriptCheckpointRequest_StartAuthorized:
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &scriptexecutions.ScriptStartAuthorizedEvidence{},
		}, nil
	case *agentpb.ScriptCheckpointRequest_BodyPrepared:
		value := evidence.BodyPrepared
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceBodyPrepared,
			BodyPrepared: &scriptexecutions.ScriptBodyPreparedEvidence{
				BodySHA256: hex.EncodeToString(value.GetBodySha256()), UID: value.GetUid(), GID: value.GetGid(),
				Device: value.GetDevice(), Inode: value.GetInode(), Leaf: value.GetLeaf(),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_ContainerCreated:
		value := evidence.ContainerCreated
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &scriptexecutions.ScriptContainerCreatedEvidence{
				ContainerID:           value.GetContainerId(),
				OwnershipLabelsSHA256: hex.EncodeToString(value.GetOwnershipLabelsSha256()),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_Outcome:
		value := evidence.Outcome
		reason, ok := scriptOutcomeReason(value.GetReason())
		if !ok {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script outcome reason is invalid")
		}
		var exitCode *int32
		if value.ExitCode != nil {
			owned := value.GetExitCode()
			exitCode = &owned
		}
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceOutcome,
			Outcome: &scriptexecutions.ScriptOutcomeEvidence{
				Reason: reason, ExitCode: exitCode, OutputTruncated: value.GetOutputTruncated(),
				ObservedAt: value.GetObservedAt().AsTime().UTC(),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_Cleanup:
		value := evidence.Cleanup
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceCleanup,
			Cleanup: &scriptexecutions.ScriptCleanupEvidence{
				ContainerID: value.GetContainerId(), BodyDevice: value.GetBodyDevice(), BodyInode: value.GetBodyInode(),
				BodyLeaf: value.GetBodyLeaf(), ContainerAbsent: value.GetContainerAbsent(), BodyAbsent: value.GetBodyAbsent(),
				ExecutionDirectoryAbsent: value.GetExecutionDirectoryAbsent(),
			},
		}, nil
	default:
		return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script checkpoint evidence is invalid")
	}
}

func scriptOutcomeReason(reason agentpb.ScriptOutcomeReason) (scriptexecutions.ScriptOutcomeReason, bool) {
	switch reason {
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT:
		return scriptexecutions.ScriptOutcomeNormalExit, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE:
		return scriptexecutions.ScriptOutcomeStartFailure, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE:
		return scriptexecutions.ScriptOutcomeRuntimeFailure, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT:
		return scriptexecutions.ScriptOutcomeTimeout, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT:
		return scriptexecutions.ScriptOutcomeAbort, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START:
		return scriptexecutions.ScriptOutcomeAbortBeforeStart, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START:
		return scriptexecutions.ScriptOutcomeExpiryBeforeStart, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE:
		return scriptexecutions.ScriptOutcomeNoServingRelease, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE:
		return scriptexecutions.ScriptOutcomeRecoveryInvariantFailure, true
	default:
		return "", false
	}
}
