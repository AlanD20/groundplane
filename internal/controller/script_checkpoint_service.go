package controller

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCheckpointRepository interface {
	CheckpointScriptExecution(
		context.Context,
		etcd.ScriptCheckpointInput,
	) (etcd.Versioned[etcd.ScriptExecutionRecord], error)
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
	_, err = service.repository.CheckpointScriptExecution(ctx, etcd.ScriptCheckpointInput{
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

func scriptExecutionState(state agentpb.ScriptExecutionState) (etcd.ScriptExecutionState, bool) {
	switch state {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		return etcd.ScriptExecutionNotStarted, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
		return etcd.ScriptExecutionStartAuthorized, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		return etcd.ScriptExecutionBodyPrepared, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		return etcd.ScriptExecutionContainerCreated, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
		return etcd.ScriptExecutionOutcomeRecorded, true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		return etcd.ScriptExecutionCleanupProven, true
	default:
		return "", false
	}
}

func scriptCheckpointEvidence(request *agentpb.ScriptCheckpointRequest) (etcd.ScriptCheckpointEvidence, error) {
	switch evidence := request.GetEvidence().(type) {
	case *agentpb.ScriptCheckpointRequest_StartAuthorized:
		return etcd.ScriptCheckpointEvidence{
			Kind: etcd.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &etcd.ScriptStartAuthorizedEvidence{},
		}, nil
	case *agentpb.ScriptCheckpointRequest_BodyPrepared:
		value := evidence.BodyPrepared
		return etcd.ScriptCheckpointEvidence{
			Kind: etcd.ScriptCheckpointEvidenceBodyPrepared,
			BodyPrepared: &etcd.ScriptBodyPreparedEvidence{
				BodySHA256: hex.EncodeToString(value.GetBodySha256()), UID: value.GetUid(), GID: value.GetGid(),
				Device: value.GetDevice(), Inode: value.GetInode(), Leaf: value.GetLeaf(),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_ContainerCreated:
		value := evidence.ContainerCreated
		return etcd.ScriptCheckpointEvidence{
			Kind: etcd.ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &etcd.ScriptContainerCreatedEvidence{
				ContainerID:           value.GetContainerId(),
				OwnershipLabelsSHA256: hex.EncodeToString(value.GetOwnershipLabelsSha256()),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_Outcome:
		value := evidence.Outcome
		reason, ok := scriptOutcomeReason(value.GetReason())
		if !ok {
			return etcd.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script outcome reason is invalid")
		}
		var exitCode *int32
		if value.ExitCode != nil {
			owned := value.GetExitCode()
			exitCode = &owned
		}
		return etcd.ScriptCheckpointEvidence{
			Kind: etcd.ScriptCheckpointEvidenceOutcome,
			Outcome: &etcd.ScriptOutcomeEvidence{
				Reason: reason, ExitCode: exitCode, OutputTruncated: value.GetOutputTruncated(),
				ObservedAt: value.GetObservedAt().AsTime().UTC(),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_Cleanup:
		value := evidence.Cleanup
		return etcd.ScriptCheckpointEvidence{
			Kind: etcd.ScriptCheckpointEvidenceCleanup,
			Cleanup: &etcd.ScriptCleanupEvidence{
				ContainerID: value.GetContainerId(), BodyDevice: value.GetBodyDevice(), BodyInode: value.GetBodyInode(),
				BodyLeaf: value.GetBodyLeaf(), ContainerAbsent: value.GetContainerAbsent(), BodyAbsent: value.GetBodyAbsent(),
				ExecutionDirectoryAbsent: value.GetExecutionDirectoryAbsent(),
			},
		}, nil
	default:
		return etcd.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script checkpoint evidence is invalid")
	}
}

func scriptOutcomeReason(reason agentpb.ScriptOutcomeReason) (etcd.ScriptOutcomeReason, bool) {
	switch reason {
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT:
		return etcd.ScriptOutcomeNormalExit, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE:
		return etcd.ScriptOutcomeStartFailure, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE:
		return etcd.ScriptOutcomeRuntimeFailure, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT:
		return etcd.ScriptOutcomeTimeout, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT:
		return etcd.ScriptOutcomeAbort, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START:
		return etcd.ScriptOutcomeAbortBeforeStart, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START:
		return etcd.ScriptOutcomeExpiryBeforeStart, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE:
		return etcd.ScriptOutcomeNoServingRelease, true
	case agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE:
		return etcd.ScriptOutcomeRecoveryInvariantFailure, true
	default:
		return "", false
	}
}
