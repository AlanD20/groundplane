package taskcheckpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCheckpointRepository interface {
	CheckpointScriptExecution(
		context.Context,
		agentregistration.WorkAuthorization,
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
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ScriptCheckpointRequest,
) (*agentpb.ScriptCheckpointAck, error) {
	if ctx == nil || service == nil || service.repository == nil || service.now == nil {
		return nil, errs.New(errs.KindInternal, "Script checkpoint service is not configured")
	}
	identity := authorization.Identity()
	validated, err := executionplan.ValidateScriptCheckpointRequest(request)
	if err != nil {
		return nil, err
	}
	request = validated
	requestDigest, err := executionplan.ComputeScriptCheckpointPayloadDigest(request)
	if err != nil {
		return nil, err
	}
	if identity.Validate() != nil || identity.Executor != workexecution.Agent || request == nil ||
		request.GetRecordedAt() == nil || request.GetRecordedAt().CheckValid() != nil ||
		len(request.GetInputSha256()) != sha256.Size ||
		request.GetRecordedAt().AsTime().After(service.now().UTC()) {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint identity or recorded time is invalid")
	}
	expected, ok := scriptExecutionState(request.GetExpectedState())
	if !ok {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint expected state is invalid")
	}
	state, ok := scriptExecutionState(request.GetState())
	if !ok {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint state is invalid")
	}
	evidence, err := scriptCheckpointEvidence(request)
	if err != nil {
		return nil, err
	}
	input := scriptexecutions.ScriptCheckpointInput{
		TaskID: request.GetTaskId(), OperationID: request.GetOperationId(),
		WorkItemID: request.GetWorkItemId(), AttemptID: request.GetAttemptId(), Executor: identity,
		Sequence: request.GetSequence(), StageID: request.GetStageId(), ExecutionID: request.GetScriptExecutionId(),
		Input: workexecution.Input{
			ID:     request.GetInputId(),
			SHA256: hex.EncodeToString(request.GetInputSha256()),
		},
		ExpectedState: expected, State: state, Evidence: evidence, At: request.GetRecordedAt().AsTime().UTC(),
	}
	encoded, digest, err := scriptexecutions.EncodeCheckpointEvidence(input)
	evidenceDigest := sha256.Sum256(encoded)
	clear(encoded)
	if err != nil {
		return nil, err
	}
	input.PayloadSHA256 = digest
	evidenceID, err := scriptexecutions.CheckpointEvidenceID(
		input.TaskID,
		input.ExecutionID,
		input.AttemptID,
		input.Sequence,
	)
	if err != nil {
		return nil, err
	}
	if err := scriptexecutions.ValidateScriptCheckpointInput(input); err != nil {
		return nil, err
	}
	_, err = service.repository.CheckpointScriptExecution(ctx, authorization, input)
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptCheckpointAck{
		TaskId: request.GetTaskId(), OperationId: request.GetOperationId(),
		WorkItemId: request.GetWorkItemId(), AttemptId: request.GetAttemptId(),
		ScriptExecutionId: request.GetScriptExecutionId(), State: request.GetState(),
		Sequence: request.GetSequence(), StageId: request.GetStageId(),
		EvidenceSha256: evidenceDigest[:], RequestSha256: requestDigest,
		EvidenceId: evidenceID,
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

func scriptCheckpointEvidence(
	request *agentpb.ScriptCheckpointRequest,
) (scriptexecutions.ScriptCheckpointEvidence, error) {
	switch evidence := request.GetEvidence().(type) {
	case *agentpb.ScriptCheckpointRequest_StartAuthorized:
		if evidence.StartAuthorized == nil {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script start evidence is missing")
		}
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &scriptexecutions.ScriptStartAuthorizedEvidence{},
		}, nil
	case *agentpb.ScriptCheckpointRequest_BodyPrepared:
		value := evidence.BodyPrepared
		if value == nil {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script body evidence is missing")
		}
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceBodyPrepared,
			BodyPrepared: &scriptexecutions.ScriptBodyPreparedEvidence{
				BodySHA256: hex.EncodeToString(value.GetBodySha256()), UID: value.GetUid(), GID: value.GetGid(),
				Device: value.GetDevice(), Inode: value.GetInode(), Leaf: value.GetLeaf(),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_ContainerCreated:
		value := evidence.ContainerCreated
		if value == nil {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script container evidence is missing")
		}
		return scriptexecutions.ScriptCheckpointEvidence{
			Kind: scriptexecutions.ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &scriptexecutions.ScriptContainerCreatedEvidence{
				ContainerID:           value.GetContainerId(),
				OwnershipLabelsSHA256: hex.EncodeToString(value.GetOwnershipLabelsSha256()),
			},
		}, nil
	case *agentpb.ScriptCheckpointRequest_Outcome:
		value := evidence.Outcome
		if value == nil || value.GetObservedAt() == nil || value.GetObservedAt().CheckValid() != nil {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script outcome observation time is missing or invalid")
		}
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
		if value == nil {
			return scriptexecutions.ScriptCheckpointEvidence{}, errs.New(errs.KindValidationFailed, "Script cleanup evidence is missing")
		}
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
