package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

func ValidateScriptCheckpointRequest(
	request *agentpb.ScriptCheckpointRequest,
) (*agentpb.ScriptCheckpointRequest, error) {
	if request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint request is required")
	}
	owned := proto.Clone(request).(*agentpb.ScriptCheckpointRequest)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, owned.TaskId) != nil || ids.Validate(ids.KindOperation, owned.OperationId) != nil ||
		ids.Validate(
			ids.KindAttempt,
			owned.AttemptId,
		) != nil || ids.Validate(ids.KindWorkItem, owned.WorkItemId) != nil ||
		!validScriptCheckpointExecutionID(owned.ScriptExecutionId) || len(owned.InputSha256) != sha256.Size ||
		ids.Validate(
			ids.KindPlan,
			owned.InputId,
		) != nil || owned.Sequence == 0 || ids.Validate(ids.KindStep, owned.StageId) != nil ||
		owned.RecordedAt == nil || owned.RecordedAt.CheckValid() != nil || owned.RecordedAt.AsTime().IsZero() {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint delivery identity is invalid")
	}
	if err := validateScriptCheckpointTransition(owned); err != nil {
		return nil, err
	}
	return owned, nil
}

func ComputeScriptCheckpointPayloadDigest(request *agentpb.ScriptCheckpointRequest) ([]byte, error) {
	if request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint request is required")
	}
	if err := RejectUnknown(request); err != nil {
		return nil, err
	}
	if err := validateScriptCheckpointTransition(request); err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return append([]byte(nil), digest[:]...), nil
}

func ValidateScriptCheckpointAck(
	ack *agentpb.ScriptCheckpointAck,
	request *agentpb.ScriptCheckpointRequest,
) (*agentpb.ScriptCheckpointAck, error) {
	if ack == nil || request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint acknowledgement is invalid")
	}
	owned := proto.Clone(ack).(*agentpb.ScriptCheckpointAck)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	digest, err := ComputeScriptCheckpointPayloadDigest(request)
	if err != nil {
		return nil, err
	}
	if owned.TaskId != request.TaskId || owned.OperationId != request.OperationId ||
		owned.AttemptId != request.AttemptId || owned.WorkItemId != request.WorkItemId ||
		owned.Sequence != request.Sequence || owned.StageId != request.StageId ||
		owned.ScriptExecutionId != request.ScriptExecutionId || owned.State != request.State ||
		ids.Validate(ids.KindPlan, owned.EvidenceId) != nil ||
		len(owned.EvidenceSha256) != sha256.Size || len(owned.RequestSha256) != sha256.Size ||
		subtle.ConstantTimeCompare(owned.RequestSha256, digest) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint acknowledgement identity is invalid")
	}
	return owned, nil
}

func ValidateScriptExecutionCheckpoint(
	checkpoint *agentpb.ScriptExecutionCheckpoint,
) (*agentpb.ScriptExecutionCheckpoint, error) {
	if checkpoint == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script execution checkpoint is required")
	}
	owned := proto.Clone(checkpoint).(*agentpb.ScriptExecutionCheckpoint)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if !validScriptCheckpointExecutionID(owned.ScriptExecutionId) || !validScriptCheckpointState(owned.State) ||
		(owned.BodyPrepared != nil && !owned.StartAuthorized) ||
		(owned.ContainerCreated != nil && owned.BodyPrepared == nil) ||
		(owned.Cleanup != nil && owned.Outcome == nil) {
		return nil, errs.New(errs.KindValidationFailed, "Script execution checkpoint shape is invalid")
	}
	if owned.BodyPrepared != nil && (len(owned.BodyPrepared.BodySha256) != sha256.Size ||
		owned.BodyPrepared.Device == 0 || owned.BodyPrepared.Inode == 0 || owned.BodyPrepared.Leaf != "body") {
		return nil, errs.New(errs.KindValidationFailed, "Script body checkpoint is invalid")
	}
	if owned.ContainerCreated != nil && (!validScriptCheckpointContainerID(owned.ContainerCreated.ContainerId) ||
		len(owned.ContainerCreated.OwnershipLabelsSha256) != sha256.Size) {
		return nil, errs.New(errs.KindValidationFailed, "Script container checkpoint is invalid")
	}
	if owned.Outcome != nil && !validScriptOutcomeForCheckpoint(owned) {
		return nil, errs.New(errs.KindValidationFailed, "Script outcome checkpoint is invalid")
	}
	if owned.Cleanup != nil && !validScriptCleanupForCheckpoint(owned) {
		return nil, errs.New(errs.KindValidationFailed, "Script cleanup checkpoint is invalid")
	}
	switch owned.State {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		if owned.StartAuthorized || owned.BodyPrepared != nil || owned.ContainerCreated != nil ||
			owned.Outcome != nil ||
			owned.Cleanup != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script not-started checkpoint is invalid")
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
		if !owned.StartAuthorized || owned.BodyPrepared != nil || owned.ContainerCreated != nil ||
			owned.Outcome != nil ||
			owned.Cleanup != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script start checkpoint is invalid")
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		if !owned.StartAuthorized || owned.BodyPrepared == nil || owned.ContainerCreated != nil ||
			owned.Outcome != nil ||
			owned.Cleanup != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script body-prepared checkpoint is invalid")
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		if !owned.StartAuthorized || owned.BodyPrepared == nil || owned.ContainerCreated == nil ||
			owned.Outcome != nil ||
			owned.Cleanup != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script container-created checkpoint is invalid")
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
		if owned.Outcome == nil || owned.Cleanup != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script outcome-recorded checkpoint is invalid")
		}
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		if owned.Outcome == nil || owned.Cleanup == nil {
			return nil, errs.New(errs.KindValidationFailed, "Script cleanup-proven checkpoint is invalid")
		}
	}
	return owned, nil
}

func validScriptOutcomeForCheckpoint(checkpoint *agentpb.ScriptExecutionCheckpoint) bool {
	outcome := checkpoint.Outcome
	if outcome == nil || outcome.ObservedAt == nil || outcome.ObservedAt.CheckValid() != nil ||
		outcome.ObservedAt.AsTime().IsZero() {
		return false
	}
	from := agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED
	if checkpoint.ContainerCreated != nil {
		from = agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED
	} else if checkpoint.BodyPrepared != nil {
		from = agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED
	} else if checkpoint.StartAuthorized {
		from = agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED
	}
	return validScriptOutcome(from, outcome)
}

func validScriptCleanupForCheckpoint(checkpoint *agentpb.ScriptExecutionCheckpoint) bool {
	cleanup := checkpoint.Cleanup
	if cleanup == nil || !cleanup.ContainerAbsent || !cleanup.BodyAbsent || !cleanup.ExecutionDirectoryAbsent ||
		(cleanup.ContainerId != "" && !validScriptCheckpointContainerID(cleanup.ContainerId)) {
		return false
	}
	if checkpoint.ContainerCreated == nil {
		if cleanup.ContainerId != "" {
			return false
		}
	} else if cleanup.ContainerId != checkpoint.ContainerCreated.ContainerId {
		return false
	}
	if checkpoint.BodyPrepared == nil {
		return cleanup.BodyDevice == 0 && cleanup.BodyInode == 0 && cleanup.BodyLeaf == ""
	}
	return cleanup.BodyDevice == checkpoint.BodyPrepared.Device && cleanup.BodyInode == checkpoint.BodyPrepared.Inode &&
		cleanup.BodyLeaf == checkpoint.BodyPrepared.Leaf
}

func validateScriptCheckpointTransition(request *agentpb.ScriptCheckpointRequest) error {
	if !validScriptCheckpointState(request.ExpectedState) || !validScriptCheckpointState(request.State) ||
		!validScriptCheckpointStateTransition(request.ExpectedState, request.State) {
		return errs.New(errs.KindValidationFailed, "Script checkpoint state transition is invalid")
	}
	valid := false
	switch evidence := request.Evidence.(type) {
	case *agentpb.ScriptCheckpointRequest_StartAuthorized:
		valid = request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED &&
			evidence.StartAuthorized != nil
	case *agentpb.ScriptCheckpointRequest_BodyPrepared:
		value := evidence.BodyPrepared
		valid = request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED && value != nil &&
			len(value.BodySha256) == sha256.Size && value.Device > 0 && value.Inode > 0 && value.Leaf == "body"
	case *agentpb.ScriptCheckpointRequest_ContainerCreated:
		value := evidence.ContainerCreated
		valid = request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED && value != nil &&
			validScriptCheckpointContainerID(value.ContainerId) && len(value.OwnershipLabelsSha256) == sha256.Size
	case *agentpb.ScriptCheckpointRequest_Outcome:
		valid = request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED &&
			validScriptOutcome(request.ExpectedState, evidence.Outcome)
	case *agentpb.ScriptCheckpointRequest_Cleanup:
		value := evidence.Cleanup
		valid = request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN && value != nil &&
			(value.ContainerId == "" || validScriptCheckpointContainerID(value.ContainerId)) &&
			((value.BodyDevice == 0 && value.BodyInode == 0 && value.BodyLeaf == "") ||
				(value.BodyDevice > 0 && value.BodyInode > 0 && value.BodyLeaf == "body")) &&
			value.ContainerAbsent && value.BodyAbsent && value.ExecutionDirectoryAbsent
	}
	if !valid {
		return errs.New(errs.KindValidationFailed, "Script checkpoint evidence is invalid")
	}
	return nil
}

func validScriptCheckpointState(value agentpb.ScriptExecutionState) bool {
	return value >= agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED &&
		value <= agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN
}

func validScriptCheckpointStateTransition(from, to agentpb.ScriptExecutionState) bool {
	if from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED &&
		(to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED ||
			to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED) {
		return true
	}
	if from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED &&
		(to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED ||
			to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED) {
		return true
	}
	if from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED &&
		(to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED ||
			to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED) {
		return true
	}
	return from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED &&
		to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED ||
		from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED &&
			to == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN
}

func validScriptOutcome(from agentpb.ScriptExecutionState, outcome *agentpb.ScriptOutcomeCheckpoint) bool {
	if outcome == nil || outcome.ObservedAt == nil || outcome.ObservedAt.CheckValid() != nil ||
		outcome.ObservedAt.AsTime().IsZero() {
		return false
	}
	if outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT {
		return from == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED &&
			outcome.ExitCode != nil && outcome.GetExitCode() >= 0
	}
	if outcome.ExitCode != nil {
		return false
	}
	switch from {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED:
		return outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		return outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		return outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT ||
			outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	default:
		return false
	}
}

func validScriptCheckpointExecutionID(value string) bool {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return false
	}
	_, err := ulid.ParseStrict(value)
	return err == nil
}

func validScriptCheckpointContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
