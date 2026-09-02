package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const executionStepResultPrefix = "/v1/records/execution-step-results/"

type ExecutionStepProcedureServiceImageResult struct {
	ServiceID          string `json:"service_id"`
	ReleaseID          string `json:"release_id"`
	RequestedReference string `json:"requested_reference"`
	ImmutableReference string `json:"immutable_reference"`
	ImageDigest        string `json:"image_digest"`
	LocalImageID       string `json:"local_image_id"`
}

type ExecutionStepResultRecord struct {
	OperationID           string                                    `json:"operation_id"`
	PlanHash              string                                    `json:"plan_hash"`
	StepID                string                                    `json:"step_id"`
	ControlPayloadSHA256  string                                    `json:"control_payload_sha256"`
	ProcedureServiceImage *ExecutionStepProcedureServiceImageResult `json:"procedure_service_image,omitempty"`
	AcknowledgedAt        time.Time                                 `json:"acknowledged_at"`
}

type ExecutionStepResultInput struct {
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	Result          *agentpb.ExecutionStepResult
	At              time.Time
}

type ExecutionStepResultWrite struct {
	Record    ExecutionStepResultRecord
	Revision  int64
	Duplicate bool
}

func (record ExecutionStepResultRecord) Proto() (*agentpb.ExecutionStepResult, error) {
	result := &agentpb.ExecutionStepResult{
		OperationId: record.OperationID,
		StepId:      record.StepID,
	}
	planHash, err := hex.DecodeString(record.PlanHash)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "execution step result plan hash is corrupt")
	}
	controlDigest, err := hex.DecodeString(record.ControlPayloadSHA256)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "execution step result control digest is corrupt")
	}
	result.PlanHash = planHash
	result.ControlPayloadSha256 = controlDigest
	if record.ProcedureServiceImage != nil {
		evidence := record.ProcedureServiceImage
		imageDigest, digestErr := hex.DecodeString(evidence.ImageDigest)
		if digestErr != nil {
			return nil, errs.New(errs.KindInternal, "execution step result image digest is corrupt")
		}
		result.Result = &agentpb.ExecutionStepResult_ProcedureServiceImage{
			ProcedureServiceImage: &agentpb.ProcedureServiceImageResult{
				ServiceId:          evidence.ServiceID,
				ReleaseId:          evidence.ReleaseID,
				RequestedReference: evidence.RequestedReference,
				ImmutableReference: evidence.ImmutableReference,
				ImageDigest:        imageDigest,
				LocalImageId:       evidence.LocalImageID,
			},
		}
	}
	if _, err := executionplan.ValidateExecutionStepResult(result); err != nil {
		return nil, errs.New(errs.KindInternal, "execution step result record is corrupt")
	}
	return result, nil
}

func newExecutionStepResultRecord(result *agentpb.ExecutionStepResult, at time.Time) (ExecutionStepResultRecord, error) {
	if _, err := executionplan.ValidateExecutionStepResult(result); err != nil {
		return ExecutionStepResultRecord{}, err
	}
	evidence := result.GetProcedureServiceImage()
	record := ExecutionStepResultRecord{
		OperationID:          result.GetOperationId(),
		PlanHash:             hex.EncodeToString(result.GetPlanHash()),
		StepID:               result.GetStepId(),
		ControlPayloadSHA256: hex.EncodeToString(result.GetControlPayloadSha256()),
		AcknowledgedAt:       at,
		ProcedureServiceImage: &ExecutionStepProcedureServiceImageResult{
			ServiceID:          evidence.GetServiceId(),
			ReleaseID:          evidence.GetReleaseId(),
			RequestedReference: evidence.GetRequestedReference(),
			ImmutableReference: evidence.GetImmutableReference(),
			ImageDigest:        hex.EncodeToString(evidence.GetImageDigest()),
			LocalImageID:       evidence.GetLocalImageId(),
		},
	}
	return record, nil
}

func (repository *TaskRepository) AcknowledgeExecutionStepResult(
	ctx context.Context,
	input ExecutionStepResultInput,
) (ExecutionStepResultWrite, error) {
	if err := validateContext(ctx); err != nil {
		return ExecutionStepResultWrite{}, err
	}
	record, err := newExecutionStepResultRecord(input.Result, input.At.UTC())
	if repository == nil || repository.store == nil || err != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, input.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, input.AgentID) != nil ||
		input.AgentGeneration == 0 || input.At.IsZero() {
		return ExecutionStepResultWrite{}, errs.New(errs.KindValidationFailed, "execution step result input is invalid")
	}
	key := executionStepResultKey(record.OperationID, record.PlanHash, record.StepID)
	for attempt := 0; attempt < 2; attempt++ {
		primary, readErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			key, taskKey(input.TaskID), taskAssignmentIndexKey(input.TaskID),
		}})
		if readErr != nil {
			return ExecutionStepResultWrite{}, readErr
		}
		if primary == nil || len(primary.Values) != 3 ||
			primary.Values[1] == nil || primary.Values[2] == nil {
			clearKeyValues(primary.Values)
			return ExecutionStepResultWrite{}, errs.New(errs.KindStateConflict, "execution step result assignment is unavailable")
		}
		task, taskErr := decodeTaskRecord(primary.Values[1].Value)
		assignment, assignmentErr := decodeTaskAssignment(primary.Values[2].Value)
		if taskErr != nil || assignmentErr != nil {
			clearKeyValues(primary.Values)
			return ExecutionStepResultWrite{}, errs.New(errs.KindInternal, "execution step result assignment is corrupt")
		}
		if task.ID != input.TaskID || task.OperationID != record.OperationID ||
			task.PlanHash != record.PlanHash || task.Status != TaskStatusRunning ||
			task.Executor != TaskExecutorAgent || !taskContainsStep(task, record.StepID) ||
			assignment.TaskID != input.TaskID || assignment.AssignmentID != input.AssignmentID ||
			assignment.Executor != TaskExecutorAgent || assignment.AgentID != input.AgentID ||
			assignment.AgentGeneration != input.AgentGeneration {
			clearKeyValues(primary.Values)
			return ExecutionStepResultWrite{}, errs.New(errs.KindStateConflict, "execution step result does not own the running assignment")
		}
		claimKey := taskExecutionClaimKey(TaskExecutorAgent, input.AgentID, input.TaskID)
		claim, claimErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{claimKey}, Revision: primary.ReadRevision,
		})
		if claimErr != nil {
			clearKeyValues(primary.Values)
			return ExecutionStepResultWrite{}, claimErr
		}
		if claim == nil || len(claim.Values) != 1 || claim.Values[0] == nil ||
			primary.Values[2].ModRevision != claim.Values[0].ModRevision ||
			!bytes.Equal(primary.Values[2].Value, claim.Values[0].Value) {
			clearKeyValues(primary.Values)
			clearKeyValues(claim.Values)
			return ExecutionStepResultWrite{}, errs.New(errs.KindStateConflict, "execution step result execution claim changed")
		}
		if primary.Values[0] != nil {
			durable, decodeErr := decodeExecutionStepResultRecord(primary.Values[0].Value)
			if decodeErr != nil {
				clearKeyValues(primary.Values)
				clearKeyValues(claim.Values)
				return ExecutionStepResultWrite{}, decodeErr
			}
			revision := primary.Values[0].ModRevision
			clearKeyValues(primary.Values)
			clearKeyValues(claim.Values)
			if !sameExecutionStepResult(durable, record) {
				return ExecutionStepResultWrite{}, errs.New(errs.KindStateConflict, "execution step result conflicts with durable evidence")
			}
			return ExecutionStepResultWrite{Record: durable, Revision: revision, Duplicate: true}, nil
		}
		value, encodeErr := encodeEnvelope("execution-step-result", record)
		if encodeErr != nil {
			clearKeyValues(primary.Values)
			clearKeyValues(claim.Values)
			return ExecutionStepResultWrite{}, encodeErr
		}
		transaction, transactionErr := repository.store.Transact(ctx, []Condition{
			{Key: key},
			{Key: taskKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
			{Key: taskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[2].ModRevision},
			{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
		}, []Mutation{{Type: MutationPut, Key: key, Value: value}})
		clear(value)
		clearKeyValues(primary.Values)
		clearKeyValues(claim.Values)
		clearKeyValues(transaction.FailureReads)
		if transactionErr != nil {
			return ExecutionStepResultWrite{}, transactionErr
		}
		if transaction.Succeeded {
			return ExecutionStepResultWrite{Record: record, Revision: transaction.Revision}, nil
		}
	}
	return ExecutionStepResultWrite{}, errs.New(errs.KindStateConflict, "execution step result changed concurrently")
}

func (repository *TaskRepository) ListExecutionStepResults(
	ctx context.Context,
	operationID string,
	planHash string,
) ([]ExecutionStepResultRecord, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if repository == nil || repository.store == nil ||
		ids.Validate(ids.KindOperation, operationID) != nil || !validSHA256Hex(planHash) {
		return nil, errs.New(errs.KindValidationFailed, "execution step result list request is invalid")
	}
	prefix := executionStepResultPlanPrefix(operationID, planHash)
	result, err := repository.store.Range(ctx, RangeRequest{Prefix: prefix, Limit: 257})
	if err != nil {
		return nil, err
	}
	if result == nil || result.More || len(result.Values) > 256 {
		clearExecutionStepResultValues(result.Values)
		return nil, errs.New(errs.KindInternal, "execution step result journal is corrupt")
	}
	defer clearExecutionStepResultValues(result.Values)
	records := make([]ExecutionStepResultRecord, 0, len(result.Values))
	for _, value := range result.Values {
		record, decodeErr := decodeExecutionStepResultRecord(value.Value)
		if decodeErr != nil || value.Key != executionStepResultKey(operationID, planHash, record.StepID) ||
			record.OperationID != operationID || record.PlanHash != planHash {
			return nil, errs.New(errs.KindInternal, "execution step result journal is corrupt")
		}
		records = append(records, record)
	}
	return records, nil
}

func decodeExecutionStepResultRecord(value []byte) (ExecutionStepResultRecord, error) {
	record, err := decodeEnvelope[ExecutionStepResultRecord](value, "execution-step-result")
	if err != nil || record.AcknowledgedAt.IsZero() {
		return ExecutionStepResultRecord{}, errs.New(errs.KindInternal, "execution step result record is corrupt")
	}
	if _, err := record.Proto(); err != nil {
		return ExecutionStepResultRecord{}, err
	}
	return record, nil
}

func sameExecutionStepResult(left, right ExecutionStepResultRecord) bool {
	left.AcknowledgedAt = time.Time{}
	right.AcknowledgedAt = time.Time{}
	if left.OperationID != right.OperationID || left.PlanHash != right.PlanHash ||
		left.StepID != right.StepID || left.ControlPayloadSHA256 != right.ControlPayloadSHA256 ||
		(left.ProcedureServiceImage == nil) != (right.ProcedureServiceImage == nil) {
		return false
	}
	if left.ProcedureServiceImage == nil {
		return true
	}
	return *left.ProcedureServiceImage == *right.ProcedureServiceImage
}

func executionStepResultPlanPrefix(operationID, planHash string) string {
	return executionStepResultPrefix + operationID + "/" + planHash + "/"
}

func executionStepResultKey(operationID, planHash, stepID string) string {
	return executionStepResultPlanPrefix(operationID, planHash) + stepID
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func clearExecutionStepResultValues(values []KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}
