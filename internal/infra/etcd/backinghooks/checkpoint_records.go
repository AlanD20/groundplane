package backinghooks

import (
	"encoding/hex"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type CheckpointState string

const (
	CheckpointStarted CheckpointState = "started"
	CheckpointResult  CheckpointState = "result"
)

type CheckpointInput struct {
	TaskID          string
	OperationID     string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	ExecutionEpoch  uint32
	StepID          string
	PlanHash        string
	AttachID        string
	Event           string
	State           CheckpointState
	ResultSHA256    string
	Facts           *attachrecord.EncryptedFacts
	At              time.Time
}

type CheckpointRecord struct {
	TaskID         string                       `json:"task_id"`
	OperationID    string                       `json:"operation_id"`
	AssignmentID   string                       `json:"assignment_id"`
	ExecutionEpoch uint32                       `json:"execution_epoch"`
	StepID         string                       `json:"step_id"`
	PlanHash       string                       `json:"plan_hash"`
	AttachID       string                       `json:"attach_id,omitempty"`
	Event          string                       `json:"event"`
	State          CheckpointState              `json:"state"`
	ResultSHA256   string                       `json:"result_sha256,omitempty"`
	Facts          *attachrecord.EncryptedFacts `json:"facts,omitempty"`
	StartedAt      time.Time                    `json:"started_at"`
	ResultAt       *time.Time                   `json:"result_at,omitempty"`
}

func AdvanceCheckpoint(
	current *CheckpointRecord,
	input CheckpointInput,
) (CheckpointRecord, error) {
	if input.State == CheckpointStarted {
		if current != nil {
			return CheckpointRecord{}, errs.New(
				errs.KindStateConflict,
				"Backing hook STARTED checkpoint already exists",
			)
		}
		return CheckpointRecord{
			TaskID: input.TaskID, OperationID: input.OperationID, AssignmentID: input.AssignmentID,
			ExecutionEpoch: input.ExecutionEpoch, StepID: input.StepID, PlanHash: input.PlanHash,
			AttachID: input.AttachID, Event: input.Event, State: input.State, StartedAt: input.At.UTC(),
		}, nil
	}
	if current == nil || current.State != CheckpointStarted ||
		!sameBackingHookCheckpointIdentity(*current, input) {
		return CheckpointRecord{}, errs.New(
			errs.KindStateConflict,
			"Backing hook RESULT has no exact STARTED checkpoint",
		)
	}
	next := *current
	next.State = CheckpointResult
	next.ResultSHA256 = input.ResultSHA256
	next.Facts = cloneAttachEncryptedFacts(input.Facts)
	at := input.At.UTC()
	next.ResultAt = &at
	return next, nil
}

func ValidateCheckpointInput(input CheckpointInput) error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(
			ids.KindAssignment,
			input.AssignmentID,
		) != nil || ids.Validate(ids.KindStep, input.StepID) != nil ||
		input.AgentID == "" || input.AgentGeneration == 0 || input.ExecutionEpoch == 0 ||
		len(input.PlanHash) != 64 || input.At.IsZero() || input.Event == "" {
		return errs.New(errs.KindValidationFailed, "Backing hook checkpoint identity is invalid")
	}
	if _, err := hex.DecodeString(input.PlanHash); err != nil {
		return errs.New(errs.KindValidationFailed, "Backing hook plan digest is invalid")
	}
	switch input.State {
	case CheckpointStarted:
		if input.ResultSHA256 != "" || input.Facts != nil {
			return errs.New(errs.KindValidationFailed, "Backing hook STARTED checkpoint carries result data")
		}
	case CheckpointResult:
		if len(input.ResultSHA256) != 64 {
			return errs.New(errs.KindValidationFailed, "Backing hook RESULT digest is invalid")
		}
		if input.Event == "attach" {
			if input.Facts == nil || attachrecord.ValidateAttachEncryptedFacts(*input.Facts) != nil ||
				input.Facts.AttachID != input.AttachID {
				return errs.New(errs.KindValidationFailed, "Backing hook RESULT facts are invalid")
			}
		} else if input.Facts != nil {
			return errs.New(errs.KindValidationFailed, "Non-Attach backing hook cannot persist facts")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Backing hook checkpoint state is invalid")
	}
	return nil
}

func ValidateCheckpointRecord(record CheckpointRecord) error {
	input := CheckpointInput{
		TaskID: record.TaskID, OperationID: record.OperationID, AssignmentID: record.AssignmentID,
		AgentID: "durable", AgentGeneration: 1, ExecutionEpoch: record.ExecutionEpoch,
		StepID: record.StepID, PlanHash: record.PlanHash, AttachID: record.AttachID,
		Event: record.Event, State: record.State, ResultSHA256: record.ResultSHA256,
		Facts: record.Facts, At: record.StartedAt,
	}
	if record.State == CheckpointResult && record.ResultAt == nil ||
		record.State == CheckpointStarted && record.ResultAt != nil {
		return errs.New(errs.KindValidationFailed, "Backing hook checkpoint timestamps are invalid")
	}
	return ValidateCheckpointInput(input)
}

func SameCheckpoint(record CheckpointRecord, input CheckpointInput) bool {
	return sameBackingHookCheckpointIdentity(record, input) && record.State == input.State &&
		record.ResultSHA256 == input.ResultSHA256 && (record.Facts == nil) == (input.Facts == nil)
}

func sameBackingHookCheckpointIdentity(record CheckpointRecord, input CheckpointInput) bool {
	return record.TaskID == input.TaskID && record.OperationID == input.OperationID &&
		record.AssignmentID == input.AssignmentID && record.ExecutionEpoch == input.ExecutionEpoch &&
		record.StepID == input.StepID && record.PlanHash == input.PlanHash &&
		record.AttachID == input.AttachID && record.Event == input.Event
}

func cloneAttachEncryptedFacts(value *attachrecord.EncryptedFacts) *attachrecord.EncryptedFacts {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Ciphertext = append([]byte(nil), value.Ciphertext...)
	return &clone
}

func CheckpointKey(taskID, stepID string) string {
	return CheckpointTaskPrefix(taskID) + stepID
}

func CheckpointTaskPrefix(taskID string) string {
	return "/v1/records/backing-hook-checkpoints/" + taskID + "/"
}
