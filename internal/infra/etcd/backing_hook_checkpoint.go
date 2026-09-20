package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackingHookCheckpointState string

const (
	BackingHookCheckpointStarted BackingHookCheckpointState = "started"
	BackingHookCheckpointResult  BackingHookCheckpointState = "result"
)

type BackingHookCheckpointInput struct {
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
	State           BackingHookCheckpointState
	ResultSHA256    string
	Facts           *AttachEncryptedFacts
	At              time.Time
}

type BackingHookCheckpointRecord struct {
	TaskID         string                     `json:"task_id"`
	OperationID    string                     `json:"operation_id"`
	AssignmentID   string                     `json:"assignment_id"`
	ExecutionEpoch uint32                     `json:"execution_epoch"`
	StepID         string                     `json:"step_id"`
	PlanHash       string                     `json:"plan_hash"`
	AttachID       string                     `json:"attach_id,omitempty"`
	Event          string                     `json:"event"`
	State          BackingHookCheckpointState `json:"state"`
	ResultSHA256   string                     `json:"result_sha256,omitempty"`
	Facts          *AttachEncryptedFacts      `json:"facts,omitempty"`
	StartedAt      time.Time                  `json:"started_at"`
	ResultAt       *time.Time                 `json:"result_at,omitempty"`
}

// CheckpointBackingHook fences one execution boundary against the exact live
// assignment. The durable record contains only ciphertext for hook results.
func (repository *AttachRepository) CheckpointBackingHook(
	ctx context.Context,
	input BackingHookCheckpointInput,
) (Versioned[BackingHookCheckpointRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BackingHookCheckpointRecord]{}, false, err
	}
	if repository == nil || repository.store == nil || validateBackingHookCheckpointInput(input) != nil {
		return Versioned[BackingHookCheckpointRecord]{}, false,
			errs.New(errs.KindValidationFailed, "Backing hook checkpoint input is invalid")
	}
	for attempt := 0; attempt < 2; attempt++ {
		anchor, err := repository.loadBackingHookCheckpointAnchor(ctx, input)
		if err != nil {
			return Versioned[BackingHookCheckpointRecord]{}, false, err
		}
		if anchor.current != nil {
			if sameBackingHookCheckpoint(*anchor.current, input) {
				return Versioned[BackingHookCheckpointRecord]{
					Record: *anchor.current, Revision: anchor.checkpointRevision, ReadRevision: anchor.readRevision,
				}, true, nil
			}
			if input.State != BackingHookCheckpointResult || anchor.current.State != BackingHookCheckpointStarted {
				return Versioned[BackingHookCheckpointRecord]{}, false,
					errs.New(errs.KindStateConflict, "Backing hook checkpoint already exists with different evidence")
			}
		}
		next, err := advanceBackingHookCheckpoint(anchor.current, input)
		if err != nil {
			return Versioned[BackingHookCheckpointRecord]{}, false, err
		}
		value, err := recordcodec.Encode("backing-hook-checkpoint", next)
		if err != nil {
			return Versioned[BackingHookCheckpointRecord]{}, false, err
		}
		transaction, err := repository.store.Transact(ctx, anchor.conditions, []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: backingHookCheckpointKey(input.TaskID, input.StepID), Value: value,
		}})
		clear(value)
		clearKeyValues(transaction.FailureReads)
		if err != nil {
			return Versioned[BackingHookCheckpointRecord]{}, false, err
		}
		if transaction.Succeeded {
			return Versioned[BackingHookCheckpointRecord]{
				Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			}, false, nil
		}
	}
	return Versioned[BackingHookCheckpointRecord]{}, false,
		errs.New(errs.KindStateConflict, "Backing hook checkpoint changed concurrently")
}

type backingHookCheckpointAnchor struct {
	current            *BackingHookCheckpointRecord
	checkpointRevision int64
	readRevision       int64
	conditions         []etcdstore.Condition
}

func (repository *AttachRepository) loadBackingHookCheckpointAnchor(
	ctx context.Context,
	input BackingHookCheckpointInput,
) (backingHookCheckpointAnchor, error) {
	checkpointKey := backingHookCheckpointKey(input.TaskID, input.StepID)
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskKey(input.TaskID), taskAssignmentIndexKey(input.TaskID), checkpointKey,
	}})
	if err != nil {
		return backingHookCheckpointAnchor{}, err
	}
	if primary == nil || len(primary.Values) != 3 || primary.Values[0] == nil || primary.Values[1] == nil {
		if primary != nil {
			clearKeyValues(primary.Values)
		}
		return backingHookCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Backing hook assignment is unavailable")
	}
	defer clearKeyValues(primary.Values)
	task, taskErr := decodeTaskRecord(primary.Values[0].Value)
	assignment, assignmentErr := decodeTaskAssignment(primary.Values[1].Value)
	if taskErr != nil || assignmentErr != nil {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook assignment is corrupt")
	}
	stepFound := false
	for _, step := range task.Steps {
		stepFound = stepFound || step.ID == input.StepID
	}
	if !stepFound || task.ID != input.TaskID || task.OperationID != input.OperationID ||
		task.Status != TaskStatusRunning || task.Executor != TaskExecutorAgent || task.PlanHash != input.PlanHash ||
		assignment.TaskID != input.TaskID || assignment.AssignmentID != input.AssignmentID ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration ||
		assignment.ExecutionEpoch != input.ExecutionEpoch || assignment.Executor != TaskExecutorAgent {
		return backingHookCheckpointAnchor{}, errs.New(
			errs.KindStateConflict,
			"Backing hook checkpoint does not own the running assignment",
		)
	}
	claimKey := taskExecutionClaimKey(TaskExecutorAgent, input.AgentID, input.TaskID)
	claim, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{claimKey}, Revision: primary.ReadRevision})
	if err != nil {
		return backingHookCheckpointAnchor{}, err
	}
	if claim == nil || len(claim.Values) != 1 || claim.Values[0] == nil {
		if claim != nil {
			clearKeyValues(claim.Values)
		}
		return backingHookCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Backing hook execution claim changed")
	}
	defer clearKeyValues(claim.Values)
	if claim.Values[0].ModRevision != primary.Values[1].ModRevision ||
		!bytes.Equal(claim.Values[0].Value, primary.Values[1].Value) {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook assignment copies diverged")
	}
	anchor := backingHookCheckpointAnchor{readRevision: primary.ReadRevision, conditions: []etcdstore.Condition{
		{Key: taskKey(input.TaskID), ModRevision: primary.Values[0].ModRevision},
		{Key: taskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
		{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
	}}
	if primary.Values[2] == nil {
		anchor.conditions = append(anchor.conditions, etcdstore.Condition{Key: checkpointKey, ModRevision: 0})
		return anchor, nil
	}
	record, err := recordcodec.Decode[BackingHookCheckpointRecord](
		primary.Values[2].Value,
		"backing-hook-checkpoint",
	)
	if err != nil || validateBackingHookCheckpointRecord(record) != nil {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook checkpoint is corrupt")
	}
	anchor.current = &record
	anchor.checkpointRevision = primary.Values[2].ModRevision
	anchor.conditions = append(anchor.conditions, etcdstore.Condition{Key: checkpointKey, ModRevision: primary.Values[2].ModRevision})
	return anchor, nil
}

func advanceBackingHookCheckpoint(
	current *BackingHookCheckpointRecord,
	input BackingHookCheckpointInput,
) (BackingHookCheckpointRecord, error) {
	if input.State == BackingHookCheckpointStarted {
		if current != nil {
			return BackingHookCheckpointRecord{}, errs.New(errs.KindStateConflict, "Backing hook STARTED checkpoint already exists")
		}
		return BackingHookCheckpointRecord{
			TaskID: input.TaskID, OperationID: input.OperationID, AssignmentID: input.AssignmentID,
			ExecutionEpoch: input.ExecutionEpoch, StepID: input.StepID, PlanHash: input.PlanHash,
			AttachID: input.AttachID, Event: input.Event, State: input.State, StartedAt: input.At.UTC(),
		}, nil
	}
	if current == nil || current.State != BackingHookCheckpointStarted ||
		!sameBackingHookCheckpointIdentity(*current, input) {
		return BackingHookCheckpointRecord{}, errs.New(errs.KindStateConflict, "Backing hook RESULT has no exact STARTED checkpoint")
	}
	next := *current
	next.State = BackingHookCheckpointResult
	next.ResultSHA256 = input.ResultSHA256
	next.Facts = cloneAttachEncryptedFacts(input.Facts)
	at := input.At.UTC()
	next.ResultAt = &at
	return next, nil
}

func validateBackingHookCheckpointInput(input BackingHookCheckpointInput) error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(ids.KindAssignment, input.AssignmentID) != nil || ids.Validate(ids.KindStep, input.StepID) != nil ||
		input.AgentID == "" || input.AgentGeneration == 0 || input.ExecutionEpoch == 0 ||
		len(input.PlanHash) != 64 || input.At.IsZero() || input.Event == "" {
		return errs.New(errs.KindValidationFailed, "Backing hook checkpoint identity is invalid")
	}
	if _, err := hex.DecodeString(input.PlanHash); err != nil {
		return errs.New(errs.KindValidationFailed, "Backing hook plan digest is invalid")
	}
	switch input.State {
	case BackingHookCheckpointStarted:
		if input.ResultSHA256 != "" || input.Facts != nil {
			return errs.New(errs.KindValidationFailed, "Backing hook STARTED checkpoint carries result data")
		}
	case BackingHookCheckpointResult:
		if len(input.ResultSHA256) != 64 {
			return errs.New(errs.KindValidationFailed, "Backing hook RESULT digest is invalid")
		}
		if input.Event == "attach" {
			if input.Facts == nil || validateAttachEncryptedFacts(*input.Facts) != nil || input.Facts.AttachID != input.AttachID {
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

func validateBackingHookCheckpointRecord(record BackingHookCheckpointRecord) error {
	input := BackingHookCheckpointInput{
		TaskID: record.TaskID, OperationID: record.OperationID, AssignmentID: record.AssignmentID,
		AgentID: "durable", AgentGeneration: 1, ExecutionEpoch: record.ExecutionEpoch,
		StepID: record.StepID, PlanHash: record.PlanHash, AttachID: record.AttachID,
		Event: record.Event, State: record.State, ResultSHA256: record.ResultSHA256,
		Facts: record.Facts, At: record.StartedAt,
	}
	if record.State == BackingHookCheckpointResult && record.ResultAt == nil ||
		record.State == BackingHookCheckpointStarted && record.ResultAt != nil {
		return errs.New(errs.KindValidationFailed, "Backing hook checkpoint timestamps are invalid")
	}
	return validateBackingHookCheckpointInput(input)
}

func sameBackingHookCheckpoint(record BackingHookCheckpointRecord, input BackingHookCheckpointInput) bool {
	return sameBackingHookCheckpointIdentity(record, input) && record.State == input.State &&
		record.ResultSHA256 == input.ResultSHA256 && (record.Facts == nil) == (input.Facts == nil)
}

func sameBackingHookCheckpointIdentity(record BackingHookCheckpointRecord, input BackingHookCheckpointInput) bool {
	return record.TaskID == input.TaskID && record.OperationID == input.OperationID &&
		record.AssignmentID == input.AssignmentID && record.ExecutionEpoch == input.ExecutionEpoch &&
		record.StepID == input.StepID && record.PlanHash == input.PlanHash &&
		record.AttachID == input.AttachID && record.Event == input.Event
}

func cloneAttachEncryptedFacts(value *AttachEncryptedFacts) *AttachEncryptedFacts {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Ciphertext = append([]byte(nil), value.Ciphertext...)
	return &clone
}

func backingHookCheckpointKey(taskID, stepID string) string {
	return backingHookCheckpointTaskPrefix(taskID) + stepID
}

func backingHookCheckpointTaskPrefix(taskID string) string {
	return "/v1/records/backing-hook-checkpoints/" + taskID + "/"
}
