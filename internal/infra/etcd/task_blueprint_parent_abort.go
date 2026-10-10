package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintParentStopRequest struct {
	TaskID      string                 `json:"task_id"`
	RequestedAt time.Time              `json:"requested_at"`
	Outcome     taskjournal.TaskStatus `json:"outcome"`
}

func validBlueprintParentStopOutcome(outcome taskjournal.TaskStatus) bool {
	return outcome == taskjournal.TaskStatusAborted || outcome == taskjournal.TaskStatusFailed ||
		outcome == taskjournal.TaskStatusTimedOut
}

func encodeBlueprintParentStopRequest(request blueprintParentStopRequest) ([]byte, error) {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		!validBlueprintParentStopOutcome(request.Outcome) ||
		recordcodec.ValidateTimestamp("Blueprint parent abort", request.RequestedAt) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint parent abort request is invalid")
	}
	return recordcodec.Encode("blueprint-parent-stop", request)
}

func decodeBlueprintParentStopRequest(value []byte) (blueprintParentStopRequest, error) {
	request, err := recordcodec.Decode[blueprintParentStopRequest](value, "blueprint-parent-stop")
	if err != nil || ids.Validate(ids.KindTask, request.TaskID) != nil ||
		!validBlueprintParentStopOutcome(request.Outcome) ||
		recordcodec.ValidateTimestamp("Blueprint parent abort", request.RequestedAt) != nil {
		return blueprintParentStopRequest{}, errs.New(errs.KindInternal, "Blueprint parent stop request is corrupt")
	}
	return request, nil
}

// RequestBlueprintParentAbort persists stop intent before the coordinator may
// cancel a child. Child and desired-plan publication compare this key absent
// in their own transactions, closing the pre-read/send race.
func (repository *TaskRepository) RequestBlueprintParentAbort(
	ctx context.Context, taskID string, at time.Time,
) error {
	return repository.requestBlueprintParentStop(ctx, taskID, taskjournal.TaskStatusAborted, at)
}

// RequestBlueprintParentTimeout closes the same durable stop gate as Abort.
// The stored start time is checked again inside the atomic publication fence.
func (repository *TaskRepository) RequestBlueprintParentTimeout(
	ctx context.Context, taskID string, at time.Time,
) error {
	return repository.requestBlueprintParentStop(ctx, taskID, taskjournal.TaskStatusTimedOut, at)
}

func (repository *TaskRepository) requestBlueprintParentStop(
	ctx context.Context, taskID string, outcome taskjournal.TaskStatus, at time.Time,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, taskID) != nil ||
		recordcodec.ValidateTimestamp("Blueprint parent abort", at) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint parent abort request is invalid")
	}
	taskKey := taskjournal.TaskStorageKey(taskID)
	claimKey := taskjournal.BlueprintParentClaimKey(taskID)
	abortKey := taskjournal.BlueprintParentAbortKey(taskID)
	for attempt := 0; attempt < 8; attempt++ {
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{taskKey, claimKey, abortKey},
		})
		if err != nil {
			return err
		}
		if read == nil || len(read.Values) != 3 || read.Values[0] == nil {
			return errs.New(errs.KindTaskNotFound, "Blueprint parent Task was not found")
		}
		defer keyvalue.ClearValues(read.Values)
		task, err := DecodeTaskRecord(read.Values[0].Value)
		if err != nil || validateBlueprintParentClaimTask(task) != nil || task.ID != taskID {
			return errs.New(errs.KindValidationFailed, "Task is not a Blueprint parent")
		}
		if read.Values[2] != nil {
			request, err := decodeBlueprintParentStopRequest(read.Values[2].Value)
			if err != nil || request.TaskID != taskID {
				return errs.New(errs.KindInternal, "Blueprint parent abort request changed")
			}
			return nil
		}
		if task.Status != taskjournal.TaskStatusRunning || read.Values[1] == nil {
			return errs.New(errs.KindTaskNotAbortable, "Blueprint parent is not running")
		}
		if outcome == taskjournal.TaskStatusTimedOut &&
			(task.StartedAt == nil || int64(at.Sub(*task.StartedAt)/time.Second) < task.TimeoutSeconds) {
			return errs.New(errs.KindStateConflict, "Blueprint Apply deadline has not elapsed")
		}
		claimed, err := idempotency.DecodeTaskReference(read.Values[1].Value)
		if err != nil || claimed != taskID {
			return errs.New(errs.KindInternal, "Blueprint parent claim is inconsistent")
		}
		value, err := encodeBlueprintParentStopRequest(blueprintParentStopRequest{
			TaskID: taskID, RequestedAt: at, Outcome: outcome,
		})
		if err != nil {
			return err
		}
		result, err := repository.store.Transact(ctx, []keyvalue.Condition{
			{Key: taskKey, ModRevision: read.Values[0].ModRevision},
			{Key: claimKey, ModRevision: read.Values[1].ModRevision},
			{Key: abortKey},
		}, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: abortKey, Value: value}})
		clear(value)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Blueprint parent abort request raced repeatedly")
}

func (repository *TaskRepository) BlueprintParentStopRequested(
	ctx context.Context, taskID string,
) (taskjournal.TaskStatus, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return "", err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Blueprint parent id is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskjournal.BlueprintParentAbortKey(taskID)},
	})
	if err != nil {
		return "", err
	}
	if read == nil || len(read.Values) != 1 {
		return "", errs.New(errs.KindInternal, "Blueprint parent stop read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return "", nil
	}
	request, err := decodeBlueprintParentStopRequest(read.Values[0].Value)
	if err != nil || request.TaskID != taskID {
		return "", errs.New(errs.KindInternal, "Blueprint parent stop request is corrupt")
	}
	return request.Outcome, nil
}

// AbortBlueprintParent terminalizes requested work only after every child
// claim is settled and unknown effects are accounted for. It never turns an
// abort request into proof that an Agent stopped.
func (repository *TaskRepository) AbortBlueprintParent(
	ctx context.Context, environmentID, taskID string, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	return repository.finishStoppedBlueprintParent(ctx, environmentID, taskID, taskjournal.TaskStatusAborted, at)
}

func (repository *TaskRepository) TimeoutBlueprintParent(
	ctx context.Context, environmentID, taskID string, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	return repository.finishStoppedBlueprintParent(ctx, environmentID, taskID, taskjournal.TaskStatusTimedOut, at)
}

func (repository *TaskRepository) finishStoppedBlueprintParent(
	ctx context.Context, environmentID, taskID string, outcome taskjournal.TaskStatus, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	requested, err := repository.BlueprintParentStopRequested(ctx, taskID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if requested != outcome {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent stop outcome does not match the requested outcome",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	snapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID == taskID {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindResourceInUse,
				"Blueprint parent has unsettled child effects",
			)
		}
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.ParentTaskID == taskID && applied.Record.State == blueprintunits.Uncertain {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindResourceInUse,
				"Blueprint parent has unknown child effects",
			)
		}
	}
	return repository.terminalizeBlueprintParent(ctx, snapshot, taskID, outcome, at)
}
