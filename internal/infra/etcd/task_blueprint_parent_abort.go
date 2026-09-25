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

type blueprintParentAbortRequest struct {
	TaskID      string    `json:"task_id"`
	RequestedAt time.Time `json:"requested_at"`
}

func encodeBlueprintParentAbortRequest(request blueprintParentAbortRequest) ([]byte, error) {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		recordcodec.ValidateTimestamp("Blueprint parent abort", request.RequestedAt) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint parent abort request is invalid")
	}
	return recordcodec.Encode("blueprint-parent-abort", request)
}

func decodeBlueprintParentAbortRequest(value []byte) (blueprintParentAbortRequest, error) {
	request, err := recordcodec.Decode[blueprintParentAbortRequest](value, "blueprint-parent-abort")
	if err != nil || ids.Validate(ids.KindTask, request.TaskID) != nil ||
		recordcodec.ValidateTimestamp("Blueprint parent abort", request.RequestedAt) != nil {
		return blueprintParentAbortRequest{}, errs.New(errs.KindInternal, "Blueprint parent abort request is corrupt")
	}
	return request, nil
}

// RequestBlueprintParentAbort persists stop intent before the coordinator may
// cancel a child. Child and desired-plan publication compare this key absent
// in their own transactions, closing the pre-read/send race.
func (repository *TaskRepository) RequestBlueprintParentAbort(
	ctx context.Context, taskID string, at time.Time,
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
			request, err := decodeBlueprintParentAbortRequest(read.Values[2].Value)
			if err != nil || request.TaskID != taskID {
				return errs.New(errs.KindInternal, "Blueprint parent abort request changed")
			}
			return nil
		}
		if task.Status != taskjournal.TaskStatusRunning || read.Values[1] == nil {
			return errs.New(errs.KindTaskNotAbortable, "Blueprint parent is not running")
		}
		claimed, err := idempotency.DecodeTaskReference(read.Values[1].Value)
		if err != nil || claimed != taskID {
			return errs.New(errs.KindInternal, "Blueprint parent claim is inconsistent")
		}
		value, err := encodeBlueprintParentAbortRequest(blueprintParentAbortRequest{TaskID: taskID, RequestedAt: at})
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

func (repository *TaskRepository) BlueprintParentAbortRequested(
	ctx context.Context, taskID string,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return false, errs.New(errs.KindValidationFailed, "Blueprint parent id is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskjournal.BlueprintParentAbortKey(taskID)},
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Blueprint parent abort read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	request, err := decodeBlueprintParentAbortRequest(read.Values[0].Value)
	if err != nil || request.TaskID != taskID {
		return false, errs.New(errs.KindInternal, "Blueprint parent abort request is corrupt")
	}
	return true, nil
}

// AbortBlueprintParent terminalizes requested work only after every child
// claim is settled and unknown effects are accounted for. It never turns an
// abort request into proof that an Agent stopped.
func (repository *TaskRepository) AbortBlueprintParent(
	ctx context.Context, environmentID, taskID string, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	requested, err := repository.BlueprintParentAbortRequested(ctx, taskID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if !requested {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent abort was not requested")
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
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint parent has unsettled child effects")
		}
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.ParentTaskID == taskID && applied.Record.State == blueprintunits.Uncertain {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint parent has unknown child effects")
		}
	}
	return repository.terminalizeBlueprintParent(ctx, snapshot, taskID, taskjournal.TaskStatusAborted, at)
}
