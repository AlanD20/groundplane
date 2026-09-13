package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func cloneRetryTask(source TaskRecord, id string, actor TaskActor, createdAt time.Time) (TaskRecord, error) {
	if err := validateTaskRecord(source); err != nil {
		return TaskRecord{}, err
	}
	if source.Params[TaskResourceKindParam] == TaskResourceController {
		return TaskRecord{}, errs.New(
			errs.KindTaskNotRetryable,
			"Controller update requires a fresh explicit release selection",
		)
	}
	if source.Status != TaskStatusFailed && source.Status != TaskStatusTimedOut && source.Status != TaskStatusAborted {
		return TaskRecord{}, errs.Newf(
			errs.KindTaskNotRetryable,
			"task %s has status %s",
			source.ID,
			source.Status,
		)
	}
	if err := validateStableID(ids.KindTask, id); err != nil {
		return TaskRecord{}, err
	}
	if id == source.ID {
		return TaskRecord{}, errs.New(errs.KindValidationFailed, "a retry requires a new task id")
	}

	retry := TaskRecord{
		ID: id, OperationID: source.OperationID, RetryOf: source.ID,
		IdempotencyKey: source.IdempotencyKey, Owner: source.Owner, Actor: actor,
		Executor: source.Executor, PlanID: source.PlanID,
		PlanHash: source.PlanHash, RenderGeneration: source.RenderGeneration,
		Type: source.Type, Target: source.Target, Params: cloneStringMap(source.Params),
		Steps: cloneTaskSteps(source.Steps), TimeoutSeconds: source.TimeoutSeconds,
		ComponentActionStepIDs:          append([]string(nil), source.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: cloneManagedComponentRuntimeSources(source.ManagedComponentTeardownSources),
		Materializations:                cloneTaskMaterializationReferences(source.Materializations),
		EntryRuntime:                    cloneEntryTaskRuntime(source.EntryRuntime),
		Status:                          TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if err := validateTaskRecord(retry); err != nil {
		return TaskRecord{}, err
	}
	return retry, nil
}
