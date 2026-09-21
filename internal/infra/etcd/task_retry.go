package etcd

import (
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func cloneRetryTask(source TaskRecord, id string, actor taskjournal.TaskActor, createdAt time.Time) (TaskRecord, error) {
	if err := validateTaskRecord(source); err != nil {
		return TaskRecord{}, err
	}
	if source.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceController {
		return TaskRecord{}, errs.New(
			errs.KindTaskNotRetryable,
			"Controller update requires a fresh explicit release selection",
		)
	}
	if source.Status != taskjournal.TaskStatusFailed && source.Status != taskjournal.TaskStatusTimedOut && source.Status != taskjournal.TaskStatusAborted {
		return TaskRecord{}, errs.Newf(
			errs.KindTaskNotRetryable,
			"task %s has status %s",
			source.ID,
			source.Status,
		)
	}
	if err := recordcodec.ValidateID(ids.KindTask, id); err != nil {
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
		Steps: taskjournal.CloneTaskSteps(source.Steps), TimeoutSeconds: source.TimeoutSeconds,
		ComponentActionStepIDs:          append([]string(nil), source.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: cloneManagedComponentRuntimeSources(source.ManagedComponentTeardownSources),
		Materializations:                materializationrecord.Clone(source.Materializations),
		EntryRuntime:                    taskjournal.CloneEntryTaskRuntime(source.EntryRuntime),
		Configuration:                   taskconfiguration.CloneTaskConfiguration(source.Configuration),
		Status:                          taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if err := validateTaskRecord(retry); err != nil {
		return TaskRecord{}, err
	}
	return retry, nil
}
