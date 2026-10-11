package etcd

import (
	"context"
	"encoding/json"
	"time"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// pendingTaskTerminalPlan changes only the queued Task, idempotency and retention
// records. Each operation must join its own no-effect cleanup and source guards.
type pendingTaskTerminalPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     TaskRecord
}

func (plan *pendingTaskTerminalPlan) clear() {
	etcdstore.ClearMutationValues(plan.mutations)
}

func (repository *TaskRepository) preparePendingTaskTerminal(
	ctx context.Context,
	current etcdstore.Versioned[TaskRecord],
	status taskjournal.TaskStatus,
	terminalAt time.Time,
	result *taskjournal.TaskResultRecord,
) (pendingTaskTerminalPlan, error) {
	task := current.Record
	if current.Revision <= 0 || current.ReadRevision <= 0 ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Status != taskjournal.TaskStatusPending ||
		(status != taskjournal.TaskStatusAborted && status != taskjournal.TaskStatusTimedOut && status != taskjournal.TaskStatusFailed) {
		return pendingTaskTerminalPlan{}, errs.New(
			errs.KindValidationFailed,
			"pending Task terminal identity is invalid",
		)
	}
	terminal, err := TransitionTaskStatus(
		task,
		taskjournal.TaskStatusPending,
		status,
		terminalAt,
	)
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	if result != nil {
		terminal.Result = result
	}
	terminalAt = *terminal.FinishedAt
	transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
		task, status, terminalAt,
	)
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	defer clear(taskRetentionValue)
	keys := []string{
		taskjournal.TaskActiveOperationKey(task.OperationID), markerKey,
		taskjournal.TaskQueueKey(task.Executor, task.ID), retentionKey, taskRetentionKey,
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: keys, Revision: current.ReadRevision,
	})
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	if companions == nil || companions.ReadRevision != current.ReadRevision ||
		len(companions.Values) != len(keys) || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] == nil ||
		companions.Values[3] != nil || companions.Values[4] != nil {
		return pendingTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Task lifecycle records are inconsistent",
		)
	}
	defer etcdstore.ClearValues(companions.Values)
	queuedTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[2].Value)
	if err != nil || queuedTaskID != task.ID ||
		validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]) != nil {
		return pendingTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Task companions do not match",
		)
	}
	transitionedMarker, err = hydrateTerminalTaskMarker(
		transitionedMarker, companions.Values[1].Value,
	)
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	terminalValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return pendingTaskTerminalPlan{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(transitionedMarker)
	clear(transitionedMarker.Intent.Ciphertext)
	clear(transitionedMarker.Response.Body)
	if err != nil {
		clear(terminalValue)
		return pendingTaskTerminalPlan{}, err
	}
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		return pendingTaskTerminalPlan{}, errs.Wrap(errs.KindInternal, err)
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: current.Revision},
		{Key: keys[0], ModRevision: companions.Values[0].ModRevision},
		{Key: keys[1], ModRevision: companions.Values[1].ModRevision},
		{Key: keys[2], ModRevision: companions.Values[2].ModRevision},
		{Key: keys[3]}, {Key: keys[4]},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: markerValue},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
		{Type: etcdstore.MutationPut, Key: keys[3], Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: append([]byte(nil), taskRetentionValue...)},
	}
	return pendingTaskTerminalPlan{conditions: conditions, mutations: mutations, record: terminal}, nil
}
