package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Backup domain terminal state and its assigned Task must share one
// transaction, so this persistence-private plan cannot commit independently.
type backupTaskTerminalPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     TaskRecord
}

func (plan *backupTaskTerminalPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.record = TaskRecord{}
}

func (repository *TaskRepository) prepareBackupTaskTerminal(
	ctx context.Context,
	current TaskAssignment,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (backupTaskTerminalPlan, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupTaskTerminalPlan{}, err
	}
	task := current.Task.Record
	assignment := current.Assignment.Record
	if current.Task.Revision <= 0 || current.Assignment.Revision <= 0 ||
		current.Task.ReadRevision <= 0 || current.Assignment.ReadRevision <= 0 ||
		current.Task.ReadRevision != current.Assignment.ReadRevision ||
		(task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune) ||
		task.Owner.EnvironmentID == "" || task.Target != task.Owner.EnvironmentID ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Status != taskjournal.TaskStatusRunning ||
		assignment.Executor != taskjournal.TaskExecutorAgent || assignment.TaskID != task.ID ||
		assignment.TaskID != current.Task.Record.ID ||
		assignment.ClaimedTaskRevision >= current.Assignment.Revision ||
		!isTerminalTaskStatus(terminalStatus) ||
		taskjournal.ValidateTaskResult(result, task.Steps, terminalStatus) != nil {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup Task terminal identity is invalid",
		)
	}
	terminalAt, err := nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminal, err := transitionTaskStatus(task, taskjournal.TaskStatusRunning, terminalStatus, terminalAt)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminal.Result = taskjournal.CloneTaskResult(&result)
	terminal.TerminalAssignment = &taskjournal.TaskTerminalAssignmentRecord{
		AssignmentID: assignment.AssignmentID,
		AgentID:      assignment.AgentID, AgentGeneration: assignment.AgentGeneration,
	}
	if err := validateTaskRecord(terminal); err != nil {
		return backupTaskTerminalPlan{}, err
	}
	transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
		task,
		terminalStatus,
		terminalAt,
	)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	claimKey := taskAssignmentKey(assignment.AgentID, task.ID)
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	defer clear(taskRetentionValue)
	keys := []string{
		taskKey(task.ID), claimKey, taskAssignmentIndexKey(task.ID),
		taskActiveOperationKey(task.OperationID), markerKey, taskQueueKey(task.Executor, task.ID),
		retentionKey, taskRetentionKey, taskTimeoutIndexKey(task.ID, assignment.Deadline),
	}
	anchor, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != len(keys) {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"backup Task terminal evidence is incomplete",
		)
	}
	defer clearKeyValues(anchor.Values)
	currentTaskValue, err := encodeTaskRecord(task)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	defer clear(currentTaskValue)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Task.Revision ||
		anchor.Values[1] == nil || anchor.Values[1].ModRevision != current.Assignment.Revision ||
		anchor.Values[2] == nil || anchor.Values[2].ModRevision != current.Assignment.Revision ||
		anchor.Values[3] == nil || anchor.Values[4] == nil || anchor.Values[5] != nil ||
		anchor.Values[6] != nil || anchor.Values[7] != nil || anchor.Values[8] == nil ||
		anchor.Values[8].ModRevision != current.Assignment.Revision {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindStateConflict,
			"backup Task terminal authority changed",
		)
	}
	storedTask, taskErr := decodeTaskRecord(anchor.Values[0].Value)
	storedAssignment, assignmentErr := decodeTaskAssignment(anchor.Values[1].Value)
	if taskErr != nil || assignmentErr != nil || !bytes.Equal(anchor.Values[0].Value, currentTaskValue) ||
		storedTask.ID != task.ID ||
		storedAssignment != assignment ||
		!bytes.Equal(anchor.Values[1].Value, anchor.Values[2].Value) ||
		!bytes.Equal(anchor.Values[1].Value, anchor.Values[8].Value) {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindStateConflict,
			"backup Task assignment changed",
		)
	}
	if err := validateTaskLifecycleCompanions(task, anchor.Values[3], anchor.Values[4]); err != nil {
		return backupTaskTerminalPlan{}, err
	}
	transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, anchor.Values[4].Value)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(transitionedMarker)
	clear(transitionedMarker.Intent.Ciphertext)
	clear(transitionedMarker.Response.Body)
	if err != nil {
		clear(terminalValue)
		return backupTaskTerminalPlan{}, err
	}
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		return backupTaskTerminalPlan{}, errs.Wrap(errs.KindInternal, err)
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key}
		if anchor.Values[index] != nil {
			conditions[index].ModRevision = anchor.Values[index].ModRevision
		}
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
		{Type: etcdstore.MutationDelete, Key: keys[3]},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: markerValue},
		{Type: etcdstore.MutationPut, Key: keys[6], Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: keys[7], Value: append([]byte(nil), taskRetentionValue...)},
		{Type: etcdstore.MutationDelete, Key: keys[8]},
	}
	return backupTaskTerminalPlan{conditions: conditions, mutations: mutations, record: terminal}, nil
}
