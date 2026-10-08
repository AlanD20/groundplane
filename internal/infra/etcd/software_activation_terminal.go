package etcd

import (
	"context"
	"encoding/json"
	"time"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TerminalizeSoftwareActivation atomically seals the final projection and the
// visible parent Task, releases coordination authority, and completes replay
// and retention records. A successful Controller is never rolled back here.
func (repository *TaskRepository) TerminalizeSoftwareActivation(
	ctx context.Context,
	taskID string,
	progress activationrecord.Progress,
	status taskjournal.TaskStatus,
	at time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if err := recordcodec.ValidateTimestamp("software activation terminal", at); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if status != taskjournal.TaskStatusCompleted && status != taskjournal.TaskStatusFailed ||
		status == taskjournal.TaskStatusCompleted && progress.Phase != activationrecord.PhaseCompleted ||
		status == taskjournal.TaskStatusFailed && progress.Phase != activationrecord.PhaseFailed {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"software activation terminal status is invalid",
		)
	}
	taskKey := taskjournal.TaskStorageKey(taskID)
	claimKey := activationrecord.ClaimKey(taskID)
	progressKey := activationrecord.Key(taskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{taskKey, claimKey, progressKey}})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"running software activation claim is unavailable",
		)
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || validateSoftwareActivationTask(task) != nil || task.ID != taskID ||
		task.Status != taskjournal.TaskStatusRunning || task.idempotencyMarker == nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"software activation terminal Task is inconsistent",
		)
	}
	claimedTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[1].Value)
	if err != nil || claimedTaskID != taskID {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"software activation terminal claim is inconsistent",
		)
	}
	activation, err := activationrecord.Decode(read.Values[2].Value)
	if err != nil || activation.TaskID != taskID || activation.OperationID != task.OperationID ||
		activation.InputSHA256 != task.PlanHash {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"software activation terminal progress is inconsistent",
		)
	}
	if err := activationrecord.ValidateTransition(activation.Progress, progress, activation.Input); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	previous := task.UpdatedAt
	if activation.UpdatedAt.After(previous) {
		previous = activation.UpdatedAt
	}
	terminalAt, err := nextTaskControllerTimestamp(previous, at)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminal, err := TransitionTaskStatus(task, taskjournal.TaskStatusRunning, status, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	prepared, markerKey, retentionKey, err := prepareTerminalTaskMarker(terminal, status, *terminal.FinishedAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	activeKey := taskjournal.TaskActiveOperationKey(task.OperationID)
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{activeKey, markerKey, retentionKey}, Revision: read.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 3 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"software activation lifecycle records are inconsistent",
		)
	}
	defer etcdstore.ClearValues(companions.Values)
	if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	prepared, err = hydrateTerminalTaskMarker(prepared, companions.Values[1].Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(prepared.Intent.Ciphertext)
	defer clear(prepared.Response.Body)
	activation.Progress = progress
	activation.UpdatedAt = terminalAt.UTC()
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(taskValue)
	progressValue, err := activationrecord.Encode(activation)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(progressValue)
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(prepared)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(markerValue)
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(retentionValue)
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(taskRetentionValue)
	conditions := []etcdstore.Condition{
		{Key: taskKey, ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: progressKey, ModRevision: read.Values[2].ModRevision},
		{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
		{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
		{Key: retentionKey}, {Key: taskRetentionKey},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey, Value: taskValue},
		{Type: etcdstore.MutationPut, Key: progressKey, Value: progressValue},
		{Type: etcdstore.MutationDelete, Key: claimKey},
		{Type: etcdstore.MutationDelete, Key: activeKey},
		{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
		{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(transaction.FailureReads)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if !transaction.Succeeded {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"software activation terminalization raced",
		)
	}
	return etcdstore.Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, nil
}
