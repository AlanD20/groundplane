package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// terminalizeBlueprintParent is only called after the status-specific unit
// proof. It fences the head, epoch and plan with the Task/claim/marker update.
func (repository *TaskRepository) terminalizeBlueprintParent(
	ctx context.Context, snapshot blueprintunits.Snapshot, taskID string,
	status taskjournal.TaskStatus, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	environmentID := snapshot.EnvironmentID
	taskKey := taskjournal.TaskStorageKey(taskID)
	claimKey := taskjournal.BlueprintParentClaimKey(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskKey, claimKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent claim is unavailable",
		)
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || validateBlueprintParentClaimTask(task) != nil || task.Owner.EnvironmentID != environmentID ||
		task.Status != taskjournal.TaskStatusRunning || task.idempotencyMarker == nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "Blueprint parent claim is inconsistent")
	}
	claimTaskID, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil || claimTaskID != taskID {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"Blueprint parent claim reference is inconsistent",
		)
	}
	terminal, err := TransitionTaskStatus(task, taskjournal.TaskStatusRunning, status, at)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	prepared, markerKey, retentionKey, err := prepareTerminalTaskMarker(terminal, terminal.Status, *terminal.FinishedAt)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	activeKey := taskjournal.TaskActiveOperationKey(task.OperationID)
	companions, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{activeKey, markerKey, retentionKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 3 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"Blueprint parent lifecycle records are inconsistent",
		)
	}
	defer keyvalue.ClearValues(companions.Values)
	if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	prepared, err = hydrateTerminalTaskMarker(prepared, companions.Values[1].Value)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(prepared.Intent.Ciphertext)
	defer clear(prepared.Response.Body)
	terminalValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(terminalValue)
	markerValue, err := idempotency.EncodeIdempotencyMarker(prepared)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(markerValue)
	retentionValue, err := json.Marshal(idempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(retentionValue)
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(taskRetentionValue)
	desiredRevision := int64(0)
	if snapshot.Desired != nil {
		desiredRevision = snapshot.Desired.Revision
	}
	conditions := []keyvalue.Condition{
		{Key: taskKey, ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
		{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
		{Key: retentionKey},
		{Key: taskRetentionKey},
		{Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), ModRevision: snapshot.HeadRevision},
		{Key: blueprintunits.EpochKey(environmentID), ModRevision: snapshot.EpochRevision},
		{Key: blueprintunits.DesiredPlanKey(environmentID), ModRevision: desiredRevision},
	}
	abortKey := taskjournal.BlueprintParentAbortKey(taskID)
	abort, abortErr := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{abortKey}, Revision: snapshot.ReadRevision,
	})
	if abortErr != nil {
		return keyvalue.Versioned[TaskRecord]{}, abortErr
	}
	if abort == nil || len(abort.Values) != 1 {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "Blueprint parent abort read is incomplete")
	}
	if abort.Values[0] == nil {
		conditions = append(conditions, keyvalue.Condition{Key: abortKey})
	} else {
		request, decodeErr := decodeBlueprintParentAbortRequest(abort.Values[0].Value)
		if decodeErr != nil || request.TaskID != taskID || status != taskjournal.TaskStatusAborted {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent abort request changed")
		}
		conditions = append(conditions, keyvalue.Condition{Key: abortKey, ModRevision: abort.Values[0].ModRevision})
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskKey, Value: terminalValue},
		{Type: keyvalue.MutationDelete, Key: claimKey},
		{Type: keyvalue.MutationDelete, Key: activeKey},
		{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue},
		{Type: keyvalue.MutationPut, Key: retentionKey, Value: retentionValue},
		{Type: keyvalue.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
	}
	if abort.Values[0] != nil {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: abortKey})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if !transaction.Succeeded {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent terminalization raced",
		)
	}
	return keyvalue.Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}
