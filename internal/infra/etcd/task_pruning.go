package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskPruneBatchRecords = (maximumTransactionOperations - 2) / 2
	maximumTaskPruneCASAttempts  = 8
)

type taskPruneIntent struct {
	TaskID                  string `json:"task_id"`
	AttachPlanID            string `json:"attach_plan_id,omitempty"`
	RemainingEvents         uint32 `json:"remaining_events"`
	RemainingDeduplications uint32 `json:"remaining_deduplications"`
}

func encodeTaskPruneIntent(intent taskPruneIntent) ([]byte, error) {
	if err := validateTaskPruneIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("task_prune_intent", intent)
}

func decodeTaskPruneIntent(value []byte) (taskPruneIntent, error) {
	intent, err := decodeEnvelope[taskPruneIntent](value, "task_prune_intent")
	if err != nil {
		return taskPruneIntent{}, err
	}
	if err := validateTaskPruneIntent(intent); err != nil {
		return taskPruneIntent{}, corruptTaskPruneIntent()
	}
	return intent, nil
}

func validateTaskPruneIntent(intent taskPruneIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		intent.RemainingEvents > MaximumTaskEvents ||
		intent.RemainingDeduplications > MaximumTaskEvents {
		return errs.New(errs.KindValidationFailed, "Task prune intent is invalid")
	}
	if intent.AttachPlanID != "" && ids.Validate(ids.KindPlan, intent.AttachPlanID) != nil {
		return errs.New(errs.KindValidationFailed, "Task prune Attach plan is invalid")
	}
	return nil
}

// PruneExpiredTasks removes at most one complete expired Task journal. A
// previously checkpointed intent is always resumed before another Task starts.
func (repository *TaskRepository) PruneExpiredTasks(ctx context.Context, now time.Time) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "Task repository is not initialized")
	}
	if !validTaskPruneTime(now) {
		return 0, errs.New(errs.KindValidationFailed, "Task prune time must be UTC")
	}
	for attempt := 0; attempt < maximumTaskPruneCASAttempts; attempt++ {
		intent, found, err := repository.nextTaskPruneIntent(ctx)
		if err != nil {
			return 0, err
		}
		if !found {
			intent, found, err = repository.beginTaskPrune(ctx, now)
			if err != nil {
				if taskPruneConflict(err) {
					continue
				}
				return 0, err
			}
			if !found {
				return 0, nil
			}
		}
		if err := repository.drainTaskPruneIntent(ctx, intent); err != nil {
			if taskPruneConflict(err) {
				continue
			}
			return 0, err
		}
		return 1, nil
	}
	return 0, errs.New(errs.KindStateConflict, "Task prune state kept changing")
}

func (repository *TaskRepository) nextTaskPruneIntent(
	ctx context.Context,
) (Versioned[taskPruneIntent], bool, error) {
	page, err := repository.store.Range(ctx, RangeRequest{Prefix: taskPruneIntentPrefix, Limit: 1})
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if len(page.Values) == 0 {
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	entry := page.Values[0]
	defer clear(entry.Value)
	intent, err := decodeTaskPruneIntent(entry.Value)
	if err != nil || entry.Key != taskPruneIntentKey(intent.TaskID) || entry.ModRevision <= 0 {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	return Versioned[taskPruneIntent]{
		Record: intent, Revision: entry.ModRevision, ReadRevision: page.ReadRevision,
	}, true, nil
}

func (repository *TaskRepository) beginTaskPrune(
	ctx context.Context,
	now time.Time,
) (Versioned[taskPruneIntent], bool, error) {
	page, err := repository.store.Range(ctx, RangeRequest{Prefix: taskRetentionIndexPrefix, Limit: 1})
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if len(page.Values) == 0 {
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	retentionEntry := page.Values[0]
	defer clear(retentionEntry.Value)
	taskID, retainUntil, err := parseTaskRetentionIndexKey(retentionEntry.Key)
	if err != nil || retentionEntry.ModRevision <= 0 {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if retainUntil.After(now) {
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	indexedTaskID, err := decodeTaskReference(retentionEntry.Value)
	if err != nil || indexedTaskID != taskID {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	taskResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskKey(taskID)}, Revision: page.ReadRevision,
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if taskResult == nil || taskResult.ReadRevision != page.ReadRevision || len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	defer clearKeyValues(taskResult.Values)
	taskValue := taskResult.Values[0]
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || !isTerminalTaskStatus(task.Status) || task.RetainUntil == nil ||
		!task.RetainUntil.Equal(retainUntil) {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	markerKey, err := idempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	companionKeys := []string{
		markerKey,
		taskActiveOperationKey(task.OperationID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskPruneIntentKey(task.ID),
		componentTaskIntentKey(task.ID),
	}
	planReferenceIndex := -1
	if task.Type == TaskAttach || task.Type == TaskDetach {
		planReferenceIndex = len(companionKeys)
		companionKeys = append(companionKeys, attachTaskPlanReferenceKey(task.PlanID, task.ID))
	}
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: companionKeys, Revision: page.ReadRevision,
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if companions == nil || companions.ReadRevision != page.ReadRevision ||
		len(companions.Values) != len(companionKeys) {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	defer clearKeyValues(companions.Values)
	if companions.Values[0] != nil {
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	if companions.Values[1] != nil {
		if _, decodeErr := decodeTaskReference(companions.Values[1].Value); decodeErr != nil {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	if companions.Values[2] == nil || companions.Values[3] != nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	historyTaskID, err := decodeTaskReference(companions.Values[2].Value)
	if err != nil || historyTaskID != task.ID {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if companions.Values[4] != nil {
		componentIntent, decodeErr := decodeComponentTaskIntent(companions.Values[4].Value)
		if decodeErr != nil || validateComponentTaskOwner(task, componentIntent) != nil ||
			componentIntent.Status != task.Status || componentIntent.TerminalAt == nil || task.TerminalAt == nil ||
			!componentIntent.TerminalAt.Equal(*task.TerminalAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	intent := taskPruneIntent{
		TaskID: task.ID, RemainingEvents: task.EventCount,
		RemainingDeduplications: task.EventCount,
	}
	if planReferenceIndex >= 0 {
		planReferenceValue := companions.Values[planReferenceIndex]
		if planReferenceValue == nil {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		if _, decodeErr := decodeAttachTaskPlanReference(
			planReferenceValue.Key,
			planReferenceValue.Value,
			task.PlanID,
		); decodeErr != nil {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		intent.AttachPlanID = task.PlanID
	}
	intentValue, err := encodeTaskPruneIntent(intent)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	defer clear(intentValue)
	conditions := []Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: retentionEntry.Key, ModRevision: retentionEntry.ModRevision},
		{Key: markerKey},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID), ModRevision: companions.Values[2].ModRevision},
		{Key: taskPruneIntentKey(task.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskPruneIntentKey(task.ID), Value: intentValue},
		{Type: MutationDelete, Key: taskKey(task.ID)},
		{Type: MutationDelete, Key: retentionEntry.Key},
		{Type: MutationDelete, Key: taskOperationIndexKey(task.OperationID, task.ID)},
	}
	componentCondition := Condition{Key: componentTaskIntentKey(task.ID)}
	if companions.Values[4] != nil {
		componentCondition.ModRevision = companions.Values[4].ModRevision
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: componentTaskIntentKey(task.ID)})
	}
	conditions = append(conditions, componentCondition)
	if planReferenceIndex >= 0 {
		planReferenceValue := companions.Values[planReferenceIndex]
		conditions = append(conditions, Condition{
			Key: planReferenceValue.Key, ModRevision: planReferenceValue.ModRevision,
		})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: planReferenceValue.Key})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[taskPruneIntent]{}, false, errs.New(errs.KindStateConflict, "Task prune start changed")
	}
	return Versioned[taskPruneIntent]{
		Record: intent, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, true, nil
}

func (repository *TaskRepository) drainTaskPruneIntent(
	ctx context.Context,
	current Versioned[taskPruneIntent],
) error {
	var err error
	for current.Record.RemainingEvents > 0 {
		current, err = repository.pruneTaskSubordinateBatch(ctx, current, true)
		if err != nil {
			return err
		}
	}
	if err := repository.verifyTaskPrunePrefixEmpty(ctx, current.Record.TaskID, true); err != nil {
		return err
	}
	for current.Record.RemainingDeduplications > 0 {
		current, err = repository.pruneTaskSubordinateBatch(ctx, current, false)
		if err != nil {
			return err
		}
	}
	if err := repository.verifyTaskPrunePrefixEmpty(ctx, current.Record.TaskID, false); err != nil {
		return err
	}
	return repository.finishTaskPruneIntent(ctx, current)
}

func (repository *TaskRepository) finishTaskPruneIntent(
	ctx context.Context,
	current Versioned[taskPruneIntent],
) error {
	conditions := []Condition{{
		Key: taskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}
	mutations := []Mutation{{Type: MutationDelete, Key: taskPruneIntentKey(current.Record.TaskID)}}
	if current.Record.AttachPlanID != "" {
		references, err := repository.store.Range(ctx, RangeRequest{
			Prefix: attachTaskPlanReferenceScopePrefix(current.Record.AttachPlanID), Limit: 1,
		})
		if err != nil {
			return err
		}
		if references == nil || references.ReadRevision <= 0 || len(references.Values) > 1 {
			return corruptTaskPruneIntent()
		}
		defer clearKeyValueSlice(references.Values)
		if len(references.Values) != 0 {
			if _, err := decodeAttachTaskPlanReference(
				references.Values[0].Key,
				references.Values[0].Value,
				current.Record.AttachPlanID,
			); err != nil {
				return corruptTaskPruneIntent()
			}
		} else {
			inputKey := attachTaskRenderInputKey(current.Record.AttachPlanID)
			inputResult, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{inputKey}})
			if err != nil {
				return err
			}
			if inputResult == nil || len(inputResult.Values) != 1 || inputResult.Values[0] == nil {
				return corruptTaskPruneIntent()
			}
			defer clearKeyValues(inputResult.Values)
			input, err := decodeAttachTaskRenderInput(inputResult.Values[0].Value)
			if err != nil || input.PlanID != current.Record.AttachPlanID {
				return corruptTaskPruneIntent()
			}
			conditions = append(conditions, Condition{
				Key: inputKey, ModRevision: inputResult.Values[0].ModRevision,
			})
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: inputKey})
		}
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "Task prune completion changed")
	}
	return nil
}

func (repository *TaskRepository) pruneTaskSubordinateBatch(
	ctx context.Context,
	current Versioned[taskPruneIntent],
	events bool,
) (Versioned[taskPruneIntent], error) {
	remaining := current.Record.RemainingDeduplications
	prefix := taskEventDedupScopePrefix(current.Record.TaskID)
	if events {
		remaining = current.Record.RemainingEvents
		prefix = taskEventScopePrefix(current.Record.TaskID)
	}
	limit := int64(maximumTaskPruneBatchRecords + 1)
	if remaining < maximumTaskPruneBatchRecords {
		limit = int64(remaining) + 1
	}
	page, err := repository.store.Range(ctx, RangeRequest{Prefix: prefix, Limit: limit})
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) == 0 {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	if uint32(len(page.Values)) > remaining && remaining <= maximumTaskPruneBatchRecords {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	count := len(page.Values)
	if count > maximumTaskPruneBatchRecords {
		count = maximumTaskPruneBatchRecords
	}
	if uint32(count) > remaining {
		count = int(remaining)
	}
	if !page.More && uint32(len(page.Values)) < remaining {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	conditions := make([]Condition, 1, count+1)
	conditions[0] = Condition{Key: taskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision}
	mutations := make([]Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskPruneSubordinate(current.Record.TaskID, entry, events); err != nil {
			return Versioned[taskPruneIntent]{}, err
		}
		conditions = append(conditions, Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: entry.Key})
	}
	next := current.Record
	if events {
		next.RemainingEvents -= uint32(count)
	} else {
		next.RemainingDeduplications -= uint32(count)
	}
	intentValue, err := encodeTaskPruneIntent(next)
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	defer clear(intentValue)
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: taskPruneIntentKey(next.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return Versioned[taskPruneIntent]{}, errs.New(errs.KindInternal, "Task prune batch exceeds transaction limit")
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[taskPruneIntent]{}, errs.New(errs.KindStateConflict, "Task prune batch changed")
	}
	return Versioned[taskPruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func validateTaskPruneSubordinate(taskID string, entry KeyValue, events bool) error {
	if entry.ModRevision <= 0 {
		return corruptTaskPruneIntent()
	}
	if events {
		sequence, err := taskEventSequenceFromKey(taskID, entry.Key)
		if err != nil {
			return corruptTaskPruneIntent()
		}
		event, err := decodeTaskEventRecord(entry.Value)
		if err != nil || event.Identity.TaskID != taskID || event.Sequence != sequence {
			return corruptTaskPruneIntent()
		}
		return nil
	}
	record, err := decodeTaskEventDedupRecord(entry.Value)
	if err != nil || record.Identity.TaskID != taskID || taskEventDedupKey(record.Identity) != entry.Key {
		return corruptTaskPruneIntent()
	}
	return nil
}

func (repository *TaskRepository) verifyTaskPrunePrefixEmpty(
	ctx context.Context,
	taskID string,
	events bool,
) error {
	prefix := taskEventDedupScopePrefix(taskID)
	if events {
		prefix = taskEventScopePrefix(taskID)
	}
	page, err := repository.store.Range(ctx, RangeRequest{Prefix: prefix, Limit: 1})
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) != 0 {
		if page != nil {
			clearKeyValueSlice(page.Values)
		}
		return corruptTaskPruneIntent()
	}
	return nil
}

func taskPruneConflict(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}

func corruptTaskPruneIntent() error {
	return errs.New(errs.KindInternal, "Task prune state is corrupt")
}
