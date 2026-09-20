package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskPruneBatchRecords = (etcdstore.MaximumOperations - 2) / 2
	maximumTaskPruneCASAttempts  = 8
)

type taskPruneIntent struct {
	TaskID                                 string `json:"task_id"`
	TaskRevision                           int64  `json:"task_revision"`
	BackupCheckpointCursorsComplete        bool   `json:"backup_checkpoint_cursors_complete"`
	BackupCheckpointDeduplicationsComplete bool   `json:"backup_checkpoint_deduplications_complete"`
	TaskPrimaryDeleted                     bool   `json:"task_primary_deleted"`
	BackupTerminalReceiptRevision          int64  `json:"backup_terminal_receipt_revision,omitempty"`
	AttachPlanID                           string `json:"attach_plan_id,omitempty"`
	RemainingEvents                        uint32 `json:"remaining_events"`
	RemainingDeduplications                uint32 `json:"remaining_deduplications"`
}

func encodeTaskPruneIntent(intent taskPruneIntent) ([]byte, error) {
	if err := validateTaskPruneIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task_prune_intent", intent)
}

func decodeTaskPruneIntent(value []byte) (taskPruneIntent, error) {
	intent, err := recordcodec.Decode[taskPruneIntent](value, "task_prune_intent")
	if err != nil {
		return taskPruneIntent{}, err
	}
	if err := validateTaskPruneIntent(intent); err != nil {
		return taskPruneIntent{}, corruptTaskPruneIntent()
	}
	return intent, nil
}

func validateTaskPruneIntent(intent taskPruneIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil || intent.TaskRevision <= 0 ||
		(intent.BackupCheckpointDeduplicationsComplete &&
			!intent.BackupCheckpointCursorsComplete) ||
		(intent.TaskPrimaryDeleted && !intent.BackupCheckpointDeduplicationsComplete) ||
		intent.BackupTerminalReceiptRevision < 0 ||
		intent.RemainingEvents > MaximumTaskEvents ||
		intent.RemainingDeduplications > MaximumTaskEvents {
		return errs.New(errs.KindValidationFailed, "task prune intent is invalid")
	}
	if intent.AttachPlanID != "" && ids.Validate(ids.KindPlan, intent.AttachPlanID) != nil {
		return errs.New(errs.KindValidationFailed, "task prune Attach plan is invalid")
	}
	return nil
}

// PruneExpiredTasks removes at most one complete expired Task journal. A
// previously checkpointed intent is always resumed before another Task starts.
func (repository *TaskRepository) PruneExpiredTasks(
	ctx context.Context,
	now time.Time,
) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "task repository is not initialized")
	}
	if !validTaskPruneTime(now) {
		return 0, errs.New(errs.KindValidationFailed, "task prune time must be UTC")
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
	return 0, errs.New(errs.KindStateConflict, "task prune state kept changing")
}

func (repository *TaskRepository) nextTaskPruneIntent(
	ctx context.Context,
) (Versioned[taskPruneIntent], bool, error) {
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: taskPruneIntentPrefix, Limit: 1})
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
	page, err := repository.nextTaskRetentionPruneCandidate(ctx, now)
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
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskKey(taskID)}, Revision: page.ReadRevision,
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if taskResult == nil || taskResult.ReadRevision != page.ReadRevision ||
		len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	defer clearKeyValues(taskResult.Values)
	taskValue := taskResult.Values[0]
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || !isTerminalTaskStatus(task.Status) ||
		task.RetainUntil == nil ||
		!task.RetainUntil.Equal(retainUntil) {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	stop, err := repository.prepareTaskPruneBoundary(
		ctx, task, taskValue.ModRevision, retentionEntry, page.ReadRevision, now,
	)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	if stop {
		return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
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
		routeRemovalIntentKey(task.ID),
		routeMutationIntentKey(task.ID),
		entryRemovalIntentKey(task.ID),
		blueprintAttachTaskIntentKey(task.ID),
	}
	environmentDeletionFenceStart := -1
	var environmentDeletionFenceKeys []string
	if task.Executor == TaskExecutorAgent && task.Type == TaskRemove &&
		ids.Validate(ids.KindEnvironment, task.Target) == nil {
		environmentDeletionFenceStart = len(companionKeys)
		environmentDeletionFenceKeys = []string{
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentOperationLockKey(task.Target),
			environmentDeletionIntentKey(task.OperationID),
		}
		companionKeys = append(companionKeys, environmentDeletionFenceKeys...)
	}
	ownerIndexStart := len(companionKeys)
	ownerIndexKeys, err := taskJournalIndexKeys(task)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	companionKeys = append(companionKeys, ownerIndexKeys...)
	planReferenceIndex := -1
	if task.Type == TaskAttach || task.Type == TaskDetach {
		planReferenceIndex = len(companionKeys)
		companionKeys = append(companionKeys, attachTaskPlanReferenceKey(task.PlanID, task.ID))
	}
	backupPruneDispatchIndex := -1
	if task.Type == TaskBackupPrune {
		backupPruneDispatchIndex = len(companionKeys)
		companionKeys = append(companionKeys, backupRecoveryPointPruneDispatchKey(task.ID))
	}
	backupTerminalReceiptIndex := -1
	if task.Type == TaskBackup || task.Type == TaskBackupPrune {
		backupTerminalReceiptIndex = len(companionKeys)
		companionKeys = append(companionKeys, backupTerminalReceiptKey(task.ID))
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
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
	environmentDeletionBlocked := false
	environmentDeletionOwnerTaskID := ""
	if environmentDeletionFenceStart >= 0 {
		environmentDeletionBlocked, environmentDeletionOwnerTaskID, err =
			environmentDeletionTaskPruneFence(
				task,
				companions.Values[environmentDeletionFenceStart:ownerIndexStart],
			)
		if err != nil {
			return Versioned[taskPruneIntent]{}, false, err
		}
	}
	activeOperationCondition := etcdstore.Condition{Key: taskActiveOperationKey(task.OperationID)}
	if companions.Values[1] != nil {
		activeTaskID, decodeErr := decodeTaskReference(companions.Values[1].Value)
		if decodeErr != nil {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		if environmentDeletionBlocked || environmentDeletionOwnerTaskID == "" ||
			activeTaskID != environmentDeletionOwnerTaskID {
			return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
		}
		activeOperationCondition.ModRevision = companions.Values[1].ModRevision
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
			componentIntent.Status != task.Status || componentIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!componentIntent.TerminalAt.Equal(*task.FinishedAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[5] != nil {
		routeIntent, decodeErr := decodeRouteRemovalIntent(companions.Values[5].Value)
		if decodeErr != nil || validateRouteRemovalTaskOwner(task, routeIntent) != nil ||
			routeIntent.Status != task.Status || routeIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!routeIntent.TerminalAt.Equal(*task.FinishedAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[6] != nil {
		mutationIntent, decodeErr := decodeRouteMutationIntent(companions.Values[6].Value)
		if decodeErr != nil || validateRouteMutationTaskOwner(task, mutationIntent) != nil ||
			mutationIntent.Status != task.Status || mutationIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!mutationIntent.TerminalAt.Equal(*task.FinishedAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[7] != nil {
		entryIntent, decodeErr := decodeEntryRemovalIntent(companions.Values[7].Value)
		if decodeErr != nil || validateEntryRemovalTaskOwner(task, entryIntent) != nil ||
			entryIntent.Status != task.Status || entryIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!entryIntent.TerminalAt.Equal(*task.FinishedAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[8] != nil {
		attachIntent, decodeErr := decodeBlueprintAttachTaskIntent(companions.Values[8].Value)
		if decodeErr != nil || validateBlueprintAttachTaskOwner(task, attachIntent) != nil ||
			attachIntent.Status != task.Status || attachIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!attachIntent.TerminalAt.Equal(*task.FinishedAt) {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if backupPruneDispatchIndex >= 0 && companions.Values[backupPruneDispatchIndex] != nil {
		return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	var backupReceiptCompanion backupTerminalReceiptPruneCompanion
	if backupTerminalReceiptIndex >= 0 {
		backupReceiptCompanion, err = prepareBackupTerminalReceiptPruneCompanion(
			task,
			taskValue.ModRevision,
			companions.Values[backupTerminalReceiptIndex],
		)
		if err != nil {
			return Versioned[taskPruneIntent]{}, false, err
		}
	}
	if environmentDeletionFenceStart >= 0 {
		if environmentDeletionBlocked {
			conditions := []etcdstore.Condition{
				{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
				{Key: retentionEntry.Key, ModRevision: retentionEntry.ModRevision},
			}
			for index, key := range environmentDeletionFenceKeys {
				conditions = append(conditions, etcdstore.Condition{
					Key:         key,
					ModRevision: companions.Values[environmentDeletionFenceStart+index].ModRevision,
				})
			}
			transaction, transactErr := repository.store.Transact(
				ctx,
				conditions,
				[]etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: retentionEntry.Key}},
			)
			if transactErr != nil {
				return Versioned[taskPruneIntent]{}, false, transactErr
			}
			clearKeyValues(transaction.FailureReads)
			if !transaction.Succeeded {
				return Versioned[taskPruneIntent]{}, false, errs.New(
					errs.KindStateConflict,
					"task prune retained ownership changed",
				)
			}
			return Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
		}
	}
	for index, key := range ownerIndexKeys {
		value := companions.Values[ownerIndexStart+index]
		if value == nil || value.Key != key || value.ModRevision <= 0 ||
			string(value.Value) != task.ID {
			return Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	intent := taskPruneIntent{
		TaskID: task.ID, TaskRevision: taskValue.ModRevision, RemainingEvents: task.EventCount,
		BackupTerminalReceiptRevision: backupReceiptCompanion.revision,
		RemainingDeduplications:       task.EventCount,
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
	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: retentionEntry.Key, ModRevision: retentionEntry.ModRevision},
		{Key: markerKey},
		activeOperationCondition,
		{Key: taskOperationIndexKey(task.OperationID, task.ID), ModRevision: companions.Values[2].ModRevision},
		{Key: taskPruneIntentKey(task.ID)},
	}
	conditions = append(conditions, taskSourcePruneConditions(task)...)
	for index, key := range environmentDeletionFenceKeys {
		condition := etcdstore.Condition{Key: key}
		value := companions.Values[environmentDeletionFenceStart+index]
		if value != nil {
			condition.ModRevision = value.ModRevision
		}
		conditions = append(conditions, condition)
	}
	if backupPruneDispatchIndex >= 0 {
		conditions = append(conditions, etcdstore.Condition{Key: backupRecoveryPointPruneDispatchKey(task.ID)})
	}
	conditions = backupReceiptCompanion.appendStartCondition(conditions)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskPruneIntentKey(task.ID), Value: intentValue},
		{Type: etcdstore.MutationDelete, Key: serviceLifecycleRenderInputKey(task.ID)},
		{Type: etcdstore.MutationDelete, Key: backingHookCheckpointTaskPrefix(task.ID), Prefix: true},
		{Type: etcdstore.MutationDelete, Key: retentionEntry.Key},
		{Type: etcdstore.MutationDelete, Key: taskOperationIndexKey(task.OperationID, task.ID)},
	}
	for index, key := range ownerIndexKeys {
		value := companions.Values[ownerIndexStart+index]
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
	}
	componentCondition := etcdstore.Condition{Key: componentTaskIntentKey(task.ID)}
	if companions.Values[4] != nil {
		componentCondition.ModRevision = companions.Values[4].ModRevision
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: componentTaskIntentKey(task.ID)},
		)
	}
	conditions = append(conditions, componentCondition)
	routeCondition := etcdstore.Condition{Key: routeRemovalIntentKey(task.ID)}
	if companions.Values[5] != nil {
		routeCondition.ModRevision = companions.Values[5].ModRevision
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: routeRemovalIntentKey(task.ID)},
		)
	}
	conditions = append(conditions, routeCondition)
	mutationCondition := etcdstore.Condition{Key: routeMutationIntentKey(task.ID)}
	if companions.Values[6] != nil {
		mutationCondition.ModRevision = companions.Values[6].ModRevision
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: routeMutationIntentKey(task.ID)})
	}
	conditions = append(conditions, mutationCondition)
	entryCondition := etcdstore.Condition{Key: entryRemovalIntentKey(task.ID)}
	if companions.Values[7] != nil {
		entryCondition.ModRevision = companions.Values[7].ModRevision
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryRemovalIntentKey(task.ID)},
		)
	}
	conditions = append(conditions, entryCondition)
	if companions.Values[8] != nil {
		conditions = append(conditions, etcdstore.Condition{
			Key: blueprintAttachTaskIntentKey(task.ID), ModRevision: companions.Values[8].ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: blueprintAttachTaskIntentKey(task.ID),
		})
	}
	if planReferenceIndex >= 0 {
		planReferenceValue := companions.Values[planReferenceIndex]
		conditions = append(conditions, etcdstore.Condition{
			Key: planReferenceValue.Key, ModRevision: planReferenceValue.ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: planReferenceValue.Key})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[taskPruneIntent]{}, false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[taskPruneIntent]{}, false, errs.New(
			errs.KindStateConflict,
			"task prune start changed",
		)
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
	for !current.Record.BackupCheckpointCursorsComplete {
		current, err = repository.pruneTaskBackupCheckpointBatch(ctx, current, true)
		if err != nil {
			return err
		}
	}
	for !current.Record.BackupCheckpointDeduplicationsComplete {
		current, err = repository.pruneTaskBackupCheckpointBatch(ctx, current, false)
		if err != nil {
			return err
		}
	}
	if !current.Record.TaskPrimaryDeleted {
		current, err = repository.deleteTaskPrunePrimary(ctx, current)
		if err != nil {
			return err
		}
	}
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

func (repository *TaskRepository) pruneTaskBackupCheckpointBatch(
	ctx context.Context,
	current Versioned[taskPruneIntent],
	cursors bool,
) (Versioned[taskPruneIntent], error) {
	prefix := backupCheckpointDedupTaskPrefix(current.Record.TaskID)
	if cursors {
		prefix = backupCheckpointCursorTaskPrefix(current.Record.TaskID)
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix,
		Limit:  int64(maximumTaskPruneBatchRecords + 1),
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	next := current.Record
	if len(page.Values) == 0 {
		if cursors {
			next.BackupCheckpointCursorsComplete = true
		} else {
			next.BackupCheckpointDeduplicationsComplete = true
		}
		return repository.advanceTaskPruneIntent(
			ctx,
			current,
			next,
			[]etcdstore.Condition{{Key: prefix, Prefix: true}},
			nil,
		)
	}
	count := len(page.Values)
	if count > maximumTaskPruneBatchRecords {
		count = maximumTaskPruneBatchRecords
	}
	conditions := make([]etcdstore.Condition, 0, count)
	mutations := make([]etcdstore.Mutation, 0, count)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskBackupCheckpointPruneEntry(
			current.Record.TaskID,
			entry,
			cursors,
		); err != nil {
			return Versioned[taskPruneIntent]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entry.Key})
	}
	return repository.advanceTaskPruneIntent(ctx, current, next, conditions, mutations)
}

func (repository *TaskRepository) deleteTaskPrunePrimary(
	ctx context.Context,
	current Versioned[taskPruneIntent],
) (Versioned[taskPruneIntent], error) {
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskKey(current.Record.TaskID)},
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	if taskResult == nil || taskResult.ReadRevision <= 0 || len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil ||
		taskResult.Values[0].ModRevision != current.Record.TaskRevision {
		if taskResult != nil {
			clearKeyValues(taskResult.Values)
		}
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValues(taskResult.Values)
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil || task.ID != current.Record.TaskID || !isTerminalTaskStatus(task.Status) {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	ownerKeys, err := taskJournalIndexKeys(task)
	if err != nil {
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	ownerResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: ownerKeys, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	if ownerResult == nil || ownerResult.ReadRevision != taskResult.ReadRevision ||
		len(ownerResult.Values) != len(ownerKeys) {
		if ownerResult != nil {
			clearKeyValues(ownerResult.Values)
		}
		return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValues(ownerResult.Values)
	conditions := []etcdstore.Condition{
		{Key: taskKey(current.Record.TaskID), ModRevision: current.Record.TaskRevision},
		{Key: backupCheckpointCursorTaskPrefix(current.Record.TaskID), Prefix: true},
		{Key: backupCheckpointDedupTaskPrefix(current.Record.TaskID), Prefix: true},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: taskKey(current.Record.TaskID)}}
	for index, key := range ownerKeys {
		value := ownerResult.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || string(value.Value) != task.ID {
			return Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
		}
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	next := current.Record
	next.TaskPrimaryDeleted = true
	return repository.advanceTaskPruneIntent(
		ctx,
		current,
		next,
		conditions,
		mutations,
	)
}

func (repository *TaskRepository) advanceTaskPruneIntent(
	ctx context.Context,
	current Versioned[taskPruneIntent],
	next taskPruneIntent,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (Versioned[taskPruneIntent], error) {
	intentValue, err := encodeTaskPruneIntent(next)
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	defer clear(intentValue)
	conditions = append([]etcdstore.Condition{{
		Key: taskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}, conditions...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskPruneIntentKey(current.Record.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return Versioned[taskPruneIntent]{}, errs.New(
			errs.KindInternal,
			"task prune batch exceeds transaction limit",
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[taskPruneIntent]{}, errs.New(
			errs.KindStateConflict,
			"task prune batch changed",
		)
	}
	return Versioned[taskPruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func validateTaskBackupCheckpointPruneEntry(
	taskID string,
	entry etcdstore.KeyValue,
	cursor bool,
) error {
	if entry.ModRevision <= 0 {
		return corruptTaskPruneIntent()
	}
	if cursor {
		record, err := decodeBackupCheckpointCursorRecord(entry.Value)
		if err != nil || record.TaskID != taskID ||
			backupCheckpointCursorKey(BackupCheckpointInput{
				TaskID: record.TaskID, AssignmentID: record.AssignmentID, StepID: record.StepID,
			}) != entry.Key {
			return corruptTaskPruneIntent()
		}
		return nil
	}
	record, err := decodeBackupCheckpointDedupRecord(entry.Value)
	if err != nil || record.TaskID != taskID ||
		backupCheckpointDedupKey(BackupCheckpointInput{
			TaskID: record.TaskID, AssignmentID: record.AssignmentID,
			StepID: record.StepID, Sequence: record.Sequence,
		}) != entry.Key {
		return corruptTaskPruneIntent()
	}
	return nil
}

func (repository *TaskRepository) finishTaskPruneIntent(
	ctx context.Context,
	current Versioned[taskPruneIntent],
) error {
	conditions := []etcdstore.Condition{{
		Key: taskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: taskPruneIntentKey(current.Record.TaskID)}}
	if current.Record.AttachPlanID != "" {
		references, err := repository.store.Range(ctx, etcdstore.RangeRequest{
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
			inputResult, err := repository.store.GetMany(
				ctx,
				etcdstore.GetManyRequest{Keys: []string{inputKey}},
			)
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
			conditions = append(conditions, etcdstore.Condition{
				Key: inputKey, ModRevision: inputResult.Values[0].ModRevision,
			})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: inputKey})
		}
	}
	conditions, mutations = appendBackupTerminalReceiptPruneFinalization(
		current.Record,
		conditions,
		mutations,
	)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "task prune completion changed")
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
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: limit})
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
	conditions := make([]etcdstore.Condition, 1, count+1)
	conditions[0] = etcdstore.Condition{
		Key:         taskPruneIntentKey(current.Record.TaskID),
		ModRevision: current.Revision,
	}
	mutations := make([]etcdstore.Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskPruneSubordinate(current.Record.TaskID, entry, events); err != nil {
			return Versioned[taskPruneIntent]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entry.Key})
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
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskPruneIntentKey(next.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return Versioned[taskPruneIntent]{}, errs.New(
			errs.KindInternal,
			"task prune batch exceeds transaction limit",
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[taskPruneIntent]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[taskPruneIntent]{}, errs.New(
			errs.KindStateConflict,
			"task prune batch changed",
		)
	}
	return Versioned[taskPruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func validateTaskPruneSubordinate(taskID string, entry etcdstore.KeyValue, events bool) error {
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
	if err != nil || record.Identity.TaskID != taskID ||
		taskEventDedupKey(record.Identity) != entry.Key {
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
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1})
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
