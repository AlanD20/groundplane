package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backinghooks "github.com/AlanD20/groundplane/internal/infra/etcd/backinghooks"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) beginTaskPrune(
	ctx context.Context,
	now time.Time,
) (etcdstore.Versioned[taskPruneIntent], bool, error) {
	page, err := repository.nextTaskRetentionPruneCandidate(ctx, now)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if len(page.Values) == 0 {
		return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	retentionEntry := page.Values[0]
	defer clear(retentionEntry.Value)
	taskID, retainUntil, err := taskjournal.ParseTaskRetentionIndexKey(retentionEntry.Key)
	if err != nil || retentionEntry.ModRevision <= 0 {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if retainUntil.After(now) {
		return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	indexedTaskID, err := idempotencyrecord.DecodeTaskReference(retentionEntry.Value)
	if err != nil || indexedTaskID != taskID {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: page.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	if taskResult == nil || taskResult.ReadRevision != page.ReadRevision ||
		len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	defer etcdstore.ClearValues(taskResult.Values)
	taskValue := taskResult.Values[0]
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || !taskjournal.IsTerminalTaskStatus(task.Status) ||
		task.RetainUntil == nil ||
		!task.RetainUntil.Equal(retainUntil) {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	stop, err := repository.prepareTaskPruneBoundary(
		ctx, task, taskValue.ModRevision, retentionEntry, page.ReadRevision, now,
	)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	if stop {
		return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	companionKeys := []string{
		markerKey,
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		taskjournal.TaskPruneIntentKey(task.ID),
		componentTaskIntentKey(task.ID),
		environmentchanges.RouteRemovalIntentKey(task.ID),
		environmentchanges.RouteMutationIntentKey(task.ID),
		environmentchanges.EntryRemovalIntentKey(task.ID),
		blueprintAttachTaskIntentKey(task.ID),
	}
	environmentDeletionFenceStart := -1
	var environmentDeletionFenceKeys []string
	if task.Executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
		ids.Validate(ids.KindEnvironment, task.Target) == nil {
		environmentDeletionFenceStart = len(companionKeys)
		environmentDeletionFenceKeys = []string{
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
			hierarchyrecord.EnvironmentOperationLockKey(task.Target),
			environmentDeletionIntentKey(task.OperationID),
		}
		companionKeys = append(companionKeys, environmentDeletionFenceKeys...)
	}
	ownerIndexStart := len(companionKeys)
	ownerIndexKeys, err := taskJournalIndexKeys(task)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	companionKeys = append(companionKeys, ownerIndexKeys...)
	planReferenceIndex := -1
	if task.Type == taskjournal.TaskAttach || task.Type == taskjournal.TaskDetach {
		planReferenceIndex = len(companionKeys)
		companionKeys = append(companionKeys, attachTaskPlanReferenceKey(task.PlanID, task.ID))
	}
	backupPruneDispatchIndex := -1
	if task.Type == taskjournal.TaskBackupPrune {
		backupPruneDispatchIndex = len(companionKeys)
		companionKeys = append(companionKeys, backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID))
	}
	backupTerminalReceiptIndex := -1
	if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune {
		backupTerminalReceiptIndex = len(companionKeys)
		companionKeys = append(companionKeys, backupruntime.BackupTerminalReceiptKey(task.ID))
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: companionKeys, Revision: page.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	if companions == nil || companions.ReadRevision != page.ReadRevision ||
		len(companions.Values) != len(companionKeys) {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	defer etcdstore.ClearValues(companions.Values)
	if companions.Values[0] != nil {
		return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
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
			return etcdstore.Versioned[taskPruneIntent]{}, false, err
		}
	}
	activeOperationCondition := etcdstore.Condition{Key: taskjournal.TaskActiveOperationKey(task.OperationID)}
	if companions.Values[1] != nil {
		activeTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(companions.Values[1].Value)
		if decodeErr != nil {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		if environmentDeletionBlocked || environmentDeletionOwnerTaskID == "" ||
			activeTaskID != environmentDeletionOwnerTaskID {
			return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
		}
		activeOperationCondition.ModRevision = companions.Values[1].ModRevision
	}
	if companions.Values[2] == nil || companions.Values[3] != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	historyTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[2].Value)
	if err != nil || historyTaskID != task.ID {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	if companions.Values[4] != nil {
		componentIntent, decodeErr := decodeComponentTaskIntent(companions.Values[4].Value)
		if decodeErr != nil || validateComponentTaskOwner(task, componentIntent) != nil ||
			componentIntent.Status != task.Status || componentIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!componentIntent.TerminalAt.Equal(*task.FinishedAt) {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[5] != nil {
		routeIntent, decodeErr := environmentchanges.DecodeRouteRemovalIntent(companions.Values[5].Value)
		if decodeErr != nil || validateRouteRemovalTaskOwner(task, routeIntent) != nil ||
			routeIntent.Status != task.Status || routeIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!routeIntent.TerminalAt.Equal(*task.FinishedAt) {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[6] != nil {
		mutationIntent, decodeErr := environmentchanges.DecodeRouteMutationIntent(companions.Values[6].Value)
		if decodeErr != nil || validateRouteMutationTaskOwner(task, mutationIntent) != nil ||
			mutationIntent.Status != task.Status || mutationIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!mutationIntent.TerminalAt.Equal(*task.FinishedAt) {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[7] != nil {
		entryIntent, decodeErr := environmentchanges.DecodeEntryRemovalIntent(companions.Values[7].Value)
		if decodeErr != nil || validateEntryRemovalTaskOwner(task, entryIntent) != nil ||
			entryIntent.Status != task.Status || entryIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!entryIntent.TerminalAt.Equal(*task.FinishedAt) {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if companions.Values[8] != nil {
		attachIntent, decodeErr := decodeBlueprintAttachTaskIntent(companions.Values[8].Value)
		if decodeErr != nil || validateBlueprintAttachTaskOwner(task, attachIntent) != nil ||
			attachIntent.Status != task.Status || attachIntent.TerminalAt == nil || task.FinishedAt == nil ||
			!attachIntent.TerminalAt.Equal(*task.FinishedAt) {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
	}
	if backupPruneDispatchIndex >= 0 && companions.Values[backupPruneDispatchIndex] != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
	}
	var backupReceiptCompanion backupTerminalReceiptPruneCompanion
	if backupTerminalReceiptIndex >= 0 {
		backupReceiptCompanion, err = prepareBackupTerminalReceiptPruneCompanion(
			task,
			taskValue.ModRevision,
			companions.Values[backupTerminalReceiptIndex],
		)
		if err != nil {
			return etcdstore.Versioned[taskPruneIntent]{}, false, err
		}
	}
	if environmentDeletionFenceStart >= 0 {
		if environmentDeletionBlocked {
			conditions := []etcdstore.Condition{
				{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
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
				return etcdstore.Versioned[taskPruneIntent]{}, false, transactErr
			}
			etcdstore.ClearValues(transaction.FailureReads)
			if !transaction.Succeeded {
				return etcdstore.Versioned[taskPruneIntent]{}, false, errs.New(
					errs.KindStateConflict,
					"task prune retained ownership changed",
				)
			}
			return etcdstore.Versioned[taskPruneIntent]{ReadRevision: page.ReadRevision}, false, nil
		}
	}
	for index, key := range ownerIndexKeys {
		value := companions.Values[ownerIndexStart+index]
		if value == nil || value.Key != key || value.ModRevision <= 0 ||
			string(value.Value) != task.ID {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
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
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		if _, decodeErr := decodeAttachTaskPlanReference(
			planReferenceValue.Key,
			planReferenceValue.Value,
			task.PlanID,
		); decodeErr != nil {
			return etcdstore.Versioned[taskPruneIntent]{}, false, corruptTaskPruneIntent()
		}
		intent.AttachPlanID = task.PlanID
	}
	intentValue, err := encodeTaskPruneIntent(intent)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	defer clear(intentValue)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: retentionEntry.Key, ModRevision: retentionEntry.ModRevision},
		{Key: markerKey},
		activeOperationCondition,
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), ModRevision: companions.Values[2].ModRevision},
		{Key: taskjournal.TaskPruneIntentKey(task.ID)},
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
		conditions = append(conditions, etcdstore.Condition{Key: backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID)})
	}
	conditions = backupReceiptCompanion.appendStartCondition(conditions)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskPruneIntentKey(task.ID), Value: intentValue},
		{Type: etcdstore.MutationDelete, Key: releaserender.ServiceLifecycleRenderInputKey(task.ID)},
		{Type: etcdstore.MutationDelete, Key: backinghooks.CheckpointTaskPrefix(task.ID), Prefix: true},
		{Type: etcdstore.MutationDelete, Key: retentionEntry.Key},
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
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
	routeCondition := etcdstore.Condition{Key: environmentchanges.RouteRemovalIntentKey(task.ID)}
	if companions.Values[5] != nil {
		routeCondition.ModRevision = companions.Values[5].ModRevision
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentchanges.RouteRemovalIntentKey(task.ID)},
		)
	}
	conditions = append(conditions, routeCondition)
	mutationCondition := etcdstore.Condition{Key: environmentchanges.RouteMutationIntentKey(task.ID)}
	if companions.Values[6] != nil {
		mutationCondition.ModRevision = companions.Values[6].ModRevision
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentchanges.RouteMutationIntentKey(task.ID)})
	}
	conditions = append(conditions, mutationCondition)
	entryCondition := etcdstore.Condition{Key: environmentchanges.EntryRemovalIntentKey(task.ID)}
	if companions.Values[7] != nil {
		entryCondition.ModRevision = companions.Values[7].ModRevision
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentchanges.EntryRemovalIntentKey(task.ID)},
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
		return etcdstore.Versioned[taskPruneIntent]{}, false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[taskPruneIntent]{}, false, errs.New(
			errs.KindStateConflict,
			"task prune start changed",
		)
	}
	return etcdstore.Versioned[taskPruneIntent]{
		Record: intent, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, true, nil
}
