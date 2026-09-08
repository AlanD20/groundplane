package etcd

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) AgentTerminalProof(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) (*HierarchyDeletionAgentTerminalProof, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	childKey, _ := HierarchyDeletionChildKey(operation.Tombstone.OperationID, action.AgentProcedure.ChildOperationID)
	childRead, err := repository.store.Get(ctx, childKey)
	if err != nil {
		return nil, err
	}
	if childRead.Entry == nil {
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion child dispatch is missing")
	}
	entry, err := decodeHierarchyDeletionChildEntry(childRead.Entry.Value)
	clear(childRead.Entry.Value)
	if err != nil || !hierarchyDeletionChildMatches(entry, operation, action) {
		return nil, corruptHierarchyDeletion()
	}
	taskRead, err := repository.store.Get(ctx, taskKey(entry.CurrentTaskID))
	if err != nil {
		return nil, err
	}
	if taskRead.Entry == nil {
		return nil, corruptHierarchyDeletion()
	}
	task, err := decodeTaskRecord(taskRead.Entry.Value)
	clear(taskRead.Entry.Value)
	if err != nil || task.ID != entry.CurrentTaskID || task.OperationID != entry.ChildOperationID {
		return nil, corruptHierarchyDeletion()
	}
	if !isTerminalTaskStatus(task.Status) {
		return nil, nil
	}
	if err := repository.ensureHierarchyDeletionTerminalReceipt(
		ctx, operation, action, entry, childRead.Entry.ModRevision, task, taskRead.Entry.ModRevision,
	); err != nil {
		return nil, err
	}
	receiptKey, _ := HierarchyDeletionReceiptKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	progressKey, _ := HierarchyDeletionProgressKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	evidence, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{receiptKey, progressKey}})
	if err != nil {
		return nil, err
	}
	if evidence == nil || len(evidence.Values) != 2 || evidence.Values[0] == nil || evidence.Values[1] == nil {
		if evidence != nil {
			clearKeyValues(evidence.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion terminal receipt is not visible")
	}
	defer clearKeyValues(evidence.Values)
	var receipt HierarchyDeletionTerminalAttemptReceipt
	var progress HierarchyDeletionChildProgress
	if decodeHierarchyDeletionRecord(evidence.Values[0].Value, hierarchyDeletionSmallRecordBytes, &receipt) != nil ||
		decodeHierarchyDeletionRecord(evidence.Values[1].Value, hierarchyDeletionSmallRecordBytes, &progress) != nil ||
		receipt.ParentOperationID != operation.Tombstone.OperationID || receipt.ActionOrdinal != action.Ordinal ||
		receipt.ChildOperationID != entry.ChildOperationID || receipt.AttemptID != entry.CurrentAttemptID ||
		progress.ReceiptRevision != evidence.Values[0].ModRevision ||
		progress.ReceiptDigest != hierarchyDeletionBytesDigest(evidence.Values[0].Value) {
		return nil, corruptHierarchyDeletion()
	}
	return &HierarchyDeletionAgentTerminalProof{
		ChildOperationID: receipt.ChildOperationID, AttemptID: receipt.AttemptID,
		TaskID: receipt.TaskID, AssignmentID: receipt.AssignmentID,
		AttemptGeneration: receipt.AttemptGeneration, ReceiptRevision: evidence.Values[0].ModRevision,
		ReceiptDigest: progress.ReceiptDigest, ProgressKey: progressKey,
		ProgressDigest:     hierarchyDeletionBytesDigest(evidence.Values[1].Value),
		TerminalTaskDigest: receipt.TerminalTaskDigest, Terminal: receipt.Terminal,
		ResultDigest: receipt.ResultDigest, ErrorDigest: receipt.ErrorDigest,
		CheckpointDigest: receipt.CheckpointDigest,
	}, nil
}

func (repository *HierarchyDeletionRepository) ensureHierarchyDeletionTerminalReceipt(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	entry HierarchyDeletionChildEntry,
	entryRevision int64,
	task TaskRecord,
	taskRevision int64,
) error {
	terminal, err := hierarchyDeletionAgentTerminalFromTask(task.Status)
	if err != nil {
		return err
	}
	if task.TerminalAssignment == nil || task.FinishedAt == nil || task.Result == nil {
		return errs.New(errs.KindInternal, "hierarchy deletion child Task terminal evidence is incomplete")
	}
	if entry.DispatchState == HierarchyDeletionChildTerminal {
		if entry.Terminal == nil || entry.RetrySharedOwner.Kind != "terminal-receipt" ||
			entry.RetrySharedOwner.AttemptID != entry.CurrentAttemptID ||
			entry.RetrySharedOwner.TaskID != task.ID || entry.RetrySharedOwner.ReceiptDigest == "" {
			return corruptHierarchyDeletion()
		}
		receiptKey, _ := HierarchyDeletionReceiptKey(
			operation.Tombstone.OperationID, entry.ChildOperationID, entry.CurrentAttemptID,
		)
		receiptRead, getErr := repository.store.Get(ctx, receiptKey)
		if getErr != nil {
			return getErr
		}
		if receiptRead.Entry == nil ||
			hierarchyDeletionBytesDigest(receiptRead.Entry.Value) != entry.RetrySharedOwner.ReceiptDigest {
			return corruptHierarchyDeletion()
		}
		var receipt HierarchyDeletionTerminalAttemptReceipt
		if decodeHierarchyDeletionRecord(receiptRead.Entry.Value, hierarchyDeletionSmallRecordBytes, &receipt) != nil ||
			receipt.ParentOperationID != operation.Tombstone.OperationID ||
			receipt.ChildOperationID != entry.ChildOperationID || receipt.AttemptID != entry.CurrentAttemptID ||
			receipt.TaskID != task.ID || receipt.Terminal != *entry.Terminal {
			clear(receiptRead.Entry.Value)
			return corruptHierarchyDeletion()
		}
		clear(receiptRead.Entry.Value)
		return repository.ensureHierarchyDeletionProgress(
			ctx, operation, action, entry, receipt, receiptKey, entry.RetrySharedOwner.ReceiptDigest,
		)
	}
	generation, err := strconv.ParseInt(task.Params[TaskHierarchyDeletionGenerationParam], 10, 64)
	if err != nil || generation <= 0 || task.Params[TaskHierarchyDeletionAttemptParam] != entry.CurrentAttemptID ||
		task.Params[TaskHierarchyDeletionChildParam] != entry.ChildOperationID ||
		task.Params[TaskHierarchyDeletionParentParam] != operation.Tombstone.OperationID {
		return corruptHierarchyDeletion()
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return err
	}
	defer clear(taskValue)
	resultDigest, errorDigest, err := hierarchyDeletionTaskResultDigest(task.Result, task.Status)
	if err != nil {
		return err
	}
	terminalTaskDigest := hierarchyDeletionBytesDigest(taskValue)
	checkpointDigest := hierarchyDeletionFoldDigest(
		"gp-deletion-terminal-checkpoint-v1", entry.CheckpointDigest, terminalTaskDigest,
		resultDigest, errorDigest, string(terminal), strconv.FormatInt(taskRevision, 10),
	)
	receipt := HierarchyDeletionTerminalAttemptReceipt{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, ActionOrdinal: action.Ordinal,
		AttemptID: entry.CurrentAttemptID, TaskID: task.ID,
		AssignmentID: task.TerminalAssignment.AssignmentID, AttemptGeneration: generation,
		Terminal: terminal, TerminalTaskDigest: terminalTaskDigest,
		ResultDigest: resultDigest, ErrorDigest: errorDigest, CheckpointDigest: checkpointDigest,
		AgentAckRevision: taskRevision, PublishedAt: *task.FinishedAt,
	}
	receiptValue, err := encodeHierarchyDeletionRecord(receipt, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(receiptValue)
	receiptDigest := hierarchyDeletionBytesDigest(receiptValue)
	receiptKey, _ := HierarchyDeletionReceiptKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	pointerKey, _ := HierarchyDeletionReceiptCurrentKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	pointerRead, err := repository.store.Get(ctx, pointerKey)
	if err != nil {
		return err
	}
	pointer := HierarchyDeletionTerminalReceiptPointer{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, CurrentAttemptID: entry.CurrentAttemptID,
		CurrentReceiptDigest: receiptDigest,
	}
	pointerRevision := int64(0)
	if pointerRead.Entry != nil {
		pointerRevision = pointerRead.Entry.ModRevision
		var previous HierarchyDeletionTerminalReceiptPointer
		if decodeHierarchyDeletionRecord(
			pointerRead.Entry.Value,
			hierarchyDeletionSmallRecordBytes,
			&previous,
		) != nil ||
			previous.ParentOperationID != operation.Tombstone.OperationID ||
			previous.ChildOperationID != entry.ChildOperationID {
			clear(pointerRead.Entry.Value)
			return corruptHierarchyDeletion()
		}
		clear(pointerRead.Entry.Value)
		if previous.CurrentAttemptID == entry.CurrentAttemptID && previous.CurrentReceiptDigest == receiptDigest {
			return repository.ensureHierarchyDeletionProgress(
				ctx,
				operation,
				action,
				entry,
				receipt,
				receiptKey,
				receiptDigest,
			)
		}
		pointer.PreviousAttemptID = previous.CurrentAttemptID
		pointer.PreviousReceiptDigest = previous.CurrentReceiptDigest
	}
	pointerValue, err := encodeHierarchyDeletionRecord(pointer, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(pointerValue)
	terminalEntry := entry
	terminalEntry.DispatchState = HierarchyDeletionChildTerminal
	terminalEntry.Terminal = &terminal
	terminalEntry.CheckpointDigest = checkpointDigest
	terminalEntry.RetrySharedOwner = HierarchyDeletionRetrySharedOwner{
		Kind: "terminal-receipt", AttemptID: entry.CurrentAttemptID,
		TaskID: task.ID, ReceiptDigest: receiptDigest,
	}
	entryValue, err := encodeHierarchyDeletionRecord(terminalEntry, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return err
	}
	defer clear(entryValue)
	childKey, _ := HierarchyDeletionChildKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	transaction, err := repository.store.Transact(ctx,
		[]Condition{
			{Key: taskKey(task.ID), ModRevision: taskRevision},
			{Key: childKey, ModRevision: entryRevision}, {Key: receiptKey},
			{Key: pointerKey, ModRevision: pointerRevision},
		},
		[]Mutation{
			{Type: MutationPut, Key: receiptKey, Value: receiptValue},
			{Type: MutationPut, Key: pointerKey, Value: pointerValue},
			{Type: MutationPut, Key: childKey, Value: entryValue},
		},
	)
	if err != nil {
		return err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "hierarchy deletion terminal receipt publication changed")
	}
	return repository.ensureHierarchyDeletionProgress(
		ctx,
		operation,
		action,
		terminalEntry,
		receipt,
		receiptKey,
		receiptDigest,
	)
}

func (repository *HierarchyDeletionRepository) ensureHierarchyDeletionProgress(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	entry HierarchyDeletionChildEntry,
	receipt HierarchyDeletionTerminalAttemptReceipt,
	receiptKey string,
	receiptDigest string,
) error {
	receiptRead, err := repository.store.Get(ctx, receiptKey)
	if err != nil {
		return err
	}
	if receiptRead.Entry == nil || hierarchyDeletionBytesDigest(receiptRead.Entry.Value) != receiptDigest {
		if receiptRead.Entry != nil {
			clear(receiptRead.Entry.Value)
		}
		return corruptHierarchyDeletion()
	}
	clear(receiptRead.Entry.Value)
	consumed := HierarchyDeletionConsumedRetry
	if receipt.Terminal == HierarchyDeletionAgentCompleted {
		consumed = HierarchyDeletionConsumedAdvance
	}
	progress := HierarchyDeletionChildProgress{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, ActionOrdinal: action.Ordinal,
		TaskID: receipt.TaskID, AssignmentID: receipt.AssignmentID,
		AttemptGeneration: receipt.AttemptGeneration, Terminal: receipt.Terminal,
		TerminalTaskDigest: receipt.TerminalTaskDigest, ResultDigest: receipt.ResultDigest,
		ErrorDigest: receipt.ErrorDigest, CheckpointDigest: receipt.CheckpointDigest,
		ReceiptRevision: receiptRead.Entry.ModRevision, ReceiptDigest: receiptDigest,
		ConsumedAction: consumed,
	}
	progressValue, err := encodeHierarchyDeletionRecord(progress, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(progressValue)
	progressKey, _ := HierarchyDeletionProgressKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	childKey, _ := HierarchyDeletionChildKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	childRead, err := repository.store.Get(ctx, childKey)
	if err != nil {
		return err
	}
	if childRead.Entry == nil {
		return corruptHierarchyDeletion()
	}
	clear(childRead.Entry.Value)
	transaction, err := repository.store.Transact(ctx,
		[]Condition{
			{Key: receiptKey, ModRevision: receiptRead.Entry.ModRevision},
			{Key: progressKey}, {Key: childKey, ModRevision: childRead.Entry.ModRevision},
		},
		[]Mutation{{Type: MutationPut, Key: progressKey, Value: progressValue}},
	)
	if err != nil {
		return err
	}
	if transaction.Succeeded {
		return nil
	}
	clearKeyValues(transaction.FailureReads)
	existing, getErr := repository.store.Get(ctx, progressKey)
	if getErr != nil {
		return getErr
	}
	if existing.Entry == nil ||
		hierarchyDeletionBytesDigest(existing.Entry.Value) != hierarchyDeletionBytesDigest(progressValue) {
		if existing.Entry != nil {
			clear(existing.Entry.Value)
		}
		return errs.New(errs.KindStateConflict, "hierarchy deletion terminal progress changed")
	}
	clear(existing.Entry.Value)
	return nil
}
