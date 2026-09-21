package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) AgentTerminalProof(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (*HierarchyDeletionAgentTerminalProof, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	childKey, _ := hierarchydeletion.HierarchyDeletionChildKey(operation.Tombstone.OperationID, action.AgentProcedure.ChildOperationID)
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
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	taskRead, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(entry.CurrentTaskID))
	if err != nil {
		return nil, err
	}
	if taskRead.Entry == nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	task, err := DecodeTaskRecord(taskRead.Entry.Value)
	clear(taskRead.Entry.Value)
	if err != nil || task.ID != entry.CurrentTaskID || task.OperationID != entry.ChildOperationID {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if !taskjournal.IsTerminalTaskStatus(task.Status) {
		return nil, nil
	}
	if err := repository.ensureHierarchyDeletionTerminalReceipt(
		ctx, operation, action, entry, childRead.Entry.ModRevision, task, taskRead.Entry.ModRevision,
	); err != nil {
		return nil, err
	}
	receiptKey, _ := hierarchydeletion.HierarchyDeletionReceiptKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	progressKey, _ := hierarchydeletion.HierarchyDeletionProgressKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{receiptKey, progressKey}})
	if err != nil {
		return nil, err
	}
	if evidence == nil || len(evidence.Values) != 2 || evidence.Values[0] == nil || evidence.Values[1] == nil {
		if evidence != nil {
			etcdstore.ClearValues(evidence.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion terminal receipt is not visible")
	}
	defer etcdstore.ClearValues(evidence.Values)
	var receipt hierarchydeletion.HierarchyDeletionTerminalAttemptReceipt
	var progress hierarchydeletion.HierarchyDeletionChildProgress
	if hierarchydeletion.DecodeHierarchyDeletionRecord(evidence.Values[0].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &receipt) != nil ||
		hierarchydeletion.DecodeHierarchyDeletionRecord(evidence.Values[1].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &progress) != nil ||
		receipt.ParentOperationID != operation.Tombstone.OperationID || receipt.ActionOrdinal != action.Ordinal ||
		receipt.ChildOperationID != entry.ChildOperationID || receipt.AttemptID != entry.CurrentAttemptID ||
		progress.ReceiptRevision != evidence.Values[0].ModRevision ||
		progress.ReceiptDigest != hierarchyDeletionBytesDigest(evidence.Values[0].Value) {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
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
	action hierarchydeletion.HierarchyDeletionAction,
	entry hierarchydeletion.HierarchyDeletionChildEntry,
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
	if entry.DispatchState == hierarchydeletion.HierarchyDeletionChildTerminal {
		if entry.Terminal == nil || entry.RetrySharedOwner.Kind != "terminal-receipt" ||
			entry.RetrySharedOwner.AttemptID != entry.CurrentAttemptID ||
			entry.RetrySharedOwner.TaskID != task.ID || entry.RetrySharedOwner.ReceiptDigest == "" {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		receiptKey, _ := hierarchydeletion.HierarchyDeletionReceiptKey(
			operation.Tombstone.OperationID, entry.ChildOperationID, entry.CurrentAttemptID,
		)
		receiptRead, getErr := repository.store.Get(ctx, receiptKey)
		if getErr != nil {
			return getErr
		}
		if receiptRead.Entry == nil ||
			hierarchyDeletionBytesDigest(receiptRead.Entry.Value) != entry.RetrySharedOwner.ReceiptDigest {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		var receipt hierarchydeletion.HierarchyDeletionTerminalAttemptReceipt
		if hierarchydeletion.DecodeHierarchyDeletionRecord(receiptRead.Entry.Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &receipt) != nil ||
			receipt.ParentOperationID != operation.Tombstone.OperationID ||
			receipt.ChildOperationID != entry.ChildOperationID || receipt.AttemptID != entry.CurrentAttemptID ||
			receipt.TaskID != task.ID || receipt.Terminal != *entry.Terminal {
			clear(receiptRead.Entry.Value)
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		clear(receiptRead.Entry.Value)
		return repository.ensureHierarchyDeletionProgress(
			ctx, operation, action, entry, receipt, receiptKey, entry.RetrySharedOwner.ReceiptDigest,
		)
	}
	generation, err := strconv.ParseInt(task.Params[taskjournal.TaskHierarchyDeletionGenerationParam], 10, 64)
	if err != nil || generation <= 0 || task.Params[taskjournal.TaskHierarchyDeletionAttemptParam] != entry.CurrentAttemptID ||
		task.Params[taskjournal.TaskHierarchyDeletionChildParam] != entry.ChildOperationID ||
		task.Params[taskjournal.TaskHierarchyDeletionParentParam] != operation.Tombstone.OperationID {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	taskValue, err := EncodeTaskRecord(task)
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
	receipt := hierarchydeletion.HierarchyDeletionTerminalAttemptReceipt{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, ActionOrdinal: action.Ordinal,
		AttemptID: entry.CurrentAttemptID, TaskID: task.ID,
		AssignmentID: task.TerminalAssignment.AssignmentID, AttemptGeneration: generation,
		Terminal: terminal, TerminalTaskDigest: terminalTaskDigest,
		ResultDigest: resultDigest, ErrorDigest: errorDigest, CheckpointDigest: checkpointDigest,
		AgentAckRevision: taskRevision, PublishedAt: *task.FinishedAt,
	}
	receiptValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(receipt, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(receiptValue)
	receiptDigest := hierarchyDeletionBytesDigest(receiptValue)
	receiptKey, _ := hierarchydeletion.HierarchyDeletionReceiptKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	pointerKey, _ := hierarchydeletion.HierarchyDeletionReceiptCurrentKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	pointerRead, err := repository.store.Get(ctx, pointerKey)
	if err != nil {
		return err
	}
	pointer := hierarchydeletion.HierarchyDeletionTerminalReceiptPointer{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, CurrentAttemptID: entry.CurrentAttemptID,
		CurrentReceiptDigest: receiptDigest,
	}
	pointerRevision := int64(0)
	if pointerRead.Entry != nil {
		pointerRevision = pointerRead.Entry.ModRevision
		var previous hierarchydeletion.HierarchyDeletionTerminalReceiptPointer
		if hierarchydeletion.DecodeHierarchyDeletionRecord(
			pointerRead.Entry.Value,
			hierarchydeletion.HierarchyDeletionSmallRecordBytes,
			&previous,
		) != nil ||
			previous.ParentOperationID != operation.Tombstone.OperationID ||
			previous.ChildOperationID != entry.ChildOperationID {
			clear(pointerRead.Entry.Value)
			return hierarchydeletion.CorruptHierarchyDeletion()
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
	pointerValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(pointer, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(pointerValue)
	terminalEntry := entry
	terminalEntry.DispatchState = hierarchydeletion.HierarchyDeletionChildTerminal
	terminalEntry.Terminal = &terminal
	terminalEntry.CheckpointDigest = checkpointDigest
	terminalEntry.RetrySharedOwner = hierarchydeletion.HierarchyDeletionRetrySharedOwner{
		Kind: "terminal-receipt", AttemptID: entry.CurrentAttemptID,
		TaskID: task.ID, ReceiptDigest: receiptDigest,
	}
	entryValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(terminalEntry, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		return err
	}
	defer clear(entryValue)
	childKey, _ := hierarchydeletion.HierarchyDeletionChildKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
			{Key: childKey, ModRevision: entryRevision}, {Key: receiptKey},
			{Key: pointerKey, ModRevision: pointerRevision},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: receiptKey, Value: receiptValue},
			{Type: etcdstore.MutationPut, Key: pointerKey, Value: pointerValue},
			{Type: etcdstore.MutationPut, Key: childKey, Value: entryValue},
		},
	)
	if err != nil {
		return err
	}
	etcdstore.ClearValues(transaction.FailureReads)
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
	action hierarchydeletion.HierarchyDeletionAction,
	entry hierarchydeletion.HierarchyDeletionChildEntry,
	receipt hierarchydeletion.HierarchyDeletionTerminalAttemptReceipt,
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
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	clear(receiptRead.Entry.Value)
	consumed := hierarchydeletion.HierarchyDeletionConsumedRetry
	if receipt.Terminal == hierarchydeletion.HierarchyDeletionAgentCompleted {
		consumed = hierarchydeletion.HierarchyDeletionConsumedAdvance
	}
	progress := hierarchydeletion.HierarchyDeletionChildProgress{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: entry.ChildOperationID, ActionOrdinal: action.Ordinal,
		TaskID: receipt.TaskID, AssignmentID: receipt.AssignmentID,
		AttemptGeneration: receipt.AttemptGeneration, Terminal: receipt.Terminal,
		TerminalTaskDigest: receipt.TerminalTaskDigest, ResultDigest: receipt.ResultDigest,
		ErrorDigest: receipt.ErrorDigest, CheckpointDigest: receipt.CheckpointDigest,
		ReceiptRevision: receiptRead.Entry.ModRevision, ReceiptDigest: receiptDigest,
		ConsumedAction: consumed,
	}
	progressValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(progress, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return err
	}
	defer clear(progressValue)
	progressKey, _ := hierarchydeletion.HierarchyDeletionProgressKey(
		operation.Tombstone.OperationID,
		entry.ChildOperationID,
		entry.CurrentAttemptID,
	)
	childKey, _ := hierarchydeletion.HierarchyDeletionChildKey(operation.Tombstone.OperationID, entry.ChildOperationID)
	childRead, err := repository.store.Get(ctx, childKey)
	if err != nil {
		return err
	}
	if childRead.Entry == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	clear(childRead.Entry.Value)
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: receiptKey, ModRevision: receiptRead.Entry.ModRevision},
			{Key: progressKey}, {Key: childKey, ModRevision: childRead.Entry.ModRevision},
		},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: progressKey, Value: progressValue}},
	)
	if err != nil {
		return err
	}
	if transaction.Succeeded {
		return nil
	}
	etcdstore.ClearValues(transaction.FailureReads)
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
