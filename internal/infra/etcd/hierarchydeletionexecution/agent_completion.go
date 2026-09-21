package hierarchydeletionexecution

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyDeletionAgentTerminalProof struct {
	ChildOperationID   string
	AttemptID          string
	TaskID             string
	AssignmentID       string
	AttemptGeneration  int64
	ReceiptRevision    int64
	ReceiptDigest      string
	ProgressKey        string
	ProgressDigest     string
	TerminalTaskDigest string
	Terminal           hierarchydeletion.HierarchyDeletionAgentTerminal
	ResultDigest       string
	ErrorDigest        string
	CheckpointDigest   string
}

func (repository *Executor) ConsumeAgentTerminal(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	completedAt time.Time,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionAgentTerminal(action, proof, completedAt); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if err := ValidateHierarchyDeletionActiveAction(current, action); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	receiptKey, _ := hierarchydeletion.HierarchyDeletionReceiptKey(
		current.Tombstone.OperationID,
		proof.ChildOperationID,
		proof.AttemptID,
	)
	progressKey, _ := hierarchydeletion.HierarchyDeletionProgressKey(
		current.Tombstone.OperationID,
		proof.ChildOperationID,
		proof.AttemptID,
	)
	if proof.ProgressKey != progressKey {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion progress evidence changed",
		)
	}
	receiptSnapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{receiptKey}, Revision: proof.ReceiptRevision,
	})
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if receiptSnapshot == nil || receiptSnapshot.ReadRevision != proof.ReceiptRevision ||
		len(receiptSnapshot.Values) != 1 || receiptSnapshot.Values[0] == nil ||
		receiptSnapshot.Values[0].ModRevision != proof.ReceiptRevision ||
		hierarchydeletion.HierarchyDeletionBytesDigest(receiptSnapshot.Values[0].Value) != proof.ReceiptDigest {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal evidence changed",
		)
	}
	receiptRead := receiptSnapshot.Values[0]
	defer clear(receiptRead.Value)
	progressRead, err := repository.store.Get(ctx, progressKey)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if progressRead.Entry == nil ||
		hierarchydeletion.HierarchyDeletionBytesDigest(progressRead.Entry.Value) != proof.ProgressDigest {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal progress changed",
		)
	}
	defer clear(progressRead.Entry.Value)
	var receipt hierarchydeletion.HierarchyDeletionTerminalAttemptReceipt
	if hierarchydeletion.DecodeHierarchyDeletionRecord(receiptRead.Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &receipt) != nil ||
		receipt.ParentOperationID != current.Tombstone.OperationID ||
		receipt.ActionOrdinal != action.Ordinal ||
		receipt.ChildOperationID != proof.ChildOperationID || receipt.AttemptID != proof.AttemptID ||
		receipt.TaskID != proof.TaskID ||
		receipt.AssignmentID != proof.AssignmentID ||
		receipt.AttemptGeneration != proof.AttemptGeneration ||
		receipt.TerminalTaskDigest != proof.TerminalTaskDigest ||
		receipt.Terminal != proof.Terminal ||
		receipt.ResultDigest != proof.ResultDigest ||
		receipt.ErrorDigest != proof.ErrorDigest ||
		receipt.CheckpointDigest != proof.CheckpointDigest {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal receipt does not match proof",
		)
	}
	var progress hierarchydeletion.HierarchyDeletionChildProgress
	wantConsumed := hierarchydeletion.HierarchyDeletionConsumedRetry
	if proof.Terminal == hierarchydeletion.HierarchyDeletionAgentCompleted {
		wantConsumed = hierarchydeletion.HierarchyDeletionConsumedAdvance
	}
	if hierarchydeletion.DecodeHierarchyDeletionRecord(progressRead.Entry.Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &progress) != nil ||
		progress.ParentOperationID != current.Tombstone.OperationID ||
		progress.ChildOperationID != proof.ChildOperationID || progress.ActionOrdinal != action.Ordinal ||
		progress.TaskID != proof.TaskID || progress.AssignmentID != proof.AssignmentID ||
		progress.AttemptGeneration != proof.AttemptGeneration ||
		progress.Terminal != proof.Terminal ||
		progress.TerminalTaskDigest != proof.TerminalTaskDigest ||
		progress.ResultDigest != proof.ResultDigest ||
		progress.ErrorDigest != proof.ErrorDigest ||
		progress.CheckpointDigest != proof.CheckpointDigest ||
		progress.ReceiptRevision != proof.ReceiptRevision ||
		progress.ReceiptDigest != proof.ReceiptDigest ||
		progress.ConsumedAction != wantConsumed {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion progress does not match proof",
		)
	}
	if proof.Terminal != hierarchydeletion.HierarchyDeletionAgentCompleted {
		return repository.releaseFailedAgentAction(
			ctx,
			current,
			action,
			receiptKey,
			receiptRead.ModRevision,
			completedAt,
		)
	}
	return repository.completeAgentAction(
		ctx, current, action, proof, receiptKey, receiptRead.ModRevision,
		progressKey, progressRead.Entry.ModRevision, completedAt,
	)
}

func (repository *Executor) completeAgentAction(
	ctx context.Context,
	current hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	receiptKey string,
	receiptRevision int64,
	progressKey string,
	progressRevision int64,
	completedAt time.Time,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	actionValue, err := hierarchydeletion.EncodeHierarchyDeletionAction(action)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(actionValue)
	completion := hierarchydeletion.HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: current.Tombstone.OperationID,
		DeletionEpoch: current.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: hierarchydeletion.HierarchyDeletionBytesDigest(actionValue), TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: hierarchydeletion.HierarchyDeletionProcedureAgent,
		AgentProof: &hierarchydeletion.HierarchyDeletionAgentCompletionProof{
			ChildOperationID: proof.ChildOperationID, AttemptID: proof.AttemptID,
			TaskID: proof.TaskID, AssignmentID: proof.AssignmentID,
			AttemptGeneration: proof.AttemptGeneration, ReceiptRevision: proof.ReceiptRevision,
			ReceiptDigest: proof.ReceiptDigest, ProgressKey: proof.ProgressKey,
			ProgressDigest: proof.ProgressDigest, TerminalTaskDigest: proof.TerminalTaskDigest,
			Terminal: proof.Terminal, ResultDigest: proof.ResultDigest,
			ErrorDigest: proof.ErrorDigest, CheckpointDigest: proof.CheckpointDigest,
		},
		CompletedAt: completedAt,
	}
	completionValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		completion,
		hierarchydeletion.HierarchyDeletionCompletionRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(completionValue)
	nextTombstone, nextFence := AdvanceHierarchyDeletionCheckpoint(
		current, action, completion.ActionDigest, proof.ReceiptDigest, completedAt,
	)
	return repository.CommitActionCompletion(
		ctx,
		current,
		nextTombstone,
		nextFence,
		action.Ordinal,
		completionValue,
		[]etcdstore.Condition{
			{Key: receiptKey, ModRevision: receiptRevision},
			{Key: progressKey, ModRevision: progressRevision},
		},
		nil,
	)
}

func (repository *Executor) releaseFailedAgentAction(
	ctx context.Context,
	current hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	receiptKey string,
	receiptRevision int64,
	completedAt time.Time,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	nextFence := current.Fence
	nextFence.Generation++
	nextFence.ActiveActionOrdinal = nil
	nextFence.ActiveChildOperationID = ""
	nextFence.ActiveChildAttemptID = ""
	nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = completedAt
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: fenceKey, ModRevision: current.FenceRevision},
			{Key: receiptKey, ModRevision: receiptRevision},
		},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue}},
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion action ownership changed",
		)
	}
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	current.FailedCount++
	current.UpdatedAt = completedAt
	return current, nil
}

func (repository *Executor) CommitActionCompletion(
	ctx context.Context,
	current hierarchydeletion.HierarchyDeletionOperation,
	nextTombstone hierarchydeletion.HierarchyDeletionTombstone,
	nextFence hierarchydeletion.HierarchyDeletionCleanupFence,
	ordinal int64,
	completionValue []byte,
	extraConditions []etcdstore.Condition,
	extraMutations []etcdstore.Mutation,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(
		string(current.Tombstone.TargetKind),
		current.Tombstone.TargetID,
	)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	completionKey, _ := hierarchydeletion.HierarchyDeletionCompletionKey(current.Tombstone.OperationID, ordinal)
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextTombstone,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	conditions := []etcdstore.Condition{
		{Key: tombstoneKey, ModRevision: current.TombstoneRevision},
		{Key: fenceKey, ModRevision: current.FenceRevision},
		{Key: completionKey},
	}
	conditions = append(conditions, extraConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: completionKey, Value: completionValue},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
	}
	mutations = append(mutations, extraMutations...)
	if err := hierarchydeletion.EnforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion completion changed",
		)
	}
	current.Tombstone = nextTombstone
	current.TombstoneRevision = transaction.Revision
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	current.SucceededCount++
	current.PlanCursor = nextTombstone.Checkpoint.NextOrdinal
	current.UpdatedAt = nextFence.UpdatedAt
	return current, nil
}

func AdvanceHierarchyDeletionCheckpoint(
	current hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	actionDigest string,
	proofDigest string,
	completedAt time.Time,
) (hierarchydeletion.HierarchyDeletionTombstone, hierarchydeletion.HierarchyDeletionCleanupFence) {
	nextTombstone := current.Tombstone
	nextTombstone.Checkpoint.NextOrdinal++
	nextTombstone.Checkpoint.CompletedCount++
	nextTombstone.Checkpoint.CompletedPrefixDigest = hierarchydeletion.HierarchyDeletionFoldDigest(
		"groundplane-deletion-completion-prefix-v1", nextTombstone.Checkpoint.CompletedPrefixDigest,
		actionDigest, proofDigest,
	)
	nextTombstone.Checkpoint.ActiveChildOperationID = ""
	nextTombstone.Checkpoint.ActiveChildAttemptID = ""
	nextFence := current.Fence
	nextFence.Generation++
	nextFence.ActiveActionOrdinal = nil
	nextFence.ActiveChildOperationID = ""
	nextFence.ActiveChildAttemptID = ""
	nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = completedAt
	return nextTombstone, nextFence
}

func ValidateHierarchyDeletionActiveAction(
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) error {
	if operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		operation.Tombstone.PlanCount == nil ||
		operation.Tombstone.PlanDigest == nil ||
		operation.Fence.ActiveActionOrdinal == nil ||
		*operation.Fence.ActiveActionOrdinal != action.Ordinal ||
		operation.Tombstone.Checkpoint.NextOrdinal != action.Ordinal ||
		action.ParentOperationID != operation.Tombstone.OperationID {
		return errs.New(errs.KindStateConflict, "hierarchy deletion action ownership changed")
	}
	return nil
}

func validateHierarchyDeletionAgentTerminal(
	action hierarchydeletion.HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	completedAt time.Time,
) error {
	if recordcodec.ValidateTimestamp("hierarchy deletion completion", completedAt) != nil ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureAgent || action.AgentProcedure == nil ||
		proof.ChildOperationID != action.AgentProcedure.ChildOperationID || proof.AttemptID == "" ||
		proof.TaskID == "" || proof.AssignmentID == "" || proof.AttemptGeneration <= 0 ||
		proof.ReceiptRevision <= 0 || proof.ProgressKey == "" ||
		!hierarchydeletion.ValidHierarchyDeletionDigest(
			proof.ReceiptDigest,
		) || !hierarchydeletion.ValidHierarchyDeletionDigest(proof.ProgressDigest) ||
		!hierarchydeletion.ValidHierarchyDeletionDigest(
			proof.TerminalTaskDigest,
		) || !hierarchydeletion.ValidHierarchyDeletionDigest(proof.CheckpointDigest) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Agent terminal proof is invalid")
	}
	switch proof.Terminal {
	case hierarchydeletion.HierarchyDeletionAgentCompleted:
		if !hierarchydeletion.ValidHierarchyDeletionDigest(proof.ResultDigest) || proof.ErrorDigest != "" {
			return errs.New(errs.KindValidationFailed, "completed hierarchy deletion proof is invalid")
		}
	case hierarchydeletion.HierarchyDeletionAgentFailed,
		hierarchydeletion.HierarchyDeletionAgentAborted,
		hierarchydeletion.HierarchyDeletionAgentTimedOut:
		if !hierarchydeletion.ValidHierarchyDeletionDigest(proof.ErrorDigest) || proof.ResultDigest != "" {
			return errs.New(errs.KindValidationFailed, "failed hierarchy deletion proof is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion terminal is invalid")
	}
	return nil
}
