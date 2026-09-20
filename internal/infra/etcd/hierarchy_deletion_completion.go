package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	Terminal           HierarchyDeletionAgentTerminal
	ResultDigest       string
	ErrorDigest        string
	CheckpointDigest   string
}

func (repository *HierarchyDeletionRepository) ConsumeAgentTerminal(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := validateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionAgentTerminal(action, proof, completedAt); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionActiveAction(current, action); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	receiptKey, _ := HierarchyDeletionReceiptKey(current.Tombstone.OperationID, proof.ChildOperationID, proof.AttemptID)
	progressKey, _ := HierarchyDeletionProgressKey(
		current.Tombstone.OperationID,
		proof.ChildOperationID,
		proof.AttemptID,
	)
	if proof.ProgressKey != progressKey {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion progress evidence changed",
		)
	}
	receiptSnapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{receiptKey}, Revision: proof.ReceiptRevision,
	})
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if receiptSnapshot == nil || receiptSnapshot.ReadRevision != proof.ReceiptRevision ||
		len(receiptSnapshot.Values) != 1 || receiptSnapshot.Values[0] == nil ||
		receiptSnapshot.Values[0].ModRevision != proof.ReceiptRevision ||
		hierarchyDeletionBytesDigest(receiptSnapshot.Values[0].Value) != proof.ReceiptDigest {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal evidence changed",
		)
	}
	receiptRead := receiptSnapshot.Values[0]
	defer clear(receiptRead.Value)
	progressRead, err := repository.store.Get(ctx, progressKey)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if progressRead.Entry == nil || hierarchyDeletionBytesDigest(progressRead.Entry.Value) != proof.ProgressDigest {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal progress changed",
		)
	}
	defer clear(progressRead.Entry.Value)
	var receipt HierarchyDeletionTerminalAttemptReceipt
	if decodeHierarchyDeletionRecord(receiptRead.Value, hierarchyDeletionSmallRecordBytes, &receipt) != nil ||
		receipt.ParentOperationID != current.Tombstone.OperationID ||
		receipt.ActionOrdinal != action.Ordinal ||
		receipt.ChildOperationID != proof.ChildOperationID || receipt.AttemptID != proof.AttemptID ||
		receipt.TaskID != proof.TaskID || receipt.AssignmentID != proof.AssignmentID ||
		receipt.AttemptGeneration != proof.AttemptGeneration || receipt.TerminalTaskDigest != proof.TerminalTaskDigest ||
		receipt.Terminal != proof.Terminal || receipt.ResultDigest != proof.ResultDigest ||
		receipt.ErrorDigest != proof.ErrorDigest || receipt.CheckpointDigest != proof.CheckpointDigest {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion terminal receipt does not match proof",
		)
	}
	var progress HierarchyDeletionChildProgress
	wantConsumed := HierarchyDeletionConsumedRetry
	if proof.Terminal == HierarchyDeletionAgentCompleted {
		wantConsumed = HierarchyDeletionConsumedAdvance
	}
	if decodeHierarchyDeletionRecord(progressRead.Entry.Value, hierarchyDeletionSmallRecordBytes, &progress) != nil ||
		progress.ParentOperationID != current.Tombstone.OperationID ||
		progress.ChildOperationID != proof.ChildOperationID || progress.ActionOrdinal != action.Ordinal ||
		progress.TaskID != proof.TaskID || progress.AssignmentID != proof.AssignmentID ||
		progress.AttemptGeneration != proof.AttemptGeneration || progress.Terminal != proof.Terminal ||
		progress.TerminalTaskDigest != proof.TerminalTaskDigest || progress.ResultDigest != proof.ResultDigest ||
		progress.ErrorDigest != proof.ErrorDigest || progress.CheckpointDigest != proof.CheckpointDigest ||
		progress.ReceiptRevision != proof.ReceiptRevision || progress.ReceiptDigest != proof.ReceiptDigest ||
		progress.ConsumedAction != wantConsumed {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion progress does not match proof",
		)
	}
	if proof.Terminal != HierarchyDeletionAgentCompleted {
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

func (repository *HierarchyDeletionRepository) completeAgentAction(
	ctx context.Context,
	current HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	receiptKey string,
	receiptRevision int64,
	progressKey string,
	progressRevision int64,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	actionValue, err := encodeHierarchyDeletionAction(action)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(actionValue)
	completion := HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: current.Tombstone.OperationID,
		DeletionEpoch: current.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: hierarchyDeletionBytesDigest(actionValue), TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: HierarchyDeletionProcedureAgent,
		AgentProof: &HierarchyDeletionAgentCompletionProof{
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
	completionValue, err := encodeHierarchyDeletionRecord(completion, hierarchyDeletionCompletionRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(completionValue)
	nextTombstone, nextFence := advanceHierarchyDeletionCheckpoint(
		current, action, completion.ActionDigest, proof.ReceiptDigest, completedAt,
	)
	return repository.commitHierarchyDeletionCompletion(
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

func (repository *HierarchyDeletionRepository) releaseFailedAgentAction(
	ctx context.Context,
	current HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	receiptKey string,
	receiptRevision int64,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	nextFence := current.Fence
	nextFence.Generation++
	nextFence.ActiveActionOrdinal = nil
	nextFence.ActiveChildOperationID = ""
	nextFence.ActiveChildAttemptID = ""
	nextFence.Dispatch = HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = completedAt
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
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
		return HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return HierarchyDeletionOperation{}, errs.New(
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

func (repository *HierarchyDeletionRepository) commitHierarchyDeletionCompletion(
	ctx context.Context,
	current HierarchyDeletionOperation,
	nextTombstone HierarchyDeletionTombstone,
	nextFence HierarchyDeletionCleanupFence,
	ordinal int64,
	completionValue []byte,
	extraConditions []etcdstore.Condition,
	extraMutations []etcdstore.Mutation,
) (HierarchyDeletionOperation, error) {
	tombstoneKey := HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	completionKey, _ := HierarchyDeletionCompletionKey(current.Tombstone.OperationID, ordinal)
	tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
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
	if err := enforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion completion changed")
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

func advanceHierarchyDeletionCheckpoint(
	current HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	actionDigest string,
	proofDigest string,
	completedAt time.Time,
) (HierarchyDeletionTombstone, HierarchyDeletionCleanupFence) {
	nextTombstone := current.Tombstone
	nextTombstone.Checkpoint.NextOrdinal++
	nextTombstone.Checkpoint.CompletedCount++
	nextTombstone.Checkpoint.CompletedPrefixDigest = hierarchyDeletionFoldDigest(
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
	nextFence.Dispatch = HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = completedAt
	return nextTombstone, nextFence
}

func validateHierarchyDeletionActiveAction(
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) error {
	if operation.Tombstone.Phase != HierarchyDeletionExecuting || operation.Tombstone.PlanCount == nil ||
		operation.Tombstone.PlanDigest == nil || operation.Fence.ActiveActionOrdinal == nil ||
		*operation.Fence.ActiveActionOrdinal != action.Ordinal ||
		operation.Tombstone.Checkpoint.NextOrdinal != action.Ordinal ||
		action.ParentOperationID != operation.Tombstone.OperationID {
		return errs.New(errs.KindStateConflict, "hierarchy deletion action ownership changed")
	}
	return nil
}

func validateHierarchyDeletionAgentTerminal(
	action HierarchyDeletionAction,
	proof HierarchyDeletionAgentTerminalProof,
	completedAt time.Time,
) error {
	if recordcodec.ValidateTimestamp("hierarchy deletion completion", completedAt) != nil ||
		action.ProcedureKind != HierarchyDeletionProcedureAgent || action.AgentProcedure == nil ||
		proof.ChildOperationID != action.AgentProcedure.ChildOperationID || proof.AttemptID == "" ||
		proof.TaskID == "" || proof.AssignmentID == "" || proof.AttemptGeneration <= 0 ||
		proof.ReceiptRevision <= 0 || proof.ProgressKey == "" ||
		!validHierarchyDeletionDigest(proof.ReceiptDigest) || !validHierarchyDeletionDigest(proof.ProgressDigest) ||
		!validHierarchyDeletionDigest(
			proof.TerminalTaskDigest,
		) || !validHierarchyDeletionDigest(proof.CheckpointDigest) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Agent terminal proof is invalid")
	}
	switch proof.Terminal {
	case HierarchyDeletionAgentCompleted:
		if !validHierarchyDeletionDigest(proof.ResultDigest) || proof.ErrorDigest != "" {
			return errs.New(errs.KindValidationFailed, "completed hierarchy deletion proof is invalid")
		}
	case HierarchyDeletionAgentFailed, HierarchyDeletionAgentAborted, HierarchyDeletionAgentTimedOut:
		if !validHierarchyDeletionDigest(proof.ErrorDigest) || proof.ResultDigest != "" {
			return errs.New(errs.KindValidationFailed, "failed hierarchy deletion proof is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion terminal is invalid")
	}
	return nil
}

func hierarchyDeletionBytesDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func hierarchyDeletionFoldDigest(domain string, values ...string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(domain))
	for _, value := range values {
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write([]byte(value))
	}
	return hex.EncodeToString(digest.Sum(nil))
}
