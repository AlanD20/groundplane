package hierarchydeletionexecution

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	hierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *Executor) CompleteControllerAction(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	completedAt time.Time,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion Controller completion", completedAt) != nil ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Controller action is invalid",
		)
	}
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if err := ValidateHierarchyDeletionActiveAction(current, action); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if action.TargetID == current.Tombstone.TargetID &&
		HierarchyDeletionRootFinalizerMatches(current.Tombstone.TargetKind, action.ActionKind) {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"root hierarchy deletion finalizer requires Task acknowledgement",
		)
	}
	effects, err := hierarchydeletionfinalization.NewPreparer(repository.store).Prepare(ctx, current, action)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer etcdstore.ClearByteSlices(effects.Values())
	expected, err := hierarchydeletionplanning.BindHierarchyDeletionControllerProcedure(
		hierarchydeletionplanning.HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		hierarchydeletionplanning.HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: effects.FixedInputDigest(), BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion finalizer template changed",
		)
	}
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
		Executor: hierarchydeletion.HierarchyDeletionProcedureController,
		ControllerProof: &hierarchydeletion.HierarchyDeletionControllerCompletionProof{
			Finalizer:                   action.ControllerProcedure.Finalizer,
			FixedInputRevision:          action.ControllerProcedure.FixedInputRevision,
			CompareTemplateDigest:       action.ControllerProcedure.CompareTemplateDigest,
			MutationTemplateDigest:      action.ControllerProcedure.MutationTemplateDigest,
			PostconditionTemplateDigest: action.ControllerProcedure.PostconditionTemplateDigest,
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
		current, action, completion.ActionDigest, action.ControllerProcedure.MutationTemplateDigest, completedAt,
	)
	return repository.CommitActionCompletion(
		ctx, current, nextTombstone, nextFence, action.Ordinal, completionValue,
		effects.Conditions(), effects.Mutations(),
	)
}
