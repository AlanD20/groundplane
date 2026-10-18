package hierarchydeletionfinalization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareCaptured binds current resource comparisons to the finalizer sealed in
// the removal plan. The caller owns the returned buffers and must commit these
// effects together with its execution result, never in a separate transaction.
func (repository *Preparer) PrepareCaptured(ctx context.Context, operation hierarchydeletion.HierarchyDeletionOperation, action hierarchydeletion.HierarchyDeletionAction) (Effects, error) {
	if action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
		return Effects{}, errs.New(errs.KindStateConflict, "removal action is not a captured Controller finalizer")
	}
	effects, err := repository.Prepare(ctx, operation, action)
	if err != nil {
		return Effects{}, err
	}
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
		keyvalue.ClearByteSlices(effects.Values())
		return Effects{}, errs.New(errs.KindStateConflict, "removal finalizer no longer matches its captured resource")
	}
	return effects, nil
}
