package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareAdmission compares the frozen provider object and its ownership in the
// caller's assignment or Start transaction. It never selects an object or performs
// remote effects; those require the committed execution attempt.
func (repository *Repository) PrepareAdmission(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) ([]keyvalue.Condition, error) {
	if repository == nil || repository.store == nil ||
		action.ParentOperationID != operation.Tombstone.OperationID ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController ||
		action.ControllerProcedure == nil || action.ControllerProcedure.FixedInputRevision != action.TargetRevision ||
		operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		operation.Fence.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen ||
		operation.Fence.ParentOperationID != action.ParentOperationID ||
		operation.Fence.DeletionEpoch != operation.Tombstone.DeletionEpoch ||
		operation.TombstoneRevision <= 0 || operation.FenceRevision <= 0 {
		return nil, errs.New(errs.KindStateConflict, "backup object removal no longer permits execution")
	}
	local, err := repository.readLocalAuthority(ctx, action)
	if err != nil {
		return nil, err
	}
	defer clear(local.primary.Value)
	if err := verifyRemoteTemplate(action, local.fixedDigest); err != nil {
		return nil, err
	}
	conditions, err := operationConditions(operation)
	if err != nil {
		return nil, err
	}
	return append(conditions, local.conditions...), nil
}
