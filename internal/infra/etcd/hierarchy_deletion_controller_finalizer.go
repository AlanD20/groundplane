package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	hierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type hierarchyDeletionControllerEffects struct {
	fixedInputDigest string
	conditions       []etcdstore.Condition
	mutations        []etcdstore.Mutation
	values           [][]byte
}

func (repository *HierarchyDeletionRepository) CompleteControllerAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion Controller completion", completedAt) != nil ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Controller action is invalid",
		)
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionActiveAction(current, action); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if action.TargetID == current.Tombstone.TargetID &&
		hierarchyDeletionRootFinalizerMatches(current.Tombstone.TargetKind, action.ActionKind) {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"root hierarchy deletion finalizer requires Task acknowledgement",
		)
	}
	effects, err := repository.prepareHierarchyDeletionControllerEffects(ctx, current, action)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clearByteSlices(effects.values)
	expected, err := bindHierarchyDeletionControllerProcedure(
		HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		hierarchydeletionplanning.HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: effects.fixedInputDigest, BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion finalizer template changed",
		)
	}
	actionValue, err := hierarchydeletion.EncodeHierarchyDeletionAction(action)
	if err != nil {
		return HierarchyDeletionOperation{}, err
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
	completionValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(completion, hierarchydeletion.HierarchyDeletionCompletionRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(completionValue)
	nextTombstone, nextFence := advanceHierarchyDeletionCheckpoint(
		current, action, completion.ActionDigest, action.ControllerProcedure.MutationTemplateDigest, completedAt,
	)
	return repository.commitHierarchyDeletionCompletion(
		ctx, current, nextTombstone, nextFence, action.Ordinal, completionValue,
		effects.conditions, effects.mutations,
	)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionControllerEffects(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionReleaseGroupRemove:
		return repository.prepareHierarchyDeletionReleaseGroupFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionServiceRemove:
		return repository.prepareHierarchyDeletionServiceFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionEntryRemove:
		return repository.prepareHierarchyDeletionEntryFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionRouteRemove:
		return repository.prepareHierarchyDeletionRouteFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionComponentRemove:
		return repository.prepareHierarchyDeletionComponentFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionScriptRemove:
		return repository.prepareHierarchyDeletionScriptFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionZoneRemove:
		return repository.prepareHierarchyDeletionZoneFinalizer(ctx, operation, action)
	case hierarchydeletion.HierarchyDeletionConnectorFinalize:
		return repository.prepareHierarchyDeletionConnectorFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionProjectSecretRemove:
		return repository.prepareHierarchyDeletionSecretFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionReservationRelease:
		return repository.prepareHierarchyDeletionReservationFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionRunnerLocalRemove:
		return repository.prepareHierarchyDeletionRunnerFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionEnvironmentFinalize:
		return repository.prepareHierarchyDeletionEnvironmentFinalizer(ctx, operation, action)
	case hierarchydeletion.HierarchyDeletionProjectFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionTenantFinalize:
		return repository.prepareHierarchyDeletionTenantFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionBackingServiceFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	default:
		return hierarchyDeletionControllerEffects{}, errs.Newf(
			errs.KindValidationFailed,
			"hierarchy deletion Controller finalizer %q is unsupported before plan execution",
			action.ActionKind,
		)
	}
}
