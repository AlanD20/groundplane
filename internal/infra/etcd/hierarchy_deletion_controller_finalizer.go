package etcd

import (
	"context"
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
	action HierarchyDeletionAction,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion Controller completion", completedAt) != nil ||
		action.ProcedureKind != HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
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
		HierarchyDeletionControllerFinalizerInput{
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
		Executor: HierarchyDeletionProcedureController,
		ControllerProof: &HierarchyDeletionControllerCompletionProof{
			Finalizer:                   action.ControllerProcedure.Finalizer,
			FixedInputRevision:          action.ControllerProcedure.FixedInputRevision,
			CompareTemplateDigest:       action.ControllerProcedure.CompareTemplateDigest,
			MutationTemplateDigest:      action.ControllerProcedure.MutationTemplateDigest,
			PostconditionTemplateDigest: action.ControllerProcedure.PostconditionTemplateDigest,
		},
		CompletedAt: completedAt,
	}
	completionValue, err := encodeHierarchyDeletionRecord(completion, hierarchyDeletionCompletionRecordBytes)
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
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	switch action.ActionKind {
	case HierarchyDeletionReleaseGroupRemove:
		return repository.prepareHierarchyDeletionReleaseGroupFinalizer(ctx, action)
	case HierarchyDeletionServiceRemove:
		return repository.prepareHierarchyDeletionServiceFinalizer(ctx, action)
	case HierarchyDeletionEntryRemove:
		return repository.prepareHierarchyDeletionEntryFinalizer(ctx, action)
	case HierarchyDeletionRouteRemove:
		return repository.prepareHierarchyDeletionRouteFinalizer(ctx, action)
	case HierarchyDeletionComponentRemove:
		return repository.prepareHierarchyDeletionComponentFinalizer(ctx, action)
	case HierarchyDeletionScriptRemove:
		return repository.prepareHierarchyDeletionScriptFinalizer(ctx, action)
	case HierarchyDeletionZoneRemove:
		return repository.prepareHierarchyDeletionZoneFinalizer(ctx, operation, action)
	case HierarchyDeletionConnectorFinalize:
		return repository.prepareHierarchyDeletionConnectorFinalizer(ctx, action)
	case HierarchyDeletionProjectSecretRemove:
		return repository.prepareHierarchyDeletionSecretFinalizer(ctx, action)
	case HierarchyDeletionReservationRelease:
		return repository.prepareHierarchyDeletionReservationFinalizer(ctx, action)
	case HierarchyDeletionRunnerLocalRemove:
		return repository.prepareHierarchyDeletionRunnerFinalizer(ctx, action)
	case HierarchyDeletionEnvironmentFinalize:
		return repository.prepareHierarchyDeletionEnvironmentFinalizer(ctx, operation, action)
	case HierarchyDeletionProjectFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	case HierarchyDeletionTenantFinalize:
		return repository.prepareHierarchyDeletionTenantFinalizer(ctx, action)
	case HierarchyDeletionBackingServiceFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	default:
		return hierarchyDeletionControllerEffects{}, errs.Newf(
			errs.KindValidationFailed,
			"hierarchy deletion Controller finalizer %q is unsupported before plan execution",
			action.ActionKind,
		)
	}
}
