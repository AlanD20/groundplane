package hierarchydeletionfinalization

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Effects struct {
	fixedInputDigest string
	conditions       []etcdstore.Condition
	mutations        []etcdstore.Mutation
	values           [][]byte
}

func (repository *Preparer) Prepare(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionAttachFinalize:
		return repository.prepareHierarchyDeletionAttachFinalizer(ctx, operation, action)
	case hierarchydeletion.HierarchyDeletionRecoveryPointRemove,
		hierarchydeletion.HierarchyDeletionOrphanObjectRemove:
		return repository.prepareHierarchyDeletionBackupRemoteFinalizer(ctx, operation, action)
	case hierarchydeletion.HierarchyDeletionBackupPolicyFinalize:
		return repository.prepareHierarchyDeletionBackupPolicyFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionKeyMaterialRemove:
		return repository.prepareHierarchyDeletionBackupKeyFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionBackupScheduleFinalize,
		hierarchydeletion.HierarchyDeletionBackupRetentionFinalize:
		return repository.prepareHierarchyDeletionBackupExactFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionBackupDueFinalize:
		return repository.prepareHierarchyDeletionBackupDueFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionBackupSourceFinalize:
		return repository.prepareHierarchyDeletionBackupSourceFinalizer(ctx, action)
	case hierarchydeletion.HierarchyDeletionBackupRunDetach,
		hierarchydeletion.HierarchyDeletionBackupRestoreDetach,
		hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach:
		return repository.prepareHierarchyDeletionBackupHistoryDetach(ctx, action)
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
		return Effects{}, errs.Newf(
			errs.KindValidationFailed,
			"hierarchy deletion Controller finalizer %q is unsupported before plan execution",
			action.ActionKind,
		)
	}
}
