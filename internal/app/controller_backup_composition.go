package app

import (
	"errors"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/app/controllerbackup"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// controllerBackupComposition aggregates the services consumed by the root.
// HTTP and scheduler wiring share these exact instances.
// Product decisions remain in the capability packages.
// Child-package construction details do not leak into other root modules.
type controllerBackupComposition struct {
	policyKeys backupkey.KeyFactory
	policies   *backupcapability.PolicyService
	points     *backupcapability.RecoveryPointReadService
	runs       *backupcapability.BackupRunService
	restores   *backupcapability.RestoreService
	retention  *backupcapability.BackupPruneService
	orphans    *backupcapability.BackupOrphanReconciliationService
	schedules  *backupcapability.BackupScheduleService
	keys       *backupkey.Service
}

// newControllerBackupComposition preserves the established initialization order.
// The foundation stops immediately before Backup key mutation construction.
// Key mutation consumes the same policy key factory returned by the foundation.
// Restore construction remains last because it depends on the completed runtime.
// Each failure stage retains ownership of closing the store.
// Selected early stages preserve a store-close error in the returned error.
// Later stages keep their established best-effort close behavior.
// No handler, scheduler or task runner starts while this function is executing.
func newControllerBackupComposition(
	store etcdstore.Store,
	logger *slog.Logger,
	hierarchyRecords *etcd.HierarchyRepository,
	backupPolicyRecords *etcd.BackupPolicyRepository,
	backupPolicyRepository *backupcapability.PolicyRepository,
	backupRuntimeRecords *etcd.BackupRuntimeRepository,
	backupKeyRecords *backuppolicy.KeyRepository,
	attachFactValues *attachments.FactService,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
	intentProtector *secretvalue.Protector,
	controllerKey *ageinfra.ControllerKey,
	backupSecrets *backupcapability.BackupSecretResolver,
	volumeRoot string,
) (*controllerBackupComposition, error) {
	// The foundation owns policy, read, run and maintenance construction.
	// It returns only concrete services needed by later root wiring.
	// Its constructor preserves the pre-key failure cleanup sequence.
	foundation, err := controllerbackup.NewFoundation(
		store,
		logger,
		hierarchyRecords,
		backupPolicyRepository,
		backupRuntimeRecords,
		attachFactValues,
		intentCoordinator,
		idempotency,
		intentProtector,
		controllerKey,
	)
	if err != nil {
		return nil, err
	}
	// Key mutation remains in the root because the native task runner consumes it.
	// It shares the foundation's policy-key implementation.
	// Its initialization failure preserves the joined store-close error.
	backupKeys, err := backupkey.NewService(
		backupPolicyRecords,
		backupKeyRecords,
		foundation.PolicyKeys,
		intentCoordinator,
		idempotency,
		intentProtector,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup key service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	// Restore is composed after the key path, matching the established order.
	// It shares runtime persistence and the Attach fact resolver with Backup runs.
	// The root returns only after every Backup service is ready for wiring.
	backupRestores, err := backupcapability.NewRestoreService(
		backupRuntimeRecords,
		intentCoordinator,
		idempotency,
		backupSecrets,
		attachFactValues.ResolveBackupIdentity,
		backupcapability.NewBackupServiceFactResolver(store),
		volumeRoot,
	)
	if err != nil {
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &controllerBackupComposition{
		policyKeys: foundation.PolicyKeys,
		policies:   foundation.Policies,
		points:     foundation.Points,
		runs:       foundation.Runs,
		restores:   backupRestores,
		retention:  foundation.Retention,
		orphans:    foundation.Orphans,
		schedules:  foundation.Schedules,
		keys:       backupKeys,
	}, nil
}
