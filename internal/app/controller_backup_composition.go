package app

import (
	"log/slog"

	"github.com/AlanD20/groundplane/internal/app/controllerbackup"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// controllerBackupComposition aggregates the services consumed by the root.
// HTTP and scheduler wiring share these exact instances.
type controllerBackupComposition struct {
	policyKeys backupkey.KeyFactory
	policies   *backupcapability.PolicyService
	points     *backupcapability.RecoveryPointReadService
	runs       *backupcapability.BackupRunService
	restores   *backupcapability.RestoreService
	retention  *backupcapability.BackupPruneService
	orphans    *backupcapability.BackupOrphanReconciliationService
	cleanup    *backupcapability.DeletionCleanup
	schedules  *backupcapability.BackupScheduleService
	keys       *backupkey.Service
}

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
	versionObserver backupcapability.DatabaseVersionObserver,
	accepted *acceptedaction.Submitter,
) (*controllerBackupComposition, error) {
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
		accepted,
	)
	if err != nil {
		return nil, err
	}
	backupKeys, err := backupkey.NewService(
		backupPolicyRecords,
		backupKeyRecords,
		foundation.PolicyKeys,
		intentCoordinator,
		idempotency,
		intentProtector,
		accepted,
	)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, wrapControllerRunError("initialize backup key service", err))
	}
	backupRestores, err := backupcapability.NewRestoreService(
		backupRuntimeRecords,
		intentCoordinator,
		idempotency,
		backupSecrets,
		attachFactValues.ResolveBackupIdentity,
		backupcapability.NewBackupServiceFactResolver(store),
		volumeRoot,
		versionObserver,
		accepted,
	)
	if err != nil {
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
		cleanup:    foundation.Cleanup,
		schedules:  foundation.Schedules,
		keys:       backupKeys,
	}, nil
}

func (composition *controllerBackupComposition) AcceptedPreparations() []taskdependencies.Registration {
	registrations := composition.runs.AcceptedPreparations()
	registrations = append(registrations, composition.retention.AcceptedPreparations()...)
	registrations = append(registrations, composition.restores.AcceptedPreparations()...)
	return append(registrations, composition.keys.AcceptedPreparations()...)
}
