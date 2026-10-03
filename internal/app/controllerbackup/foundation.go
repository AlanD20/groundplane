// Package controllerbackup constructs the shared foundation for the
// Controller's Backup capability services.
package controllerbackup

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/controller/attachments"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Foundation contains the services established before key mutation and Restore
// composition. The caller retains those later ordering and wiring decisions.
type Foundation struct {
	PolicyKeys backupkey.KeyFactory
	Policies   *backupcapability.PolicyService
	Points     *backupcapability.RecoveryPointReadService
	Runs       *backupcapability.BackupRunService
	Retention  *backupcapability.BackupPruneService
	Orphans    *backupcapability.BackupOrphanReconciliationService
	Cleanup    *backupcapability.DeletionCleanup
	Schedules  *backupcapability.BackupScheduleService
}

// NewFoundation preserves the original initialization and cleanup order.
func NewFoundation(
	store etcdstore.Store,
	logger *slog.Logger,
	hierarchyRecords *etcd.HierarchyRepository,
	backupPolicyRepository *backupcapability.PolicyRepository,
	backupRuntimeRecords *etcd.BackupRuntimeRepository,
	attachFactValues *attachments.FactService,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
	intentProtector *secretvalue.Protector,
	controllerKey *ageinfra.ControllerKey,
) (*Foundation, error) {
	backupPolicyIdempotency, err := backupcapability.NewDurableBackupPolicyIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, closeStoreError(store, "initialize backup policy idempotency", err)
	}
	backupPolicyKeys, err := backupcapability.NewAgeBackupPolicyKeyFactory(intentProtector)
	if err != nil {
		return nil, closeStoreError(store, "initialize backup policy key factory", err)
	}
	backupPolicies, err := backupcapability.NewBackupPolicyService(
		backupPolicyRepository,
		backupPolicyKeys,
		backupPolicyIdempotency,
	)
	if err != nil {
		return nil, closeStoreError(store, "initialize backup policy service", err)
	}
	backupPointReads, err := backupcapability.NewRecoveryPointReadService(
		hierarchyRecords,
		backupRuntimeRecords,
		secretvalue.NewControllerKeyCipher(controllerKey),
	)
	if err != nil {
		return nil, closeStoreError(store, "initialize recovery point reads", err)
	}
	backupRunRepository, err := backupcapability.NewDurableBackupRunRepository(
		backupRuntimeRecords,
		attachFactValues.ResolveBackupIdentity,
		store,
		intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup run repository: %w", err)
	}
	backupRunIdempotency, err := backupcapability.NewDurableBackupRunIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup run idempotency: %w", err)
	}
	backupRuns := backupcapability.NewBackupRunService(
		backupRunRepository,
		backupcapability.BackupRunPlanBuilderFunc(backupcapability.BuildBackupRunPlan),
		backupRunIdempotency,
	)
	backupRetention, err := backupcapability.NewBackupPruneService(backupRuntimeRecords, backupRunIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup retention: %w", err)
	}
	orphanCredentials, err := backupcapability.NewBackupOrphanCredentialStore(
		connectors.NewReader(store),
		hierarchy.NewReader(store),
		secrets.NewReader(store),
		intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	backupOrphans, err := backupcapability.NewBackupOrphanReconciliationService(backupRuntimeRecords, orphanCredentials)
	if err != nil {
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	backupCleanup, err := backupcapability.NewDeletionCleanup(
		hierarchydeletionbackupcleanup.NewRepository(store),
		orphanCredentials,
	)
	if err != nil {
		return nil, closeStoreError(store, "initialize backup deletion cleanup", err)
	}
	backupSchedules, err := backupcapability.NewBackupScheduleService(backupscheduling.New(store), backupRuns, logger)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup scheduler: %w", err)
	}
	return &Foundation{
		PolicyKeys: backupPolicyKeys,
		Policies:   backupPolicies,
		Points:     backupPointReads,
		Runs:       backupRuns,
		Retention:  backupRetention,
		Orphans:    backupOrphans,
		Cleanup:    backupCleanup,
		Schedules:  backupSchedules,
	}, nil
}

func closeStoreError(store etcdstore.Store, operation string, err error) error {
	return errs.Wrap(errs.KindInternal, errors.Join(
		fmt.Errorf("controller: %s: %w", operation, err),
		wrapCloseError(store.Close()),
	))
}

func wrapCloseError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("controller: close etcd: %w", err)
}
