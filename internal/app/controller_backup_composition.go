package app

import (
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"log/slog"
)

type controllerBackupComposition struct {
	policyKeys backupkey.KeyFactory
	policies   *backupcapability.PolicyService
	points     *backupcapability.RecoveryPointReadService
	runs       *backupcapability.BackupRunService
	schedules  *backupcapability.BackupScheduleService
	keys       *backupkey.Service
}

// Failure cleanup stays with the original initialization stage, including
// which stages retain an etcd shutdown error in the returned error.
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
) (*controllerBackupComposition, error) {
	backupPolicyIdempotency, err := backupcapability.NewDurableBackupPolicyIdempotency(intentCoordinator, idempotency)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy idempotency", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicyKeys, err := backupcapability.NewAgeBackupPolicyKeyFactory(intentProtector)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy key factory", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicies, err := backupcapability.NewBackupPolicyService(
		backupPolicyRepository,
		backupPolicyKeys,
		backupPolicyIdempotency,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPointReads, err := backupcapability.NewRecoveryPointReadService(
		hierarchyRecords,
		backupRuntimeRecords,
		secretvalue.NewControllerKeyCipher(controllerKey),
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize recovery point reads", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupRunRepository, err := backupcapability.NewDurableBackupRunRepository(
		backupRuntimeRecords,
		attachFactValues.ResolveBackupIdentity,
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
	backupSchedules, err := backupcapability.NewBackupScheduleService(backupscheduling.New(store), backupRuns, logger)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup scheduler: %w", err)
	}
	backupKeys, err := backupkey.NewService(
		backupPolicyRecords,
		backupKeyRecords,
		backupPolicyKeys,
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
	return &controllerBackupComposition{
		policyKeys: backupPolicyKeys, policies: backupPolicies, points: backupPointReads,
		runs: backupRuns, schedules: backupSchedules, keys: backupKeys,
	}, nil
}
