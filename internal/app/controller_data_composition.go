package app

import (
	"context"
	"errors"
	"fmt"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	taskcheckpoint "github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	backingrecords "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"

	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"

	scriptoperations "github.com/AlanD20/groundplane/internal/controller/scripts"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	serviceoperations "github.com/AlanD20/groundplane/internal/controller/services"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerDataComposition struct {
	entryValues                   *entryvalues.Repository
	entryRecords                  *etcd.EntryRepository
	entryReads                    handlers.EntryReader
	serviceRecords                *etcd.ServiceRepository
	routeRecords                  *etcd.RouteRepository
	scriptRecords                 *etcd.ScriptRepository
	scriptReads                   handlers.ScriptReader
	zoneRecords                   *etcd.ZoneRepository
	serviceDesiredRevisionRecords *desiredrevisionstore.Repository
	serviceMutationRepository     *serviceoperations.MutationRepository
	scriptMutationRepository      *scriptoperations.MutationRepository
	componentRecords              *etcd.ComponentRepository
	resolverBaselines             *resolverbaseline.Repository
	resolutionProjections         *resolutionrecord.Repository
	backingServiceReads           *backingservices.ReadService
	secretRecords                 *etcd.SecretRepository
	secretReadRepository          *secrets.Repository
	secretReads                   handlers.SecretReader
	connectorRecords              *etcd.ConnectorRepository
	connectorReads                handlers.ConnectorReader
	volumeReads                   *volume.ReadService
	backupPolicyRecords           *etcd.BackupPolicyRepository
	backupKeyRecords              *backuppolicy.KeyRepository
	backupRuntimeRecords          *etcd.BackupRuntimeRepository
	backupCheckpoints             *backupcapability.BackupCheckpointService
	scriptCheckpoints             *taskcheckpoint.ScriptCheckpointService
	backupPolicyRepository        *backupcapability.PolicyRepository
}

func newControllerDataComposition(
	ctx context.Context, store etcdstore.Store, authority controllerAuthorityComposition,
) (controllerDataComposition, error) {
	entryValues, err := entryvalues.New(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Entry value generation repository: %w", err)
	}
	entryRecords, err := etcd.NewEntryRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Entry repository: %w", err)
	}
	entryReadRepository, err := entryoperations.NewReadRepository(authority.hierarchyRecords, entryRecords, entryValues)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Entry read repositories: %w", err)
	}
	entryReads, err := entryoperations.NewReadService(entryReadRepository, authority.intentProtector)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Entry reads: %w", err)
	}
	serviceRecords, err := etcd.NewServiceRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Service repository: %w", err)
	}
	routeRecords, err := etcd.NewRouteRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Route repository: %w", err)
	}
	scriptRecords, err := etcd.NewScriptRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Script repository: %w", err)
	}
	scriptReadRepository, err := scriptoperations.NewReadRepository(authority.hierarchyRecords, serviceRecords, scriptRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Script read repositories: %w", err)
	}
	scriptReads, err := scriptoperations.NewReadService(scriptReadRepository)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Script reads: %w", err)
	}
	zoneRecords, err := etcd.NewZoneRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Zone repository: %w", err)
	}
	serviceDesiredRevisionRecords, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Service desired revision repository: %w", err)
	}
	serviceMutationRepository, err := serviceoperations.NewMutationRepository(
		authority.hierarchyRecords, serviceRecords, zoneRecords, serviceDesiredRevisionRecords, authority.releaseLedger,
	)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Service mutation repositories: %w", err)
	}
	scriptMutationRepository, err := scriptoperations.NewMutationRepository(
		authority.hierarchyRecords, serviceRecords, scriptRecords, authority.releaseLedger,
	)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Script mutation repositories: %w", err)
	}
	componentRecords, err := etcd.NewComponentRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Component repository: %w", err)
	}
	resolverBaselines, err := resolverbaseline.New(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize host resolver baseline repository: %w", err)
	}
	resolutionProjections, err := resolutionrecord.New(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize host-resolution projection repository: %w", err)
	}
	platformComponents, err := etcd.DefaultPlatformComponents(detectTailnetDelegationDefault())
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize platform Components: %w", err)
	}
	if _, err := componentRecords.EnsurePlatformComponents(ctx, platformComponents); err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: bootstrap platform Components: %w", err)
	}
	backingServiceRecords, err := backingrecords.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize backing-service repository: %w", err)
	}
	backingServiceReads, err := backingservices.NewReadService(backingServiceRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize backing-service reads: %w", err)
	}
	secretRecords, err := etcd.NewSecretRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Secret repository: %w", err)
	}
	secretReadRepository, err := secrets.NewRepository(authority.hierarchyRecords, secretRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Secret read repositories: %w", err)
	}
	secretReads, err := secrets.NewReadService(secretReadRepository, authority.intentProtector)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Secret reads: %w", err)
	}
	connectorRecords, err := etcd.NewConnectorRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Connector repository: %w", err)
	}
	connectorReadRepository, err := connectors.NewReadRepository(authority.hierarchyRecords, connectorRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Connector read repositories: %w", err)
	}
	connectorReads, err := connectors.NewReadService(connectorReadRepository)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Connector reads: %w", err)
	}
	volumeReads, err := volume.NewReadService(authority.hierarchyRecords)
	if err != nil {
		closeErr := store.Close()
		return controllerDataComposition{}, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize volume reads", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicyRecords, err := etcd.NewBackupPolicyRepository(store)
	if err != nil {
		closeErr := store.Close()
		return controllerDataComposition{}, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy repository", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupKeyRecords, err := backuppolicy.NewKeyRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize backup key repository: %w", err)
	}
	backupRuntimeRecords, err := etcd.NewBackupRuntimeRepository(store)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize backup runtime repository: %w", err)
	}
	backupCheckpoints, err := backupcapability.NewBackupCheckpointService(backupRuntimeRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Backup checkpoint service: %w", err)
	}
	scriptCheckpoints, err := taskcheckpoint.NewScriptCheckpointService(scriptRecords)
	if err != nil {
		_ = store.Close()
		return controllerDataComposition{}, fmt.Errorf("controller: initialize Script checkpoint service: %w", err)
	}
	backupPolicyRepository, err := backupcapability.NewDurableBackupPolicyRepository(backupPolicyRecords)
	if err != nil {
		closeErr := store.Close()
		return controllerDataComposition{}, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy repository adapter", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	return controllerDataComposition{
		entryValues:                   entryValues,
		entryRecords:                  entryRecords,
		entryReads:                    entryReads,
		serviceRecords:                serviceRecords,
		routeRecords:                  routeRecords,
		scriptRecords:                 scriptRecords,
		scriptReads:                   scriptReads,
		zoneRecords:                   zoneRecords,
		serviceDesiredRevisionRecords: serviceDesiredRevisionRecords,
		serviceMutationRepository:     serviceMutationRepository,
		scriptMutationRepository:      scriptMutationRepository,
		componentRecords:              componentRecords,
		resolverBaselines:             resolverBaselines,
		resolutionProjections:         resolutionProjections,
		backingServiceReads:           backingServiceReads,
		secretRecords:                 secretRecords,
		secretReadRepository:          secretReadRepository,
		secretReads:                   secretReads,
		connectorRecords:              connectorRecords,
		connectorReads:                connectorReads,
		volumeReads:                   volumeReads,
		backupPolicyRecords:           backupPolicyRecords,
		backupKeyRecords:              backupKeyRecords,
		backupRuntimeRecords:          backupRuntimeRecords,
		backupCheckpoints:             backupCheckpoints,
		scriptCheckpoints:             scriptCheckpoints,
		backupPolicyRepository:        backupPolicyRepository,
	}, nil
}
