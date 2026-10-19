package app

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/app/controllerbackup"
	"github.com/AlanD20/groundplane/internal/app/hostplatform"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	componentcapability "github.com/AlanD20/groundplane/internal/controller/component"
	desiredrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"
	releaseoperation "github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
)

// NewController constructs the Controller composition root.
func NewController(ctx context.Context, configPath string) (*Controller, error) {
	bootstrap, err := newControllerBootstrapComposition(ctx, configPath)
	if err != nil {
		return nil, err
	}
	cfg := bootstrap.config
	store := bootstrap.store
	keepEtcdLifecycle := false
	defer func() {
		if !keepEtcdLifecycle {
			_ = bootstrap.etcdLifecycle.Close()
		}
	}()
	authority, err := newControllerAuthorityComposition(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	dataServices, err := newControllerDataComposition(ctx, store, authority)
	if err != nil {
		return nil, err
	}
	execution, err := newControllerExecutionComposition(
		ctx,
		cfg,
		store,
		authority,
		dataServices,
		bootstrap.componentCatalog,
	)
	if err != nil {
		return nil, err
	}
	resolverComposition, err := newControllerResolverComposition(
		cfg,
		dataServices.componentRecords,
		dataServices.resolverBaselines,
		dataServices.resolutionProjections,
		authority.tasks,
		authority.intentCoordinator,
		bootstrap.logger,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	agentRuntime := channeltransport.New(
		authority.authenticator,
		authority.tasks,
		execution.backupSecrets,
		dataServices.scriptCheckpoints,
	)
	imageSelections := imagedelivery.SelectionResolver{Images: agentRuntime.Registry, Store: store}
	blueprintReleases, err := blueprintrelease.NewService(
		authority.releaseLedger,
		dataServices.scriptRecords,
		execution.planResolver,
		execution.scriptArtifacts,
		execution.scriptSourceReferences,
		authority.agents,
		imageSelections, agentRuntime.Registry,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Blueprint candidate-release service: %w", err)
	}
	runnerComposition, err := newControllerRunnerComposition(cfg, store,
		authority.runnerRecords,
		authority.hierarchyRecords,
		authority.idempotency,
		authority.intentCoordinator,
		bootstrap.logger,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	releaseGroupMutations, err := releasegroup.NewMutationService(
		authority.releaseGroups,
		authority.hierarchyRecords,
		authority.tasks,
		authority.idempotency,
		authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release group mutation service: %w", err)
	}
	releaseExecutionTimeout, err := cfg.ParsedReleaseExecutionTimeout()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release execution timeout: %w", err)
	}
	releaseOperations, err := releaseoperation.NewService(
		authority.releaseLedger,
		dataServices.serviceRecords,
		authority.releaseGroups,
		authority.idempotency,
		authority.intentCoordinator,
		execution.planResolver,
		dataServices.scriptRecords,
		execution.scriptArtifacts,
		releaseExecutionTimeout,
		authority.agents,
		imageSelections, agentRuntime.Registry,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release operation service: %w", err)
	}
	backups, err := newControllerBackupComposition(
		store,
		bootstrap.logger,
		authority.hierarchyRecords,
		dataServices.backupPolicyRecords,
		dataServices.backupPolicyRepository,
		dataServices.backupRuntimeRecords,
		dataServices.backupKeyRecords,
		execution.attachFactValues,
		authority.intentCoordinator,
		authority.idempotency,
		authority.intentProtector,
		authority.controllerKey,
		execution.backupSecrets, cfg.Storage.VolumeRoot,
		controllerbackup.NewDatabaseVersionObserver(authority.agents, agentRuntime.Registry),
	)
	if err != nil {
		return nil, err
	}
	hierarchyDeletions, err := hierarchydeletion.NewRuntime(
		store,
		authority.idempotency,
		authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy deletion runtime: %w", err)
	}
	networkRecords, err := networketcd.NewRepository(
		authority.hierarchyRecords,
		dataServices.serviceRecords,
		dataServices.zoneRecords,
		dataServices.routeRecords,
		execution.attachRecords,
		authority.tasks,
		authority.idempotency,
		authority.environmentBlueprintRecords,
		execution.attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network persistence adapter: %w", err)
	}
	if err := networkRecords.EnableDesiredRevisions(dataServices.serviceDesiredRevisionRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network desired revision persistence: %w", err)
	}
	componentReads, err := newComponentReadService(
		cfg,
		dataServices.componentRecords,
		dataServices.resolverBaselines,
		dataServices.resolutionProjections,
		networkRecords,
		bootstrap.componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component reads: %w", err)
	}
	routePublisher, err := newNetworkRoutePublisher(store, authority.hierarchyRecords,
		dataServices.serviceDesiredRevisionRecords, networkRecords, bootstrap.componentCatalog)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize native Route publication: %w", err)
	}
	networkCapability, err := networkcontroller.NewEtcdService(
		networkRecords,
		authority.intentCoordinator,
		routePublisher,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network capability: %w", err)
	}
	serviceMutations, err := newControllerServiceMutations(
		dataServices.serviceMutationRepository,
		execution.planResolver,
		execution.attachFactValues,
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	scriptMutations, err := newControllerScriptMutations(
		dataServices.scriptMutationRepository,
		execution.scriptArtifacts,
		authority.agents,
		imageSelections,
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	entryGeneration, err := entrygeneration.NewEntryGenerationService(
		dataServices.secretRecords,
		execution.attachFactValues,
		authority.intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry generation: %w", err)
	}
	entryCreationIdempotency, err := entryoperations.NewCreationIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation idempotency: %w", err)
	}
	entryBulkUpsertIdempotency, err := entryoperations.NewBulkUpsertIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert idempotency: %w", err)
	}
	entryEditIdempotency, err := entryoperations.NewEditIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit idempotency: %w", err)
	}
	entryRemovalIdempotency, err := entryoperations.NewRemovalIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry removal idempotency: %w", err)
	}
	secretCreationIdempotency, err := secrets.NewCreationIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation idempotency: %w", err)
	}
	secretMutations, err := secrets.NewCreationService(
		dataServices.secretReadRepository,
		authority.intentProtector,
		secretCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation service: %w", err)
	}
	connectorComposition, err := newControllerConnectorComposition(
		store,
		authority.hierarchyRecords,
		dataServices.secretRecords,
		dataServices.connectorRecords,
		authority.intentCoordinator,
		authority.idempotency,
		authority.intentProtector,
	)
	if err != nil {
		return nil, err
	}
	secretDeletionIdempotency, err := secrets.NewDeletionIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion idempotency: %w", err)
	}
	secretDeletions, err := secrets.NewDeletionService(dataServices.secretReadRepository, secretDeletionIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion service: %w", err)
	}
	attachMutationRecords, err := attachments.NewRepository(
		authority.hierarchyRecords, dataServices.serviceRecords, execution.attachRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation repository: %w", err)
	}
	attachMutationIdempotency, err := attachments.NewMutationIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation idempotency: %w", err)
	}
	attachMutations, err := attachments.NewMutationService(
		attachMutationRecords,
		execution.attachFactValues,
		attachMutationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation service: %w", err)
	}
	environmentBlueprintIdempotency, err := desiredrevision.NewIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint idempotency: %w", err)
	}
	desiredRevisionRecords, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize desired revision repository: %w", err)
	}
	environmentBlueprintRepository, err := blueprint.NewRepository(
		authority.environmentBlueprintRecords,
		desiredRevisionRecords,
		dataServices.zoneRecords,
		dataServices.serviceRecords,
		dataServices.routeRecords,
		dataServices.entryRecords,
		dataServices.entryValues,
		execution.attachRecords,
		dataServices.componentRecords,
		dataServices.scriptRecords,
		dataServices.backupPolicyRecords,
		dataServices.connectorRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint repositories: %w", err)
	}
	entryRuntime, err := taskplanning.NewEntryRuntimeCapture(authority.releaseLedger)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry runtime capture: %w", err)
	}
	entryDesiredMutations, err := entryoperations.NewDesiredMutationService(
		environmentBlueprintRepository,
		entryGeneration,
		execution.materializationResolver,
		entryCreationIdempotency,
		entryEditIdempotency,
		entryRemovalIdempotency,
		entryRuntime,
		authority.hierarchyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry desired mutation service: %w", err)
	}
	entryBulkUpserts, err := entryoperations.NewBulkUpsertService(entryDesiredMutations, entryBulkUpsertIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert service: %w", err)
	}
	entryMutations, err := entryoperations.NewMutationService(entryDesiredMutations, entryBulkUpserts)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry mutation service: %w", err)
	}
	releaseGroupBlueprints, err := releasegroup.NewReleaseGroupBlueprintPlanner(
		authority.releaseGroups,
		authority.hierarchyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Release Group Blueprint planner: %w", err)
	}
	blueprintAdmission, err := newBlueprintWorkAdmission(store, dataServices.serviceRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Blueprint work admission: %w", err)
	}
	componentConfiguration, err := newComponentConfigurationCapture(store, bootstrap.componentCatalog)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	environmentBlueprints, err := blueprint.NewService(
		cfg.Storage.VolumeRoot, cfg.EnvironmentPool,
		environmentBlueprintRepository, environmentBlueprintIdempotency, execution.materializationResolver,
		releaseGroupBlueprints,
		blueprintReleases,
		entryGeneration,
		execution.attachFactValues,
		bootstrap.componentCatalog, componentConfiguration,
		blueprintAdmission,
		environmentBlueprintRepository, backups.policyKeys, releaseExecutionTimeout,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint service: %w", err)
	}
	backingCreationInputs, err := backingprovisionwork.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing creation capture: %w", err)
	}
	backingServiceCreations, err := backingservices.NewCreationService(
		cfg.Storage.VolumeRoot,
		runnerComposition.pools.Environment,
		environmentBlueprintRepository,
		environmentBlueprintIdempotency,
		authority.intentProtector,
		backingCreationInputs,
		execution.attachFactValues,
		bootstrap.componentCatalog,
		authority.agents,
		imageSelections,
		registryimages.Local{},
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service creation: %w", err)
	}
	componentCredentials, err := componentcapability.NewCredentialReferenceResolver(
		authority.hierarchyRecords,
		dataServices.secretReads,
		secretMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component credentials: %w", err)
	}
	platformComponentMutations, err := controllerdns.NewPlatformMutationService(
		dataServices.componentRecords,
		authority.tasks,
		authority.idempotency,
		authority.intentCoordinator,
		resolverComposition.renderer,
		resolverComposition.renderPlanner,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Platform Component mutations: %w", err)
	}
	componentMutations, err := componentcapability.NewMutationService(
		dataServices.componentRecords, environmentBlueprints,
		componentCredentials, platformComponentMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component mutations: %w", err)
	}
	backingServiceMutations, err := backingservices.NewMutationService(
		dataServices.backingServiceReads,
		serviceMutations,
		backingServiceCreations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service mutations: %w", err)
	}
	volumeMutations, err := configureVolumeMutations(
		environmentBlueprintRepository,
		authority.intentCoordinator,
		authority.idempotency,
		dataServices.volumeReads, dataServices.backupPolicyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Volume mutations: %w", err)
	}
	hierarchyMutations, err := newControllerHierarchyMutations(
		cfg, authority.hierarchyRecords, dataServices.zoneRecords, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	platform, err := hostplatform.New(ctx, hostplatform.Dependencies{
		Config: cfg, Key: authority.controllerKey, Store: store, Agents: authority.agents, Tasks: authority.tasks,
		Intents: authority.intentCoordinator, Idempotency: authority.idempotency, Channel: agentRuntime,
		EtcdEndpoints: bootstrap.etcdEndpoints, Tick: bootstrap.tick, Logger: bootstrap.logger,
		EtcdLifecycle:      bootstrap.etcdLifecycle,
		StorageMaintenance: bootstrap.storageMaintenance,
		Protector:          authority.intentProtector,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform runtime: %w", err)
	}
	imageDiagnostics, err := hostplatform.NewExecutionDiagnostics(store)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, err
	}
	imageDelivery, err := imagedelivery.New(
		registryimages.Local{},
		store,
		authority.tasks,
		authority.idempotency,
		authority.intentCoordinator,
		imageDiagnostics,
		authority.releaseLedger,
	)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, err
	}
	software, err := platform.NewSoftware()
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, err
	}
	wired, err := newControllerHTTPComposition(controllerHTTPDependencies{
		bootstrap:               bootstrap,
		store:                   store,
		authority:               authority,
		dataServices:            dataServices,
		execution:               execution,
		platform:                platform,
		software:                software,
		images:                  imageDelivery,
		agentRuntime:            agentRuntime,
		blueprintReleases:       blueprintReleases,
		runner:                  runnerComposition,
		backup:                  backups,
		hierarchy:               hierarchyMutations,
		connectors:              connectorComposition,
		backingServiceMutations: backingServiceMutations,
		componentReads:          componentReads,
		componentMutations:      componentMutations,
		network:                 networkCapability,
		serviceMutations:        serviceMutations,
		releaseGroupMutations:   releaseGroupMutations,
		releaseOperations:       releaseOperations,
		scriptMutations:         scriptMutations,
		entryMutations:          entryMutations,
		secretMutations:         secretMutations,
		secretDeletions:         secretDeletions,
		volumeMutations:         volumeMutations,
		environmentBlueprints:   environmentBlueprints,
		hierarchyDeletions:      hierarchyDeletions,
		attachMutations:         attachMutations,
	})
	if err != nil {
		return nil, err
	}
	keepEtcdLifecycle = true
	wired.runners = runnerComposition.maintenance
	wired.resolverConfiguration = resolverComposition.configuration
	return wired, nil
}
