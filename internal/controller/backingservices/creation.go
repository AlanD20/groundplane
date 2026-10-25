package backingservices

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"time"

	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingcatalog"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingServiceCreateRoute = "/backing-services"

type backingServiceCreationRepository interface {
	desiredrevision.Repository
	ResolveResourceHost(context.Context) (workexecution.Placement, error)
	GetEnvironmentPoolRegistry(
		context.Context,
	) (etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry], error)
	ClaimBackingServiceCreationStage(
		context.Context,
		etcd.BackingServiceCreationStage,
	) (etcdstore.Versioned[etcd.BackingServiceCreationStage], error)
	PublishBackingServiceWithWork(
		context.Context,
		etcd.BackingServiceCreation,
	) (etcd.IdempotencyTransactionResult, error)
}

type backingServiceHookInputs interface {
	SealBackingHookTaskInputs(
		context.Context,
		string,
		string,
		backinghook.Configuration,
	) (*taskconfiguration.BackingHookEncryptedInputs, error)
}

type CreationService struct {
	volumeRoot       string
	environmentPool  netip.Prefix
	repository       backingServiceCreationRepository
	idempotency      *desiredrevision.Idempotency
	protector        *secretvalue.Protector
	workInputs       *backingprovisionwork.Repository
	agents           *agentregistration.Repository
	images           workloadseal.Resolver
	imageRegistry    creationImageRegistry
	hookInputs       backingServiceHookInputs
	componentCatalog []componentrender.EnvironmentComponentRegistration
	now              func() time.Time
}

func NewCreationService(
	volumeRoot string,
	environmentPool netip.Prefix,
	repository backingServiceCreationRepository,
	idempotency *desiredrevision.Idempotency,
	protector *secretvalue.Protector,
	workInputs *backingprovisionwork.Repository,
	hookInputs backingServiceHookInputs,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
	agents *agentregistration.Repository,
	images workloadseal.Resolver,
	imageRegistry creationImageRegistry,
) (*CreationService, error) {
	if volumeRoot == "" || !environmentPool.IsValid() || repository == nil || idempotency == nil || protector == nil ||
		workInputs == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service creation dependencies are incomplete")
	}
	if err := componentrender.ValidateEnvironmentComponentCatalog(componentCatalog); err != nil {
		return nil, err
	}
	return &CreationService{
		volumeRoot: volumeRoot, environmentPool: environmentPool,
		repository: repository, idempotency: idempotency, protector: protector,
		workInputs: workInputs, hookInputs: hookInputs,
		agents: agents, images: images, imageRegistry: imageRegistry,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
		now:              time.Now,
	}, nil
}

func (service *CreationService) CreateBackingService(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	ctx = imagefence.WithScope(ctx)
	if idempotencyKey == "" {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Idempotency-Key is required",
		)
	}
	adapter, ok := adapters.Get(input.Adapter)
	if !ok {
		return idempotencyrecord.IdempotencyResponse{}, errs.Newf(
			errs.KindValidationFailed, "unsupported backing-service adapter %q", input.Adapter,
		)
	}
	hooks := taskplanning.BackingHookConfigurationFromAPI(input.Hooks)
	if hooks != nil && !adapter.Custom() {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"managed backing-service adapters do not accept hooks",
		)
	}
	if hooks != nil {
		if err := backinghook.ValidateConfiguration(*hooks); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), core.BackingAuthentication(input.Authentication),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var spec adapters.CreationSpec
	if adapter.Custom() {
		if input.AdapterVersion != "" {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindValidationFailed,
				"Custom backing services do not select a managed server version",
			)
		}
		spec, err = adapters.CustomBackingCreationSpec(input.Slug, input.Image)
	} else {
		spec, err = adapters.BackingCreationSpec(input.Adapter, input.AdapterVersion, authentication)
		if err == nil && input.Image != "" {
			if !backingcatalog.ValidImage(input.Adapter, input.AdapterVersion, input.Image) {
				return idempotencyrecord.IdempotencyResponse{}, errs.New(
					errs.KindValidationFailed, "image must match the selected database family and server version")
			}
			spec.Image = input.Image
		}
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	input.Authentication = string(authentication)
	requestBytes, err := json.Marshal(input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	requestDigest := sha256.Sum256(requestBytes)
	requestSHA256 := hex.EncodeToString(requestDigest[:])
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: backingServiceCreateRoute, Key: idempotencyKey,
	}
	bundle := core.BlueprintBundle{
		RootPath: "backing-service.yaml", ComposeSources: []string{"backing-service.yaml"},
		Files: []core.BlueprintFile{{Path: "backing-service.yaml", Content: append([]byte(nil), requestBytes...)}},
	}
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPost, Route: backingServiceCreateRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
	}, bundle)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	if response, existing, err := service.replayCreation(ctx, locator, evidence); existing || err != nil {
		return response, err
	}
	createdAt := service.now().UTC()
	stage, err := service.repository.ClaimBackingServiceCreationStage(ctx, etcd.BackingServiceCreationStage{
		Locator: locator, RequestSHA256: requestSHA256,
		ProjectID: ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
		TaskID: ids.New(ids.KindTask), CreatedAt: createdAt,
	})
	if err != nil {
		// A concurrent acceptance can win after our initial replay read. Stage
		// admission compares marker absence, so resolve that outcome without
		// creating another inert staging record or choosing fresh image bytes.
		if response, existing, replayErr := service.replayCreation(ctx, locator, evidence); existing ||
			replayErr != nil {
			return response, replayErr
		}
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.createBackingServiceFromStage(ctx, input, bundle, stage, adapter, spec, evidence)
}

func (service *CreationService) replayCreation(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence desiredrevision.Evidence,
) (idempotencyrecord.IdempotencyResponse, bool, error) {
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil || !existing {
		return idempotencyrecord.IdempotencyResponse{}, existing, err
	}
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, true, errs.New(
			errs.KindInternal,
			"backing-service replay resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), true, nil
}

func (service *CreationService) createBackingServiceFromStage(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	bundle core.BlueprintBundle,
	stage etcdstore.Versioned[etcd.BackingServiceCreationStage],
	adapter adapters.Adapter,
	spec adapters.CreationSpec,
	evidence desiredrevision.Evidence,
) (response idempotencyrecord.IdempotencyResponse, resultErr error) {
	authentication := core.BackingAuthentication(input.Authentication)
	blueprintLocator := stage.Record.Locator
	blueprintLocator.ScopeKind = idempotencyrecord.IdempotencyScopeEnvironment
	blueprintLocator.ScopeID = stage.Record.EnvironmentID
	claim, err := desiredrevision.Claim(ctx, service.repository, desiredrevision.ClaimInput{
		EnvironmentID: stage.Record.EnvironmentID, CandidateTaskID: stage.Record.TaskID,
		Locator: blueprintLocator, Intent: evidence.Durable,
		MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
			return service.idempotency.MatchesStaged(ctx, evidence, existing)
		},
		BaselineHeadRevision: 0, SourceKind: blueprints.EnvironmentBlueprintSourceApply,
		RenderGeneration: 1, CreatedAt: stage.Record.CreatedAt,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if claim.TaskID != stage.Record.TaskID || !claim.CreatedAt.Equal(stage.Record.CreatedAt) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Backing-service desired claim changed creation authority",
		)
	}

	project := hierarchyrecord.ProjectRecord{
		ID: stage.Record.ProjectID, Slug: input.Slug, Name: input.Name,
		Description: input.Description, Kind: hierarchyrecord.ProjectKindBacking,
	}
	poolRegistry, err := service.repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	reservedPools, networkPool, err := poolRegistry.Record.Reserve(
		service.environmentPool, stage.Record.EnvironmentID, input.NetworkPool,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	poolRegistry.Record = reservedPools
	environment, err := hierarchyrecord.NewProvisioningEnvironment(
		service.volumeRoot, project, stage.Record.EnvironmentID, "main", networkPool,
		stage.Record.TaskID, stage.Record.CreatedAt,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}

	zone, err := zonerecord.NewRecord(environment.ID, core.Zone{
		ID: allocator.New(ids.KindNetwork), Name: input.Zone.Name, Subnet: input.Zone.Subnet,
		Internal: input.Zone.Internal, OwnerKind: core.ZoneOwnerBackingProject, OwnerID: project.ID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	entries, generations, secrets, secretValues, resolved, err := service.backingCreationEntries(
		ctx, project.ID, environment.ID, spec, allocator, stage.Record.CreatedAt,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for key := range resolved {
		defer func(key string) { resolved[key] = "" }(key)
	}

	var volume *core.Volume
	var volumeID string
	if spec.HasVolume() {
		volumeID = allocator.New(ids.KindVolume)
		resolvedVolume := core.Volume{ID: volumeID, Slug: spec.VolumeSlug, Key: spec.VolumeKey}
		volume = &resolvedVolume
	}
	serviceID := allocator.New(ids.KindService)
	sealedImage, err := service.sealCreationImage(ctx, spec.Image)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !adapter.Custom() {
		spec.Image = sealedImage.reference
	}
	var postgresRelease *backingpostgresrelease.Record
	if input.Adapter == "postgres" {
		release, releaseErr := postgres16protocol.CompiledManagedRelease()
		if releaseErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, releaseErr
		}
		// Database selection is operator-owned; only the backup tool artifacts
		// come from the authenticated installation catalog.
		release.DatabaseImage = spec.Image
		selected, releaseErr := backingpostgresrelease.New(environment.ID, serviceID, release)
		if releaseErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, releaseErr
		}
		postgresRelease = &selected
	}
	mounts := []core.Mount(nil)
	if volume != nil {
		mounts = []core.Mount{{Volume: volumeID, Mount: spec.MountPath}}
	}
	desiredService := core.Service{
		ID: serviceID, Name: spec.ServiceName, Image: spec.Image,
		Zones: []string{
			zone.Desired.Name,
		}, Healthcheck: core.Healthcheck{Test: append([]string(nil), spec.HealthCommand...)},
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureLeaveActive, Replicas: 1,
		Command: append([]string(nil), spec.Command...),
		Mounts:  mounts,
		Expose:  append([]string(nil), spec.Expose...), Restart: "unless-stopped",
		Adapter: input.Adapter, AdapterVersion: input.AdapterVersion, Authentication: authentication,
		FactsPrefix: adapter.FactsPrefix(), Label: input.Name,
		Hooks: taskplanning.BackingHookConfigurationFromAPI(input.Hooks),
	}
	serviceRecord, err := servicerecord.NewServiceRecord(environment.ID, desiredService, zone.Desired.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if postgresRelease != nil {
		serviceRecord.Runtime.PostgresToolsImage = postgresRelease.Release.Image
	}
	entryDesired := make([]core.EnvEntry, len(entries))
	for index := range entries {
		entryDesired[index] = entries[index].Entry
	}
	volumes := map[string]core.Volume(nil)
	if volume != nil {
		volumes = map[string]core.Volume{volume.Key: *volume}
	}
	environmentProjection := core.Environment{
		ID: environment.ID, ProjectID: project.ID, Name: environment.Name,
		NetworkPool: environment.NetworkPool, VolumeDir: environment.VolumeDir,
		Zones:      map[string]core.Zone{zone.Desired.Name: zone.Desired},
		Services:   map[string]core.Service{desiredService.Name: desiredService},
		Components: nil, Volumes: volumes,
		Entries: entryDesired, CreatedAt: environment.CreatedAt,
	}
	baseProject := backingComposeProject(spec, serviceID, zone.Desired, volume, environment)
	workload := baseProject.Services[spec.ServiceName]
	workload.Image = sealedImage.localID
	baseProject.Services[spec.ServiceName] = workload
	componentProjection, err := composerender.ProjectEnvironmentComponents(
		baseProject,
		environmentProjection,
		service.componentCatalog,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	identities := composeidentity.Snapshot{
		Services: []composeidentity.Resource{{ID: serviceID, Name: spec.ServiceName}},
		Networks: []composeidentity.Resource{{ID: zone.Desired.ID, Name: zone.Desired.Name}},
	}
	if postgresRelease != nil {
		identities.Services[0].PostgresToolsImage = postgresRelease.Release.Image
	}
	if volume != nil {
		identities.Volumes = []composeidentity.Resource{{ID: volumeID, Name: volume.Key}}
	}
	identities.Services = append(identities.Services, componentProjection.Services...)
	planID := allocator.Named(ids.KindPlan, "execution-plan")
	artifactID := allocator.Named(ids.KindConfig, "compose-artifact")
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: artifactID,
		ProjectOwnerKind: composerender.ComposeProjectOwnerBacking,
		ProjectID:        project.ID, EnvironmentID: environment.ID, PlanID: planID,
		RenderGeneration: 1, AuthorizedVolumeDir: environment.VolumeDir, Identities: identities,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for _, workload := range artifact.Services {
		if workload.ServiceId == serviceID {
			workload.ImageOs = "linux"
			workload.ImageArchitecture = sealedImage.architecture
			workload.ImageVariant = sealedImage.variant
		}
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	captured, err := service.captureCreation(ctx, creationCapture{PlanID: planID, Environment: environment,
		Service: desiredService, Artifact: artifact, Entries: entries, Resolved: resolved, Allocator: allocator})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer captured.Clear()
	task, _, err := captured.Input.Work()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	published := false
	defer func() {
		if !published {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, captured.AbandonUnpublished(cleanup))
		}
	}()
	volumeSlugs := map[string]string(nil)
	volumeMounts := []projectionrecord.EnvironmentServiceVolumeMount(nil)
	if volume != nil {
		volumeSlugs = map[string]string{volume.Key: volume.Slug}
		volumeMounts = []projectionrecord.EnvironmentServiceVolumeMount{
			{ServiceID: serviceID, VolumeID: volumeID, Target: spec.MountPath},
		}
	}
	normalizedCompose, err := composerender.MarshalNormalizedEnvironmentProject(
		backingDesiredComposeProject(spec, zone.Desired, volume, environment),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desiredInput, err := projectionrecord.NewEnvironmentDesiredInput(
		environment.ID, claim.RevisionID, claim.RenderGeneration,
		core.BlueprintDesiredInput{
			NormalizedCompose: normalizedCompose,
			NetworkPool:       environment.NetworkPool,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projection := buildBackingServiceCreationProjection(
		environment.ID, task.ID, identities, volumeSlugs, volumeMounts,
		artifactValue, normalizedCompose, zone, serviceRecord, entries,
	)
	projectionEvidence, err := desiredrevision.PreflightProjection(projection)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	revision := desiredrevision.BlueprintRevision(environment.ID, task.ID, stage.Record.CreatedAt, bundle)
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &revision, DesiredInput: desiredInput,
		DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.BackingServiceCreated{
		BackingService: apiTypes.BackingService{
			Authentication: input.Authentication,
			ProjectID:      project.ID, EnvironmentID: environment.ID,
			ServiceID: serviceID, BackingNetworkID: zone.Desired.ID,
		},
		TaskID: task.ID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response = idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body:        responseBody,
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: stage.Record.Locator, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	result, publishErr := service.repository.PublishBackingServiceWithWork(ctx, etcd.BackingServiceCreation{
		VolumeRoot: service.volumeRoot, Stage: stage, PoolRegistry: poolRegistry,
		Project: project, Environment: environment, Components: nil,
		Zone: zone, Service: serviceRecord, Secrets: secrets, SecretValues: secretValues,
		Entries: entries, EntryValues: generations, Claim: claim,
		Revision:   blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.ID, RevisionID: task.ID},
		Projection: projection, Task: task, Work: captured, Marker: marker,
		PostgresRelease: postgresRelease,
	})
	if publishErr != nil {
		if !isUnknownBackingServiceCreationOutcome(publishErr) {
			return idempotencyrecord.IdempotencyResponse{}, publishErr
		}
		resolved, resolveErr := service.idempotency.ResolveUnknown(ctx, stage.Record.Locator, evidence, publishErr)
		if resolveErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, resolveErr
		}
		return requestidempotency.CloneResponse(resolved.Response), nil
	}
	outcome, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch outcome.Kind {
	case requestidempotency.ResolutionApplied:
		published = true
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		published = true
		return requestidempotency.CloneResponse(outcome.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Backing-service creation resolution is invalid",
		)
	}
}

func isUnknownBackingServiceCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
