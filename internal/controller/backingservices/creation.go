package backingservices

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/http"
	"net/netip"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const backingServiceCreateRoute = "/backing-services"

type backingServiceCreationRepository interface {
	desiredrevision.Repository
	GetEnvironmentPoolRegistry(context.Context) (etcd.Versioned[etcd.EnvironmentPoolRegistry], error)
	ClaimBackingServiceCreationStage(
		context.Context,
		etcd.BackingServiceCreationStage,
	) (etcd.Versioned[etcd.BackingServiceCreationStage], error)
	PublishBackingServiceWithTask(
		context.Context,
		etcd.BackingServiceCreation,
	) (etcd.IdempotencyTransactionResult, error)
}

type backingServiceHookInputs interface {
	SealBackingHookTaskInputs(context.Context, string, string, backinghook.Configuration) (*etcd.BackingHookEncryptedInputs, error)
}

type CreationService struct {
	volumeRoot       string
	environmentPool  netip.Prefix
	repository       backingServiceCreationRepository
	idempotency      *desiredrevision.Idempotency
	protector        *secretvalue.Protector
	plans            *controller.TaskPlanResolver
	hookInputs       backingServiceHookInputs
	componentCatalog []controller.EnvironmentComponentRegistration
	now              func() time.Time
}

func NewCreationService(
	volumeRoot string,
	environmentPool netip.Prefix,
	repository backingServiceCreationRepository,
	idempotency *desiredrevision.Idempotency,
	protector *secretvalue.Protector,
	plans *controller.TaskPlanResolver,
	hookInputs backingServiceHookInputs,
	componentCatalog []controller.EnvironmentComponentRegistration,
) (*CreationService, error) {
	if volumeRoot == "" || !environmentPool.IsValid() || repository == nil || idempotency == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service creation dependencies are incomplete")
	}
	if err := controller.ValidateEnvironmentComponentCatalog(componentCatalog); err != nil {
		return nil, err
	}
	return &CreationService{
		volumeRoot: volumeRoot, environmentPool: environmentPool,
		repository: repository, idempotency: idempotency, protector: protector,
		plans: plans, hookInputs: hookInputs,
		componentCatalog: controller.CloneEnvironmentComponentCatalog(componentCatalog),
		now:              time.Now,
	}, nil
}

func (service *CreationService) CreateBackingService(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if idempotencyKey == "" {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Idempotency-Key is required")
	}
	adapter, ok := adapters.Get(input.Adapter)
	if !ok {
		return etcd.IdempotencyResponse{}, errs.Newf(
			errs.KindValidationFailed, "unsupported backing-service adapter %q", input.Adapter,
		)
	}
	hooks := controller.BackingHookConfigurationFromAPI(input.Hooks)
	if hooks != nil && !adapter.Custom() {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"managed backing-service adapters do not accept hooks",
		)
	}
	if hooks != nil {
		if err := backinghook.ValidateConfiguration(*hooks); err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), core.BackingAuthentication(input.Authentication),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var spec adapters.CreationSpec
	if adapter.Custom() {
		spec, err = adapters.CustomBackingCreationSpec(input.Slug, input.Image)
	} else {
		if input.Image != "" {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindValidationFailed,
				"managed backing-service adapters do not accept an image",
			)
		}
		spec, err = adapters.BackingCreationSpec(input.Adapter, authentication)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	input.Authentication = string(authentication)
	requestBytes, err := json.Marshal(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	requestDigest := sha256.Sum256(requestBytes)
	requestSHA256 := hex.EncodeToString(requestDigest[:])
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: backingServiceCreateRoute, Key: idempotencyKey,
	}
	createdAt := service.now().UTC()
	stage, err := service.repository.ClaimBackingServiceCreationStage(ctx, etcd.BackingServiceCreationStage{
		Locator: locator, RequestSHA256: requestSHA256,
		ProjectID: ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
		TaskID: ids.New(ids.KindTask), CreatedAt: createdAt,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.createBackingServiceFromStage(ctx, input, requestBytes, stage, adapter, spec)
}

func (service *CreationService) createBackingServiceFromStage(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	requestBytes []byte,
	stage etcd.Versioned[etcd.BackingServiceCreationStage],
	adapter adapters.Adapter,
	spec adapters.CreationSpec,
) (etcd.IdempotencyResponse, error) {
	bundle := core.BlueprintBundle{
		RootPath: "backing-service.yaml", ComposeSources: []string{"backing-service.yaml"},
		Files: []core.BlueprintFile{{Path: "backing-service.yaml", Content: append([]byte(nil), requestBytes...)}},
	}
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPost, Route: backingServiceCreateRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
	}, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, stage.Record.Locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Backing-service replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	authentication := core.BackingAuthentication(input.Authentication)
	blueprintLocator := stage.Record.Locator
	blueprintLocator.ScopeKind = etcd.IdempotencyScopeEnvironment
	blueprintLocator.ScopeID = stage.Record.EnvironmentID
	claim, err := desiredrevision.Claim(ctx, service.repository, desiredrevision.ClaimInput{
		EnvironmentID: stage.Record.EnvironmentID, CandidateTaskID: stage.Record.TaskID,
		Locator: blueprintLocator, Intent: evidence.Durable,
		MatchExistingIntent: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
			return service.idempotency.MatchesStaged(ctx, evidence, existing)
		},
		BaselineHeadRevision: 0, SourceKind: etcd.EnvironmentBlueprintSourceApply,
		RenderGeneration: 1, CreatedAt: stage.Record.CreatedAt,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if claim.TaskID != stage.Record.TaskID || !claim.CreatedAt.Equal(stage.Record.CreatedAt) {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Backing-service desired claim changed creation authority",
		)
	}

	project := etcd.ProjectRecord{
		ID: stage.Record.ProjectID, Slug: input.Slug, Name: input.Name,
		Description: input.Description, Kind: etcd.ProjectKindBacking,
	}
	poolRegistry, err := service.repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	reservedPools, networkPool, err := poolRegistry.Record.Reserve(
		service.environmentPool, stage.Record.EnvironmentID, input.NetworkPool,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	poolRegistry.Record = reservedPools
	environment, err := etcd.NewProvisioningEnvironment(
		service.volumeRoot, project, stage.Record.EnvironmentID, "main", networkPool,
		stage.Record.TaskID, stage.Record.CreatedAt,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environment, err = etcd.CompleteEnvironmentProvisioning(environment, stage.Record.TaskID, true)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}

	zone, err := zonerecord.NewRecord(environment.ID, core.Zone{
		ID: allocator.New(ids.KindNetwork), Name: input.Zone.Name, Subnet: input.Zone.Subnet,
		Internal: input.Zone.Internal, OwnerKind: core.ZoneOwnerBackingProject, OwnerID: project.ID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	entries, generations, secrets, secretValues, resolved, err := service.backingCreationEntries(
		ctx, project.ID, environment.ID, spec, allocator, stage.Record.CreatedAt,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
	mounts := []core.Mount(nil)
	if volume != nil {
		mounts = []core.Mount{{Volume: volumeID, Mount: spec.MountPath}}
	}
	desiredService := core.Service{
		ID: serviceID, Name: spec.ServiceName, Image: spec.Image,
		Zones: []string{zone.Desired.ID}, Healthcheck: core.Healthcheck{TCP: spec.HealthTCP},
		Command: append([]string(nil), spec.Command...),
		Mounts:  mounts,
		Expose:  append([]string(nil), spec.Expose...), Restart: "unless-stopped",
		Adapter: input.Adapter, Authentication: authentication,
		FactsPrefix: adapter.FactsPrefix(), Label: input.Name,
		Hooks: controller.BackingHookConfigurationFromAPI(input.Hooks),
	}
	serviceRecord, err := etcd.NewServiceRecord(environment.ID, desiredService, zone.Desired.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
	componentProjection, err := controller.ProjectEnvironmentComponents(
		baseProject,
		environmentProjection,
		service.componentCatalog,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	identities := controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: spec.ServiceName}},
		Networks: []controller.ComposeResourceIdentity{{ID: zone.Desired.ID, Name: zone.Desired.Name}},
	}
	if volume != nil {
		identities.Volumes = []controller.ComposeResourceIdentity{{ID: volumeID, Name: volume.Key}}
	}
	identities.Services = append(identities.Services, componentProjection.Services...)
	planID := allocator.Named(ids.KindPlan, "execution-plan")
	artifactID := allocator.Named(ids.KindConfig, "compose-artifact")
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerBacking,
		ProjectID:        project.ID, EnvironmentID: environment.ID, PlanID: planID,
		RenderGeneration: 1, AuthorizedVolumeDir: environment.VolumeDir, Identities: identities,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	environmentStepID := allocator.Named(ids.KindStep, "environment-directory")
	applyStepID := allocator.Named(ids.KindStep, "compose-apply")
	steps := []*agentpb.ExecutionStep{
		{
			StepId:         environmentStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId:     environment.ID,
					ExpectedVolumeDir: environment.VolumeDir,
				},
			},
		},
	}
	stepRecords := []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: environmentStepID}}
	taskParams := map[string]string{
		etcd.EnvironmentDesiredRevisionParam:         stage.Record.TaskID,
		etcd.TaskMaterializationEnvironmentParam:     environment.ID,
		etcd.TaskBackingServiceCreationParam:         serviceID,
		etcd.TaskBackingServiceVolumeDirectoryParam:  environment.VolumeDir,
		controller.EnvironmentBlueprintArtifactParam: artifactID,
		taskcontract.EnvironmentBlueprintProcedureParam: string(
			taskcontract.BlueprintComposeProcedureFullReconcile,
		),
	}
	if volume != nil {
		intentDigest, decodeErr := hex.DecodeString(claim.Intent.CiphertextDigest)
		if decodeErr != nil || len(intentDigest) != sha256.Size {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Backing-service protected intent digest is invalid",
			)
		}
		volumeStepID := allocator.Named(ids.KindStep, "managed-volume-directories")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId:         volumeStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId:   artifactID,
					VolumeIds:    []string{volumeID},
					IntentSha256: append([]byte(nil), intentDigest...),
				},
			},
		})
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: volumeStepID})
		taskParams[controller.EnvironmentBlueprintManagedVolumesParam] = volumeID
		taskParams[controller.VolumeTaskIntentSHA256Param] = hex.EncodeToString(intentDigest)
	}
	materializations := []etcd.TaskMaterializationRecord(nil)
	if spec.HasEnvironment() {
		materialization, materializeStep, materializeErr := backingEnvironmentMaterialization(
			environment.ID, artifactID, entries, resolved, allocator,
		)
		if materializeErr != nil {
			return etcd.IdempotencyResponse{}, materializeErr
		}
		steps = append(steps, materializeStep)
		stepRecords = append(
			stepRecords,
			etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: materializeStep.StepId},
		)
		materializations = []etcd.TaskMaterializationRecord{materialization}
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId:         applyStepID,
		TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{ArtifactId: artifactID, FullReconcile: true},
		},
	})
	stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: applyStepID})
	if spec.HasHealthcheck() {
		healthStepID := allocator.Named(ids.KindStep, "wait-healthy")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId:         healthStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{
				WaitHealthy: &agentpb.WaitHealthy{ArtifactId: artifactID, ServiceIds: []string{serviceID}},
			},
		})
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: healthStepID})
		taskParams[etcd.TaskBackingServiceHealthParam] = serviceID
	}
	owner, err := etcd.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: stage.Record.TaskID, OperationID: allocator.Named(ids.KindOperation, "operation"),
		IdempotencyKey: stage.Record.Locator.Key, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: 1, Type: etcd.TaskUpdate, Target: environment.ID,
		Params: taskParams, Steps: stepRecords,
		Materializations: materializations,
		TimeoutSeconds:   desiredrevision.TaskTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: stage.Record.CreatedAt, UpdatedAt: stage.Record.CreatedAt,
	}
	var hookInputs *etcd.BackingHookEncryptedInputs
	if desiredService.Hooks != nil && desiredService.Hooks.AfterStart != nil {
		if service.plans == nil || service.hookInputs == nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Backing-service creation hook dependencies are incomplete",
			)
		}
		hookInputs, err = service.hookInputs.SealBackingHookTaskInputs(
			ctx, task.OperationID, project.ID, *desiredService.Hooks,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if hookInputs != nil {
			defer clear(hookInputs.Ciphertext)
			task, err = etcd.BindBackingHookTaskInputs(task, project.ID, *hookInputs)
			if err != nil {
				return etcd.IdempotencyResponse{}, err
			}
		}
		task.Params[etcd.TaskBackingServiceAfterStartParam] = serviceID
		task.Steps = append(task.Steps, etcd.TaskStepRecord{
			Kind: etcd.TaskStepOperation, ID: allocator.Named(ids.KindStep, "backing-after-start"),
		})
		minimumTimeout := int64(desiredService.Hooks.AfterStart.TimeoutSeconds) + 30
		if task.TimeoutSeconds < minimumTimeout {
			task.TimeoutSeconds = minimumTimeout
		}
		task, err = service.plans.PrepareBackingServiceCreationTask(
			ctx, task, artifact, steps, desiredService.Hooks, hookInputs,
		)
	} else {
		plan, buildErr := controller.BuildPlan(controller.PlanBuildInput{
			VolumeRoot: service.volumeRoot, PlanID: planID, RenderGeneration: 1,
			Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, TargetID: environment.ID,
			Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
		})
		if buildErr != nil {
			return etcd.IdempotencyResponse{}, buildErr
		}
		task.PlanHash = hex.EncodeToString(plan.PlanHash)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volumeSlugs := map[string]string(nil)
	volumeMounts := []etcd.EnvironmentServiceVolumeMount(nil)
	if volume != nil {
		volumeSlugs = map[string]string{volume.Key: volume.Slug}
		volumeMounts = []etcd.EnvironmentServiceVolumeMount{
			{ServiceID: serviceID, VolumeID: volumeID, Target: spec.MountPath},
		}
	}
	normalizedCompose, err := controller.MarshalNormalizedEnvironmentProject(baseProject)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection := buildBackingServiceCreationProjection(
		environment.ID, task.ID, identities, volumeSlugs, volumeMounts,
		artifactValue, normalizedCompose, zone, serviceRecord, entries,
	)
	projectionEvidence, err := desiredrevision.PreflightProjection(projection)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	revision := desiredrevision.BlueprintRevision(environment.ID, task.ID, stage.Record.CreatedAt, bundle)
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &revision, Projection: projection,
		DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
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
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body:        responseBody,
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: stage.Record.Locator, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	result, publishErr := service.repository.PublishBackingServiceWithTask(ctx, etcd.BackingServiceCreation{
		VolumeRoot: service.volumeRoot, Stage: stage, PoolRegistry: poolRegistry,
		Project: project, Environment: environment, Components: nil,
		Zone: zone, Service: serviceRecord, Secrets: secrets, SecretValues: secretValues,
		Entries: entries, EntryValues: generations, Claim: claim,
		Revision:   etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.ID, RevisionID: task.ID},
		Projection: projection, Task: task, HookInputs: hookInputs, Marker: marker,
	})
	if publishErr != nil {
		if !isUnknownBackingServiceCreationOutcome(publishErr) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolved, resolveErr := service.idempotency.ResolveUnknown(ctx, stage.Record.Locator, evidence, publishErr)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		return requestidempotency.CloneResponse(resolved.Response), nil
	}
	outcome, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch outcome.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(outcome.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing-service creation resolution is invalid")
	}
}

func isUnknownBackingServiceCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
