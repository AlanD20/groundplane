package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"path/filepath"
	"sort"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
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
	ClaimBackingServiceCreationStage(context.Context, etcd.BackingServiceCreationStage) (etcd.Versioned[etcd.BackingServiceCreationStage], error)
	PublishBackingServiceWithTask(context.Context, etcd.BackingServiceCreation) (etcd.IdempotencyTransactionResult, error)
}

type backingServiceCreationService struct {
	volumeRoot       string
	environmentPool  netip.Prefix
	repository       backingServiceCreationRepository
	idempotency      *desiredrevision.Idempotency
	protector        *secretvalue.Protector
	componentCatalog []controller.EnvironmentComponentRegistration
	now              func() time.Time
}

func newBackingServiceCreationService(
	volumeRoot string,
	environmentPool netip.Prefix,
	repository backingServiceCreationRepository,
	idempotency *desiredrevision.Idempotency,
	protector *secretvalue.Protector,
	componentCatalog []controller.EnvironmentComponentRegistration,
) (*backingServiceCreationService, error) {
	if volumeRoot == "" || !environmentPool.IsValid() || repository == nil || idempotency == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service creation dependencies are incomplete")
	}
	if err := controller.ValidateEnvironmentComponentCatalog(componentCatalog); err != nil {
		return nil, err
	}
	return &backingServiceCreationService{
		volumeRoot: volumeRoot, environmentPool: environmentPool,
		repository: repository, idempotency: idempotency, protector: protector,
		componentCatalog: controller.CloneEnvironmentComponentCatalog(componentCatalog),
		now:              time.Now,
	}, nil
}

func (service *backingServiceCreationService) CreateBackingService(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if idempotencyKey == "" {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Idempotency-Key is required")
	}
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
	return service.createBackingServiceFromStage(ctx, input, requestBytes, stage)
}

func (service *backingServiceCreationService) createBackingServiceFromStage(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	requestBytes []byte,
	stage etcd.Versioned[etcd.BackingServiceCreationStage],
) (etcd.IdempotencyResponse, error) {
	bundle := core.BlueprintBundle{
		RootPath: "backing-service.yaml", ComposeSources: []string{"backing-service.yaml"},
		Files: []core.BlueprintFile{{Path: "backing-service.yaml", Content: append([]byte(nil), requestBytes...)}},
	}
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPost, Route: backingServiceCreateRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
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
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing-service replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	spec, err := adapters.BackingCreationSpec(input.Adapter)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	adapter, ok := adapters.Get(input.Adapter)
	if !ok {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing-service adapter disappeared after validation")
	}
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing-service desired claim changed creation authority")
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

	zone, err := etcd.NewZoneRecord(environment.ID, core.Zone{
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

	volumeID := allocator.New(ids.KindVolume)
	serviceID := allocator.New(ids.KindService)
	desiredService := core.Service{
		ID: serviceID, Name: spec.ServiceName, Image: adapter.DefaultImage(),
		Zones: []string{zone.Desired.ID}, Healthcheck: core.Healthcheck{TCP: spec.HealthTCP},
		Command: append([]string(nil), spec.Command...),
		Mounts:  []core.Mount{{Volume: volumeID, Mount: spec.MountPath}},
		Expose:  append([]string(nil), spec.Expose...), Restart: "unless-stopped",
		Adapter: input.Adapter, FactsPrefix: adapter.FactsPrefix(), Label: input.Name,
	}
	serviceRecord, err := etcd.NewServiceRecord(environment.ID, desiredService, zone.Desired.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volume := core.Volume{ID: volumeID, Slug: spec.VolumeSlug, Key: spec.VolumeKey}
	entryDesired := make([]core.EnvEntry, len(entries))
	for index := range entries {
		entryDesired[index] = entries[index].Entry
	}
	environmentProjection := core.Environment{
		ID: environment.ID, ProjectID: project.ID, Name: environment.Name,
		NetworkPool: environment.NetworkPool, VolumeDir: environment.VolumeDir,
		Zones:      map[string]core.Zone{zone.Desired.Name: zone.Desired},
		Services:   map[string]core.Service{desiredService.Name: desiredService},
		Components: nil, Volumes: map[string]core.Volume{volume.Key: volume},
		Entries: entryDesired, CreatedAt: environment.CreatedAt,
	}
	baseProject := backingComposeProject(spec, adapter.DefaultImage(), zone.Desired, volume, environment)
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
		Volumes:  []controller.ComposeResourceIdentity{{ID: volumeID, Name: volume.Key}},
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
	materialization, materializeStep, err := backingEnvironmentMaterialization(
		environment.ID, artifactID, entries, resolved, allocator,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intentDigest, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil || len(intentDigest) != sha256.Size {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing-service protected intent digest is invalid")
	}
	environmentStepID := allocator.Named(ids.KindStep, "environment-directory")
	volumeStepID := allocator.Named(ids.KindStep, "managed-volume-directories")
	applyStepID := allocator.Named(ids.KindStep, "compose-apply")
	healthStepID := allocator.Named(ids.KindStep, "wait-healthy")
	steps := []*agentpb.ExecutionStep{
		{StepId: environmentStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds), Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{EnvironmentId: environment.ID, ExpectedVolumeDir: environment.VolumeDir}}},
		{StepId: volumeStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds), Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{ArtifactId: artifactID, VolumeIds: []string{volumeID}, IntentSha256: append([]byte(nil), intentDigest...)}}},
		materializeStep,
		{StepId: applyStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds), Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{ArtifactId: artifactID, FullReconcile: true}}},
		{StepId: healthStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds), Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{ArtifactId: artifactID, ServiceIds: []string{serviceID}}}},
	}
	plan, err := controller.BuildPlan(controller.PlanBuildInput{
		VolumeRoot: service.volumeRoot, PlanID: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, TargetID: environment.ID,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner, err := etcd.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: stage.Record.TaskID, OperationID: allocator.Named(ids.KindOperation, "operation"),
		IdempotencyKey: stage.Record.Locator.Key, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: 1, Type: etcd.TaskUpdate, Target: environment.ID,
		Params: map[string]string{
			etcd.EnvironmentDesiredRevisionParam:               stage.Record.TaskID,
			etcd.TaskMaterializationEnvironmentParam:           environment.ID,
			etcd.TaskBackingServiceHealthParam:                 serviceID,
			etcd.TaskBackingServiceVolumeDirectoryParam:        environment.VolumeDir,
			controller.EnvironmentBlueprintArtifactParam:       artifactID,
			controller.EnvironmentBlueprintManagedVolumesParam: volumeID,
			controller.VolumeTaskIntentSHA256Param:             hex.EncodeToString(intentDigest),
		},
		Steps:            []etcd.TaskStepRecord{{ID: environmentStepID}, {ID: volumeStepID}, {ID: materializeStep.StepId}, {ID: applyStepID}, {ID: healthStepID}},
		Materializations: []etcd.TaskMaterializationRecord{materialization},
		TimeoutSeconds:   environmentBlueprintTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: stage.Record.CreatedAt, UpdatedAt: stage.Record.CreatedAt,
	}
	volumeMounts := []etcd.EnvironmentServiceVolumeMount{{ServiceID: serviceID, VolumeID: volumeID, Target: spec.MountPath}}
	normalizedCompose, err := controller.MarshalNormalizedEnvironmentProject(baseProject)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection := buildBackingServiceCreationProjection(
		environment.ID, task.ID, identities, map[string]string{volume.Key: volume.Slug}, volumeMounts,
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
			ProjectID: project.ID, EnvironmentID: environment.ID,
			ServiceID: serviceID, BackingNetworkID: zone.Desired.ID,
		},
		TaskID: task.ID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{Status: http.StatusCreated, ContentKind: "application/json", Body: responseBody}
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
		Projection: projection, Task: task, Marker: marker,
	})
	if publishErr != nil {
		if !isUnknownBackingServiceCreationOutcome(publishErr) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolved, resolveErr := service.idempotency.ResolveUnknown(ctx, stage.Record.Locator, evidence, publishErr)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		return cloneIdempotencyResponse(resolved.Response), nil
	}
	outcome, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch outcome.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(outcome.Response), nil
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

func (service *backingServiceCreationService) backingCreationEntries(
	ctx context.Context,
	projectID string,
	environmentID string,
	spec adapters.CreationSpec,
	allocator *desiredrevision.BlueprintIdentityAllocator,
	createdAt time.Time,
) ([]etcd.EntryRecord, []etcd.EntryValueGeneration, []etcd.SecretRecord, []etcd.SecretEncryptedValue, map[string]string, error) {
	bootstrapValues := make(map[string][]byte)
	bootstrapSecrets := make(map[string]string)
	var secrets []etcd.SecretRecord
	var secretValues []etcd.SecretEncryptedValue
	for _, declaration := range spec.Environment {
		if declaration.BootstrapKey == "" {
			continue
		}
		if _, exists := bootstrapValues[declaration.BootstrapKey]; exists {
			continue
		}
		value, err := randomBackingCredential()
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		bootstrapValues[declaration.BootstrapKey] = value
		secretID := allocator.Named(ids.KindSecret, "bootstrap-secret-"+declaration.BootstrapKey)
		secret, err := etcd.NewProjectSecretRecord(secretID, projectID, declaration.Name, core.SecretKindEnvVar, "", createdAt)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		encrypted, err := sealBackingSecret(ctx, service.protector, secretID, value)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		bootstrapSecrets[declaration.BootstrapKey] = secretID
		secrets = append(secrets, secret)
		secretValues = append(secretValues, encrypted)
	}
	defer func() {
		for key, value := range bootstrapValues {
			clear(value)
			delete(bootstrapValues, key)
		}
	}()
	entries := make([]etcd.EntryRecord, 0, len(spec.Environment))
	generations := make([]etcd.EntryValueGeneration, 0, len(spec.Environment))
	resolved := make(map[string]string, len(spec.Environment))
	for index, declaration := range spec.Environment {
		entryID := allocator.Named(ids.KindEnvEntry, "bootstrap-entry-"+declaration.Name)
		generationID := allocator.Named(ids.KindConfig, "bootstrap-entry-generation-"+declaration.Name)
		entry := core.EnvEntry{
			ID: entryID, Kind: core.EntryKindEnv, Key: declaration.Name,
			Exposure: []string{"all"}, Secret: declaration.Secret,
		}
		var value []byte
		if declaration.BootstrapKey == "" {
			entry.Source = core.EntrySource{Kind: core.SourceLiteral, Literal: declaration.Literal}
			value = []byte(declaration.Literal)
		} else {
			entry.Source = core.EntrySource{Kind: core.SourceSecretRef, SecretRef: bootstrapSecrets[declaration.BootstrapKey]}
			value = bootstrapValues[declaration.BootstrapKey]
		}
		record, err := etcd.NewEntryRecord(environmentID, entry, generationID)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		var generation etcd.EntryValueGeneration
		if declaration.Secret {
			encrypted, err := sealBackingEntry(ctx, service.protector, environmentID, entryID, generationID, value, createdAt)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			generation.Secret = &encrypted
		} else {
			digest := sha256.Sum256(value)
			generation.Plain = &etcd.PlainEntryValueGeneration{
				EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
				Content: append([]byte(nil), value...), PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: createdAt,
			}
		}
		entries = append(entries, record)
		generations = append(generations, generation)
		resolved[entryID] = string(value)
		_ = index
	}
	order := make([]int, len(entries))
	for index := range order {
		order[index] = index
	}
	sort.Slice(order, func(left, right int) bool {
		return entries[order[left]].Entry.ID < entries[order[right]].Entry.ID
	})
	sortedEntries := make([]etcd.EntryRecord, len(entries))
	sortedGenerations := make([]etcd.EntryValueGeneration, len(generations))
	for index, source := range order {
		sortedEntries[index] = entries[source]
		sortedGenerations[index] = generations[source]
	}
	return sortedEntries, sortedGenerations, secrets, secretValues, resolved, nil
}

func randomBackingCredential() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(encoded, raw)
	clear(raw)
	return encoded, nil
}

func sealBackingSecret(ctx context.Context, protector *secretvalue.Protector, secretID string, value []byte) (etcd.SecretEncryptedValue, error) {
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return etcd.SecretEncryptedValue{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return etcd.SecretEncryptedValue{
		SecretID: secretID, EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
		DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
		Ciphertext: envelope.Ciphertext(),
	}, nil
}

func sealBackingEntry(ctx context.Context, protector *secretvalue.Protector, environmentID, entryID, generationID string, value []byte, createdAt time.Time) (etcd.SecretEntryValueGeneration, error) {
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return etcd.SecretEntryValueGeneration{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return etcd.SecretEntryValueGeneration{
		EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
		EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
		DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
		Ciphertext: envelope.Ciphertext(), CreatedAt: createdAt,
	}, nil
}

func backingComposeProject(
	spec adapters.CreationSpec,
	image string,
	zone core.Zone,
	volume core.Volume,
	environment etcd.EnvironmentRecord,
) *composetypes.Project {
	environmentFile := controller.EnvFileName(environment.ID)
	return &composetypes.Project{
		Name: "groundplane-backing",
		Services: composetypes.Services{spec.ServiceName: {
			Name: spec.ServiceName, Image: image, Command: composetypes.ShellCommand(spec.Command),
			Expose: composetypes.StringOrNumberList(spec.Expose), Restart: "unless-stopped",
			Networks:    map[string]*composetypes.ServiceNetworkConfig{zone.Name: {}},
			Volumes:     []composetypes.ServiceVolumeConfig{{Type: "volume", Source: volume.Key, Target: spec.MountPath}},
			EnvFiles:    []composetypes.EnvFile{{Path: filepath.Join(environment.VolumeDir, filepath.FromSlash(environmentFile)), Required: true}},
			HealthCheck: &composetypes.HealthCheckConfig{Test: composetypes.HealthCheckTest(spec.HealthCommand)},
		}},
		Networks: composetypes.Networks{zone.Name: {Internal: zone.Internal, Ipam: composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: zone.Subnet}}}}},
		Volumes:  composetypes.Volumes{volume.Key: {}},
	}
}

func backingEnvironmentMaterialization(
	environmentID string,
	artifactID string,
	entries []etcd.EntryRecord,
	resolved map[string]string,
	allocator *desiredrevision.BlueprintIdentityAllocator,
) (etcd.TaskMaterializationRecord, *agentpb.ExecutionStep, error) {
	desired := make([]core.EnvEntry, len(entries))
	references := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(entries))
	for index, entry := range entries {
		desired[index] = entry.Entry
		storage := etcd.TaskEntryValueStoragePlain
		if entry.Entry.Secret {
			storage = etcd.TaskEntryValueStorageSecret
		}
		references[index] = etcd.TaskGeneratedEnvironmentEntryReference{
			Name:  entry.Entry.Key,
			Value: etcd.TaskEntryValueReference{EntryID: entry.Entry.ID, ValueGenerationID: entry.CurrentValueGenerationID, Storage: storage},
		}
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Name < references[right].Name })
	content, err := controller.RenderEnvFile(desired, resolved)
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	defer clear(content)
	digest := sha256.Sum256(content)
	destination, err := entrymaterialization.GeneratedEnvDestination(environmentID, "")
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	record := etcd.TaskMaterializationRecord{
		StepID:            allocator.Named(ids.KindStep, "bootstrap-environment"),
		MaterializationID: allocator.Named(ids.KindConfig, "bootstrap-environment-materialization"),
		EnvironmentID:     environmentID, Destination: destination,
		OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
		Mode:       uint32(entrymaterialization.ModePrivate), Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Source: etcd.TaskMaterializationSource{
			Kind:                 etcd.TaskMaterializationSourceGeneratedEnvironment,
			GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{FormatVersion: 1, Values: references},
		},
	}
	step, err := controller.BuildTaskMaterializationStep(record, artifactID, uint32(environmentBlueprintTimeoutSeconds))
	if err != nil {
		return etcd.TaskMaterializationRecord{}, nil, err
	}
	return record, step, nil
}
