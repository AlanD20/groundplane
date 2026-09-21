package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerCreateRoute          = "/runners"
	runnerRetryRoute           = "/runners/{id}/retry"
	runnerCreateTimeoutSeconds = int64(300)
)

type provisioningRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	CreateRunnerWithTask(
		context.Context,
		runnerallocation.RunnerAllocationConfig,
		runnerrecord.RunnerDesiredRecord,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	RetryRunnerCreationWithTask(
		context.Context,
		string,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type runnerTaskReader interface {
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
}

type runnerProjectReader interface {
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
}

// ProvisioningService turns Runner create and failed-create retry requests
// into durable native Controller Tasks. The registration token is owned only
// by its in-process broker and never crosses the persistence seam.
type ProvisioningService struct {
	runners     provisioningRepository
	tasks       runnerTaskReader
	projects    runnerProjectReader
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	broker      *TokenBroker
	allocation  runnerallocation.RunnerAllocationConfig
	imageRef    string
	now         func() time.Time
}

func NewProvisioningService(
	runners provisioningRepository,
	tasks runnerTaskReader,
	projects runnerProjectReader,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	broker *TokenBroker,
	allocation runnerallocation.RunnerAllocationConfig,
	imageRef string,
) (*ProvisioningService, error) {
	if runners == nil || tasks == nil || projects == nil || idempotency == nil || coordinator == nil ||
		broker == nil || imageRef == "" {
		return nil, errs.New(errs.KindInternal, "Runner provisioning service is not configured")
	}
	if _, err := allocation.Validate(); err != nil {
		return nil, err
	}
	return &ProvisioningService{
		runners: runners, tasks: tasks, projects: projects,
		idempotency: idempotency, coordinator: coordinator, broker: broker,
		allocation: allocation, imageRef: imageRef, now: time.Now,
	}, nil
}

func (service *ProvisioningService) CreateRunner(
	ctx context.Context,
	request apiTypes.RunnerCreateRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner creation context is required")
	}
	token, err := runnerRegistrationToken(request.RegistrationToken)
	request.RegistrationToken = ""
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer token.clear()
	desired, err := service.normalizeCreate(ctx, request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator := runnerProvisioningLocator(desired, http.MethodPost, runnerCreateRoute, idempotencyKey)
	evidence, err := service.protectCreateIntent(ctx, locator, desired)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner creation replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task, err := newRunnerCreateTask(desired, idempotencyKey, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, marker, err := service.newTaskMarker(locator, evidence, task, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	key := TokenKey{TaskID: task.ID, Attempt: 1}
	if err := service.broker.Stage(key, token); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, mutationErr := service.runners.CreateRunnerWithTask(
		ctx, service.allocation, desired, task, marker,
	)
	return service.resolvePublication(ctx, locator, evidence, response, result, mutationErr, key)
}

func (service *ProvisioningService) RetryRunner(
	ctx context.Context,
	runnerID string,
	request apiTypes.RunnerRetryRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner retry context is required")
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	token, err := runnerRegistrationToken(request.RegistrationToken)
	request.RegistrationToken = ""
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer token.clear()
	current, err := service.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator := runnerProvisioningLocator(current.Record.Desired, http.MethodPost, runnerRetryRoute, idempotencyKey)
	evidence, err := service.protectRetryIntent(ctx, locator, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner retry replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}
	source, err := service.tasks.GetTask(ctx, current.Record.CreateTaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	retry := newRunnerRetryTask(source.Record, now)
	response, marker, err := service.newTaskMarker(locator, evidence, retry, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	key := TokenKey{TaskID: retry.ID, Attempt: 1}
	if err := service.broker.Stage(key, token); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, mutationErr := service.runners.RetryRunnerCreationWithTask(
		ctx, source.Record.ID, retry, marker,
	)
	return service.resolvePublication(ctx, locator, evidence, response, result, mutationErr, key)
}

func (service *ProvisioningService) normalizeCreate(
	ctx context.Context,
	request apiTypes.RunnerCreateRequest,
) (runnerrecord.RunnerDesiredRecord, error) {
	desired := runnerrecord.RunnerDesiredRecord{
		ID: ids.New(ids.KindRunner), Slug: request.Slug, GitHubURL: request.GitHubURL,
		Labels: slices.Clone(request.Labels), ImageRef: service.imageRef,
	}
	switch {
	case request.TenantID != "" && request.ProjectID == "":
		desired.OwnerKind = runnerrecord.RunnerOwnerTenant
		desired.OwnerID = request.TenantID
		desired.TenantID = request.TenantID
	case request.ProjectID != "" && request.TenantID == "":
		project, err := service.projects.GetProject(ctx, request.ProjectID)
		if err != nil {
			return runnerrecord.RunnerDesiredRecord{}, err
		}
		if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Record.TenantID == "" {
			return runnerrecord.RunnerDesiredRecord{}, errs.New(
				errs.KindValidationFailed,
				"Runner project owner must be a Tenant Project",
			)
		}
		desired.OwnerKind = runnerrecord.RunnerOwnerProject
		desired.OwnerID = project.Record.ID
		desired.TenantID = project.Record.TenantID
	default:
		return runnerrecord.RunnerDesiredRecord{}, errs.New(
			errs.KindValidationFailed,
			"Runner creation requires exactly one of tenant_id or project_id",
		)
	}
	return runnerrecord.NormalizeRunnerDesired(desired)
}

func runnerRegistrationToken(value string) (*RegistrationToken, error) {
	bytes := []byte(value)
	return NewRegistrationToken(bytes)
}

func runnerProvisioningLocator(
	desired runnerrecord.RunnerDesiredRecord,
	method string,
	route string,
	key string,
) idempotencyrecord.IdempotencyLocator {
	scopeKind := idempotencyrecord.IdempotencyScopeTenant
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		scopeKind = idempotencyrecord.IdempotencyScopeProject
	}
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: desired.OwnerID, Method: method, Route: route, Key: key,
	}
}

func (service *ProvisioningService) protectCreateIntent(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	desired runnerrecord.RunnerDesiredRecord,
) (requestidempotency.ProtectedEvidence, error) {
	labels := make([]requestidempotency.Value, len(desired.Labels))
	for index, label := range desired.Labels {
		labels[index] = requestidempotency.String(label)
	}
	ownerField := requestidempotency.Field{Name: "tenant_id", Value: requestidempotency.String(desired.OwnerID)}
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		ownerField = requestidempotency.Field{Name: "project_id", Value: requestidempotency.String(desired.OwnerID)}
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerCreateRoute,
		Scope: runnerIntentScope(locator), Path: nil, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(desired.Slug)},
			ownerField,
			requestidempotency.Field{Name: "github_url", Value: requestidempotency.String(desired.GitHubURL)},
			requestidempotency.Field{Name: "labels", Value: requestidempotency.List(labels...)},
			requestidempotency.Field{Name: "registration_token_present", Value: requestidempotency.Bool(true)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func (service *ProvisioningService) protectRetryIntent(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string,
) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerRetryRoute,
		Scope: runnerIntentScope(locator),
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: runnerID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "registration_token_present", Value: requestidempotency.Bool(true)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func runnerIntentScope(locator idempotencyrecord.IdempotencyLocator) requestidempotency.Scope {
	kind := requestidempotency.ScopeTenant
	if locator.ScopeKind == idempotencyrecord.IdempotencyScopeProject {
		kind = requestidempotency.ScopeProject
	}
	return requestidempotency.Scope{Kind: kind, ID: locator.ScopeID}
}

func (service *ProvisioningService) newTaskMarker(
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
	task etcd.TaskRecord,
	now time.Time,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	return response, marker, nil
}

func (service *ProvisioningService) resolvePublication(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
	response idempotencyrecord.IdempotencyResponse,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	key TokenKey,
) (idempotencyrecord.IdempotencyResponse, error) {
	var (
		resolution requestidempotency.Resolution
		err        error
	)
	if mutationErr != nil {
		if !unknownMutationOutcome(mutationErr) {
			service.broker.Drop(key)
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, evidence, mutationErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		if !taskResponseOwnsToken(resolution.Response, key.TaskID) {
			service.broker.Drop(key)
		}
		return cloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner publication resolution is invalid")
	}
}

func taskResponseOwnsToken(response idempotencyrecord.IdempotencyResponse, taskID string) bool {
	if response.Status != http.StatusAccepted {
		return false
	}
	var accepted apiTypes.TaskAccepted
	return json.Unmarshal(response.Body, &accepted) == nil && accepted.TaskID == taskID
}

func newRunnerCreateTask(
	desired runnerrecord.RunnerDesiredRecord,
	idempotencyKey string,
	now time.Time,
) (etcd.TaskRecord, error) {
	owner, err := runnerTaskOwnerForController(desired)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	planInput := strings.Join([]string{
		"groundplane.runner-create.v1", desired.ID, desired.TenantID, string(desired.OwnerKind),
		desired.OwnerID, desired.Slug, desired.GitHubURL, strings.Join(desired.Labels, "\x1f"), desired.ImageRef,
	}, "\x00")
	planDigest := sha256.Sum256([]byte(planInput))
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: hex.EncodeToString(planDigest[:]), RenderGeneration: 1,
		Type: taskjournal.TaskCreate, Target: desired.ID,
		Params: map[string]string{
			etcd.TaskResourceKindParam:               etcd.TaskResourceRunner,
			etcd.RunnerRegistrationTokenPresentParam: "true",
		},
		Steps: []taskjournal.TaskStepRecord{
			{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: runnerCreateTimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func newRunnerRetryTask(source etcd.TaskRecord, now time.Time) etcd.TaskRecord {
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: source.OperationID, RetryOf: source.ID,
		IdempotencyKey: source.IdempotencyKey, Owner: source.Owner, Actor: taskjournal.TaskActorOperator,
		Executor: source.Executor, PlanID: source.PlanID, PlanHash: source.PlanHash,
		RenderGeneration: source.RenderGeneration, Type: source.Type, Target: source.Target,
		Params: cloneRunnerTaskParams(source.Params), Steps: slices.Clone(source.Steps),
		Materializations: slices.Clone(source.Materializations), TimeoutSeconds: source.TimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func runnerTaskOwnerForController(desired runnerrecord.RunnerDesiredRecord) (taskjournal.TaskOwner, error) {
	if desired.OwnerKind == runnerrecord.RunnerOwnerTenant {
		return taskjournal.TenantTaskOwner(desired.TenantID)
	}
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		return taskjournal.TenantProjectTaskOwner(desired.TenantID, desired.OwnerID)
	}
	return taskjournal.TaskOwner{}, errs.New(errs.KindValidationFailed, "Runner owner is invalid")
}

func cloneRunnerTaskParams(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
