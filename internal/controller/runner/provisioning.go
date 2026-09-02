package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	GetRunner(context.Context, string) (etcd.Versioned[etcd.RunnerRecord], error)
	CreateRunnerWithTask(
		context.Context,
		runnerallocation.RunnerAllocationConfig,
		etcd.RunnerDesiredRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	RetryRunnerCreationWithTask(
		context.Context,
		string,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type runnerTaskReader interface {
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
}

type runnerProjectReader interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
}

// ProvisioningService turns Runner create and failed-create retry requests
// into durable native Controller Tasks. The registration token is owned only
// by its in-process broker and never crosses the persistence seam.
type ProvisioningService struct {
	runners     provisioningRepository
	tasks       runnerTaskReader
	projects    runnerProjectReader
	idempotency *etcd.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
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
	coordinator *idempotentintent.Coordinator,
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner creation context is required")
	}
	token, err := runnerRegistrationToken(request.RegistrationToken)
	request.RegistrationToken = ""
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer token.clear()
	desired, err := service.normalizeCreate(ctx, request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator := runnerProvisioningLocator(desired, http.MethodPost, runnerCreateRoute, idempotencyKey)
	evidence, err := service.protectCreateIntent(ctx, locator, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner creation replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task, err := newRunnerCreateTask(desired, idempotencyKey, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.newTaskMarker(locator, evidence, task, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	key := TokenKey{TaskID: task.ID, Attempt: 1}
	if err := service.broker.Stage(key, token); err != nil {
		return etcd.IdempotencyResponse{}, err
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner retry context is required")
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	token, err := runnerRegistrationToken(request.RegistrationToken)
	request.RegistrationToken = ""
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer token.clear()
	current, err := service.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator := runnerProvisioningLocator(current.Record.Desired, http.MethodPost, runnerRetryRoute, idempotencyKey)
	evidence, err := service.protectRetryIntent(ctx, locator, runnerID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner retry replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}
	source, err := service.tasks.GetTask(ctx, current.Record.CreateTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	retry := newRunnerRetryTask(source.Record, now)
	response, marker, err := service.newTaskMarker(locator, evidence, retry, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	key := TokenKey{TaskID: retry.ID, Attempt: 1}
	if err := service.broker.Stage(key, token); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, mutationErr := service.runners.RetryRunnerCreationWithTask(
		ctx, source.Record.ID, retry, marker,
	)
	return service.resolvePublication(ctx, locator, evidence, response, result, mutationErr, key)
}

func (service *ProvisioningService) normalizeCreate(
	ctx context.Context,
	request apiTypes.RunnerCreateRequest,
) (etcd.RunnerDesiredRecord, error) {
	desired := etcd.RunnerDesiredRecord{
		ID: ids.New(ids.KindRunner), Slug: request.Slug, GitHubURL: request.GitHubURL,
		Labels: slices.Clone(request.Labels), ImageRef: service.imageRef,
	}
	switch {
	case request.TenantID != "" && request.ProjectID == "":
		desired.OwnerKind = etcd.RunnerOwnerTenant
		desired.OwnerID = request.TenantID
		desired.TenantID = request.TenantID
	case request.ProjectID != "" && request.TenantID == "":
		project, err := service.projects.GetProject(ctx, request.ProjectID)
		if err != nil {
			return etcd.RunnerDesiredRecord{}, err
		}
		if project.Record.Kind != etcd.ProjectKindTenant || project.Record.TenantID == "" {
			return etcd.RunnerDesiredRecord{}, errs.New(
				errs.KindValidationFailed,
				"Runner project owner must be a Tenant Project",
			)
		}
		desired.OwnerKind = etcd.RunnerOwnerProject
		desired.OwnerID = project.Record.ID
		desired.TenantID = project.Record.TenantID
	default:
		return etcd.RunnerDesiredRecord{}, errs.New(
			errs.KindValidationFailed,
			"Runner creation requires exactly one of tenant_id or project_id",
		)
	}
	return etcd.NormalizeRunnerDesired(desired)
}

func runnerRegistrationToken(value string) (*RegistrationToken, error) {
	bytes := []byte(value)
	return NewRegistrationToken(bytes)
}

func runnerProvisioningLocator(
	desired etcd.RunnerDesiredRecord,
	method string,
	route string,
	key string,
) etcd.IdempotencyLocator {
	scopeKind := etcd.IdempotencyScopeTenant
	if desired.OwnerKind == etcd.RunnerOwnerProject {
		scopeKind = etcd.IdempotencyScopeProject
	}
	return etcd.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: desired.OwnerID, Method: method, Route: route, Key: key,
	}
}

func (service *ProvisioningService) protectCreateIntent(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	desired etcd.RunnerDesiredRecord,
) (idempotentintent.ProtectedEvidence, error) {
	labels := make([]idempotentintent.Value, len(desired.Labels))
	for index, label := range desired.Labels {
		labels[index] = idempotentintent.String(label)
	}
	ownerField := idempotentintent.Field{Name: "tenant_id", Value: idempotentintent.String(desired.OwnerID)}
	if desired.OwnerKind == etcd.RunnerOwnerProject {
		ownerField = idempotentintent.Field{Name: "project_id", Value: idempotentintent.String(desired.OwnerID)}
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerCreateRoute,
		Scope: runnerIntentScope(locator), Path: nil, Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "slug", Value: idempotentintent.String(desired.Slug)},
			ownerField,
			idempotentintent.Field{Name: "github_url", Value: idempotentintent.String(desired.GitHubURL)},
			idempotentintent.Field{Name: "labels", Value: idempotentintent.List(labels...)},
			idempotentintent.Field{Name: "registration_token_present", Value: idempotentintent.Bool(true)},
		)),
	})
	if err != nil {
		return idempotentintent.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func (service *ProvisioningService) protectRetryIntent(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	runnerID string,
) (idempotentintent.ProtectedEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerRetryRoute,
		Scope: runnerIntentScope(locator),
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: runnerID}},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "registration_token_present", Value: idempotentintent.Bool(true)},
		)),
	})
	if err != nil {
		return idempotentintent.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func runnerIntentScope(locator etcd.IdempotencyLocator) idempotentintent.Scope {
	kind := idempotentintent.ScopeTenant
	if locator.ScopeKind == etcd.IdempotencyScopeProject {
		kind = idempotentintent.ScopeProject
	}
	return idempotentintent.Scope{Kind: kind, ID: locator.ScopeID}
}

func (service *ProvisioningService) newTaskMarker(
	locator etcd.IdempotencyLocator,
	evidence idempotentintent.ProtectedEvidence,
	task etcd.TaskRecord,
	now time.Time,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, err
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	return response, marker, nil
}

func (service *ProvisioningService) resolvePublication(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence idempotentintent.ProtectedEvidence,
	response etcd.IdempotencyResponse,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	key TokenKey,
) (etcd.IdempotencyResponse, error) {
	var (
		resolution idempotentintent.Resolution
		err        error
	)
	if mutationErr != nil {
		if !unknownMutationOutcome(mutationErr) {
			service.broker.Drop(key)
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, evidence, mutationErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneResponse(response), nil
	case idempotentintent.ResolutionReplay:
		if !taskResponseOwnsToken(resolution.Response, key.TaskID) {
			service.broker.Drop(key)
		}
		return cloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner publication resolution is invalid")
	}
}

func taskResponseOwnsToken(response etcd.IdempotencyResponse, taskID string) bool {
	if response.Status != http.StatusAccepted {
		return false
	}
	var accepted apiTypes.TaskAccepted
	return json.Unmarshal(response.Body, &accepted) == nil && accepted.TaskID == taskID
}

func newRunnerCreateTask(
	desired etcd.RunnerDesiredRecord,
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
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: hex.EncodeToString(planDigest[:]), RenderGeneration: 1,
		Type: etcd.TaskCreate, Target: desired.ID,
		Params: map[string]string{
			etcd.TaskResourceKindParam:               etcd.TaskResourceRunner,
			etcd.RunnerRegistrationTokenPresentParam: "true",
		},
		Steps: []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}}, TimeoutSeconds: runnerCreateTimeoutSeconds,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func newRunnerRetryTask(source etcd.TaskRecord, now time.Time) etcd.TaskRecord {
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: source.OperationID, RetryOf: source.ID,
		IdempotencyKey: source.IdempotencyKey, Owner: source.Owner, Actor: etcd.TaskActorOperator,
		Executor: source.Executor, PlanID: source.PlanID, PlanHash: source.PlanHash,
		RenderGeneration: source.RenderGeneration, Type: source.Type, Target: source.Target,
		Params: cloneRunnerTaskParams(source.Params), Steps: slices.Clone(source.Steps),
		Materializations: slices.Clone(source.Materializations), TimeoutSeconds: source.TimeoutSeconds,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func runnerTaskOwnerForController(desired etcd.RunnerDesiredRecord) (etcd.TaskOwner, error) {
	if desired.OwnerKind == etcd.RunnerOwnerTenant {
		return etcd.TenantTaskOwner(desired.TenantID)
	}
	if desired.OwnerKind == etcd.RunnerOwnerProject {
		return etcd.TenantProjectTaskOwner(desired.TenantID, desired.OwnerID)
	}
	return etcd.TaskOwner{}, errs.New(errs.KindValidationFailed, "Runner owner is invalid")
}

func cloneRunnerTaskParams(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
