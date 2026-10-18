package runner

import (
	"context"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
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
	PrepareRunnerCreation(
		context.Context,
		runnerallocation.RunnerAllocationConfig,
		runnerrecord.RunnerDesiredRecord,
		string,
		time.Time,
	) (etcd.RunnerCreationSource, error)
	PublishRunnerCreation(
		context.Context,
		etcd.RunnerCreationSource,
		runnerwork.Input,
		workexecution.Task,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	PrepareRunnerRetry(context.Context, etcdstore.Versioned[runnerrecord.RunnerRecord], runnerwork.Input) (workqueue.Change, error)
}

type runnerProjectReader interface {
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
}

// ProvisioningService turns Runner create and failed-create retry requests
// into durable native Controller Tasks. The registration token is owned only
// by its in-process broker and never crosses the persistence seam.
type ProvisioningService struct {
	runners     provisioningRepository
	queue       *workqueue.Repository
	inputs      *runnerwork.Repository
	projects    runnerProjectReader
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	broker      *TokenBroker
	allocation  runnerallocation.RunnerAllocationConfig
	imageRef    string
	policy      corerunner.IsolationPolicy
	now         func() time.Time
}

func NewProvisioningService(
	runners provisioningRepository,
	queue *workqueue.Repository,
	inputs *runnerwork.Repository,
	projects runnerProjectReader,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	broker *TokenBroker,
	allocation runnerallocation.RunnerAllocationConfig,
	imageRef string,
	policy corerunner.IsolationPolicy,
) (*ProvisioningService, error) {
	if runners == nil || queue == nil || inputs == nil || projects == nil || idempotency == nil || coordinator == nil ||
		broker == nil || imageRef == "" {
		return nil, errs.New(errs.KindInternal, "Runner provisioning service is not configured")
	}
	if _, err := allocation.Validate(); err != nil {
		return nil, err
	}
	return &ProvisioningService{
		runners: runners, queue: queue, inputs: inputs, projects: projects,
		idempotency: idempotency, coordinator: coordinator, broker: broker,
		allocation: allocation, imageRef: imageRef, policy: policy, now: time.Now,
	}, nil
}

func (service *ProvisioningService) CreateRunner(
	ctx context.Context,
	request apiTypes.RunnerCreateRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Runner creation context is required",
		)
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
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Runner creation replay is invalid",
			)
		}
		return cloneResponse(resolution.Response), nil
	}

	if err := service.policy.RequireControllerEndpoint(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	source, err := service.runners.PrepareRunnerCreation(ctx, service.allocation, desired, taskID, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer source.Clear()
	record := source.Runner()
	plan, err := corerunner.NewPlan(corerunner.Target{
		RunnerID: desired.ID, TenantID: desired.TenantID,
		OwnerKind: corerunner.OwnerKind(desired.OwnerKind), OwnerID: desired.OwnerID,
		GitHubURL: desired.GitHubURL, Labels: desired.Labels, ImageRef: desired.ImageRef,
		RuntimeEpoch: record.RuntimeEpoch + 1, AllocationConfig: service.allocation, Allocation: record.Allocation,
	}, service.policy)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	input := runnerwork.Input{TaskID: taskID, WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), Operation: runnerallocation.RuntimeOperationCreate,
		Desired: desired, Plan: plan, CreatedAt: now}
	ref, err := input.Reference()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor,
		Scope:  workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()},
		Action: workexecution.ActionCreate, TargetID: desired.ID, TargetName: desired.Slug,
		Intent: ref, ExpectedItems: 1, CreatedAt: now, UpdatedAt: now,
		Deadline: now.Add(time.Duration(runnerCreateTimeoutSeconds) * time.Second)}
	response, marker, err := service.newTaskMarker(locator, evidence, task.ID, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	key := TokenKey{TaskID: task.ID, WorkItemID: input.WorkItemID, Attempt: 1}
	if err := service.broker.Stage(key, token, task.Deadline); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, mutationErr := service.runners.PublishRunnerCreation(
		ctx, source, input, task, marker,
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
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Runner retry replay is invalid",
			)
		}
		return cloneResponse(resolution.Response), nil
	}
	if err := service.policy.RequireControllerEndpoint(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	source, err := service.inputs.LoadTask(ctx, current.Record.CreateTaskID, current.ReadRevision)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	view, err := service.queue.ReadWork(ctx, source.Record.TaskID, source.Record.WorkItemID, current.ReadRevision)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var previous workqueue.AttemptSnapshot
	if view.Item.Item.AttemptNumber != 0 {
		_, previous, err = service.queue.ReadAttempt(ctx, source.Record.TaskID, source.Record.WorkItemID,
			view.Item.Item.AttemptNumber, current.ReadRevision)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	now := service.now().UTC()
	change, err := runnerwork.PrepareRetry(source, view.Task, view.Item, previous, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer etcdstore.ClearMutationValues(change.Mutations)
	resource, err := service.runners.PrepareRunnerRetry(ctx, current, source.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer etcdstore.ClearMutationValues(resource.Mutations)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	response, marker, err := service.newTaskMarker(locator, evidence, source.Record.TaskID, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	// Retry is a completed admission receipt, not a second pending Task.
	marker.Kind, marker.State, marker.TaskID = idempotencyrecord.IdempotencyMarkerDirect, idempotencyrecord.IdempotencyMarkerCompleted, ""
	marker.TerminalAt, marker.RetainUntil = now, now.Add(idempotencyrecord.MarkerRetention)
	key := TokenKey{TaskID: source.Record.TaskID, WorkItemID: source.Record.WorkItemID, Attempt: view.Item.Item.AttemptNumber + 1}
	if err := service.broker.Stage(key, token, now.Add(time.Duration(runnerCreateTimeoutSeconds)*time.Second)); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, mutationErr := service.idempotency.ApplyExecutionRetry(ctx, change, marker)
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
	case request.TenantID != "" && request.ProjectID == "" && request.EnvironmentID == "":
		desired.OwnerKind = runnerrecord.RunnerOwnerTenant
		desired.OwnerID = request.TenantID
		desired.TenantID = request.TenantID
	case request.TenantID == "" && ((request.ProjectID != "") != (request.EnvironmentID != "")):
		projectID := request.ProjectID
		if request.EnvironmentID != "" {
			environment, err := service.projects.GetEnvironment(ctx, request.EnvironmentID)
			if err != nil {
				return runnerrecord.RunnerDesiredRecord{}, err
			}
			projectID = environment.Record.ProjectID
		}
		project, err := service.projects.GetProject(ctx, projectID)
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
		if request.EnvironmentID != "" {
			desired.OwnerKind = runnerrecord.RunnerOwnerEnvironment
			desired.OwnerID = request.EnvironmentID
			desired.ParentProjectID = project.Record.ID
		}
	default:
		return runnerrecord.RunnerDesiredRecord{}, errs.New(
			errs.KindValidationFailed,
			"Runner creation requires exactly one of tenant_id, project_id or environment_id",
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
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: desired.IdempotencyScope(), ScopeID: desired.OwnerID, Method: method, Route: route, Key: key,
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
	} else if desired.OwnerKind == runnerrecord.RunnerOwnerEnvironment {
		ownerField = requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(desired.OwnerID)}
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
	} else if locator.ScopeKind == idempotencyrecord.IdempotencyScopeEnvironment {
		kind = requestidempotency.ScopeEnvironment
	}
	return requestidempotency.Scope{Kind: kind, ID: locator.ScopeID}
}

func (service *ProvisioningService) newTaskMarker(
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
	taskID string,
	now time.Time,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(
			errs.KindInternal,
			err,
		)
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
		TaskID: taskID, CreatedAt: now, UpdatedAt: now,
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Runner publication resolution is invalid",
		)
	}
}

func taskResponseOwnsToken(response idempotencyrecord.IdempotencyResponse, taskID string) bool {
	if response.Status != http.StatusAccepted {
		return false
	}
	var accepted apiTypes.TaskAccepted
	return json.Unmarshal(response.Body, &accepted) == nil && accepted.TaskID == taskID
}
