package connectors

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorDeletionRoute          = "/connectors/{id}"
	connectorDeletionTimeoutSeconds = int64(30)
)

type connectorDeletionRepository interface {
	GetConnector(context.Context, string) (etcdstore.Versioned[connectorrecord.Record], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	CaptureConnectorRemoval(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[connectorrecord.Record],
		string,
		string,
		string,
		time.Time,
	) (connectorremoval.Input, error)
	PrepareConnectorRemovalAuthority(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[connectorrecord.Record],
		connectorremoval.Input,
		workexecution.Task,
	) (workqueue.Change, workexecution.Item, error)
}

type connectorDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type connectorDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string, []string) (connectorDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		connectorDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
}

type durableConnectorDeletionIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDeletionIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableConnectorDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "connector deletion idempotency is not configured")
	}
	return &durableConnectorDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableConnectorDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableConnectorDeletionIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	connectorID string,
	after []string,
) (connectorDeletionEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment {
		return connectorDeletionEvidence{}, errs.New(errs.KindInternal, "connector deletion replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: connectorDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: connectorID}},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)})),
	})
	if err != nil {
		return connectorDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return connectorDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return connectorDeletionEvidence{}, err
	}
	return connectorDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableConnectorDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence connectorDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

type connectorDeletionService struct {
	repository  connectorDeletionRepository
	idempotency connectorDeletionIdempotency
	now         func() time.Time
	accepted    *acceptedaction.Submitter
}

func NewDeletionService(
	repository connectorDeletionRepository,
	idempotency connectorDeletionIdempotency,
	accepted *acceptedaction.Submitter,
) (*connectorDeletionService, error) {
	if repository == nil || idempotency == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "connector deletion service is not configured")
	}
	return &connectorDeletionService{
		repository:  repository,
		idempotency: idempotency,
		now:         time.Now,
		accepted:    accepted,
	}, nil
}

type acceptedConnectorRemoval struct {
	After []string `json:"after,omitempty"`
}

func (service *connectorDeletionService) DeleteConnector(
	ctx context.Context,
	connectorID string,
	request apiTypes.TaskRunAfterRequest,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ids.Validate(ids.KindConnector, connectorID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "connector id is invalid")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetConnector,
		ID:   connectorID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		connectorDeletionRoute,
		key,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var current etcdstore.Versioned[connectorrecord.Record]
	var project etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	if !indexed {
		current, err = service.repository.GetConnector(ctx, connectorID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		environment, readErr := service.repository.GetEnvironment(ctx, current.Record.Connector.EnvironmentID)
		if readErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, readErr
		}
		project, err = service.repository.GetProject(ctx, environment.Record.ProjectID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		locator = idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
			ScopeID:   environment.Record.ID,
			Method:    http.MethodDelete,
			Route:     connectorDeletionRoute,
			Key:       key,
		}
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, connectorID, request.After)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.candidate.Destroy()
	defer clear(evidence.durable.Ciphertext)
	resolution, exists, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if exists {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if indexed {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"connector replay target is inconsistent",
		)
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRemove,
		TargetID:   connectorID,
		TargetName: current.Record.Connector.Name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: locator.ScopeID,
		},
		After:     request.After,
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		acceptedaction.Submission{
			Task:         task,
			Kind:         "connector-remove",
			ReplayTarget: &target,
			Locator:      locator,
			Intent:       evidence.durable,
			Evidence:     evidence.candidate,
		},
		acceptedConnectorRemoval{After: request.After},
	)
}

func (service *connectorDeletionService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"connector-remove",
			workexecution.ActionRemove,
			ids.KindConnector,
			service.prepareAcceptedRemoval,
		),
	}
}

func (service *connectorDeletionService) prepareAcceptedRemoval(
	ctx context.Context,
	snapshot workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	_ acceptedConnectorRemoval,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	current, err := service.repository.GetConnector(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.Connector.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Connector ownership changed while waiting",
		)
	}
	input, err := service.repository.CaptureConnectorRemoval(
		ctx,
		environment,
		project,
		current,
		task.ID,
		ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "connector-remove-work"),
		ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "connector-remove-input"),
		at,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, err = input.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.ExpectedItems = 1
	task.UpdatedAt = at
	authority, item, err := service.repository.PrepareConnectorRemovalAuthority(
		ctx,
		environment,
		project,
		current,
		input,
		task,
	)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}

type durableConnectorDeletionRepository struct {
	hierarchy  *etcd.HierarchyRepository
	connectors *etcd.ConnectorRepository
}

func NewDeletionRepository(
	hierarchy *etcd.HierarchyRepository,
	connectors *etcd.ConnectorRepository,
) (*durableConnectorDeletionRepository, error) {
	if hierarchy == nil || connectors == nil {
		return nil, errs.New(errs.KindInternal, "connector deletion repository dependencies are required")
	}
	return &durableConnectorDeletionRepository{hierarchy: hierarchy, connectors: connectors}, nil
}

func (repository *durableConnectorDeletionRepository) GetConnector(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[connectorrecord.Record], error) {
	return repository.connectors.GetConnector(ctx, id)
}

func (repository *durableConnectorDeletionRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableConnectorDeletionRepository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableConnectorDeletionRepository) CaptureConnectorRemoval(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	taskID, workID, planID string,
	at time.Time,
) (connectorremoval.Input, error) {
	return repository.connectors.CaptureConnectorRemoval(ctx, environment, project, current, taskID, workID, planID, at)
}

func (repository *durableConnectorDeletionRepository) PrepareConnectorRemovalAuthority(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	input connectorremoval.Input,
	task workexecution.Task,
) (workqueue.Change, workexecution.Item, error) {
	return repository.connectors.PrepareConnectorRemovalAuthority(ctx, environment, project, current, input, task)
}
