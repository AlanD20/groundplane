package connectors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorDeletionRoute           = "/connectors/{id}"
	connectorDeletionTimeoutSeconds  = int64(30)
	maximumConnectorDeletionAttempts = 3
)

type connectorDeletionRepository interface {
	GetConnector(context.Context, string) (etcd.Versioned[connectorrecord.Record], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[hierarchyrecord.ProjectRecord], error)
	BeginConnectorDeletionWithTask(
		context.Context,
		etcd.Versioned[hierarchyrecord.EnvironmentRecord],
		etcd.Versioned[hierarchyrecord.ProjectRecord],
		etcd.Versioned[connectorrecord.Record],
		etcd.DeletionTombstoneRecord,
		etcd.ConnectorRemovalIntent,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type connectorDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type connectorDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, etcd.IdempotencyLocator, string) (connectorDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		connectorDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		connectorDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		connectorDeletionEvidence,
		error,
	) (requestidempotency.Resolution, error)
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
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableConnectorDeletionIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	connectorID string,
) (connectorDeletionEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment {
		return connectorDeletionEvidence{}, errs.New(errs.KindInternal, "connector deletion replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: connectorDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: connectorID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
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
	locator etcd.IdempotencyLocator,
	evidence connectorDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableConnectorDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence connectorDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableConnectorDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence connectorDeletionEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type connectorDeletionService struct {
	repository  connectorDeletionRepository
	idempotency connectorDeletionIdempotency
	now         func() time.Time
}

func NewDeletionService(
	repository connectorDeletionRepository,
	idempotency connectorDeletionIdempotency,
) (*connectorDeletionService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "connector deletion service is not configured")
	}
	return &connectorDeletionService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *connectorDeletionService) DeleteConnector(
	ctx context.Context,
	connectorID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "connector deletion context is required")
	}
	if ids.Validate(ids.KindConnector, connectorID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "connector id is invalid")
	}
	for attempt := 0; attempt < maximumConnectorDeletionAttempts; attempt++ {
		response, err := service.deleteConnectorOnce(ctx, connectorID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumConnectorDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "connector deletion retry bound was not enforced")
}

func (service *connectorDeletionService) deleteConnectorOnce(
	ctx context.Context,
	connectorID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetConnector, ID: connectorID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, connectorDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayConnectorDeletion(ctx, locator, connectorID)
	}

	current, err := service.repository.GetConnector(ctx, connectorID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.Connector.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: connectorDeletionRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, connectorID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"connector deletion replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	connector := current.Record.Connector
	task, err := newConnectorDeletionTask(taskOwner, connector, idempotencyKey, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetConnector, TargetID: connectorID,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: etcd.DeletionPhaseFinalizing,
		CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewConnectorRemovalIntent(
		task.ID, environment.Record.ID, connectorID, current.Revision, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, deleteErr := service.repository.BeginConnectorDeletionWithTask(
		ctx, environment, project, current, tombstone, intent, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownConnectorDeletionOutcome(deleteErr) {
			return etcd.IdempotencyResponse{}, deleteErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, deleteErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "connector deletion resolution is invalid")
	}
}

func (service *connectorDeletionService) replayConnectorDeletion(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	connectorID string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, connectorID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"connector deletion replay target is inconsistent",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func newConnectorDeletionTask(
	owner etcd.TaskOwner,
	connector core.Connector,
	idempotencyKey string,
	createdAt time.Time,
) (etcd.TaskRecord, error) {
	planHash, err := connectorDeletionPlanHash(connector.ID, connector.EnvironmentID, connector.Name)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorController, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: connector.ID,
		Params: map[string]string{
			etcd.TaskResourceKindParam:         etcd.TaskResourceConnector,
			etcd.TaskConnectorEnvironmentParam: connector.EnvironmentID,
			etcd.TaskConnectorNameParam:        connector.Name,
		},
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: connectorDeletionTimeoutSeconds, PlanHash: planHash,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, nil
}

func connectorDeletionPlanHash(connectorID string, environmentID string, name string) (string, error) {
	value, err := json.Marshal(struct {
		Version       int    `json:"version"`
		Type          string `json:"type"`
		ConnectorID   string `json:"connector_id"`
		EnvironmentID string `json:"environment_id"`
		Name          string `json:"name"`
	}{
		Version: 1, Type: string(etcd.TaskRemove), ConnectorID: connectorID,
		EnvironmentID: environmentID, Name: name,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func isUnknownConnectorDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
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
) (etcd.Versioned[connectorrecord.Record], error) {
	return repository.connectors.GetConnector(ctx, id)
}

func (repository *durableConnectorDeletionRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableConnectorDeletionRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableConnectorDeletionRepository) BeginConnectorDeletionWithTask(
	ctx context.Context,
	environment etcd.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcd.Versioned[hierarchyrecord.ProjectRecord],
	current etcd.Versioned[connectorrecord.Record],
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.ConnectorRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.connectors.BeginConnectorDeletionWithTask(
		ctx, environment, project, current, tombstone, intent, task, marker,
	)
}
