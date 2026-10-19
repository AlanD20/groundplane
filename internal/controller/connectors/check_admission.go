package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectorCheckRoute = "/api/v1/connectors/checks"

type CheckService struct {
	hierarchy   *etcd.HierarchyRepository
	connectors  *etcd.ConnectorRepository
	secrets     *etcd.SecretRepository
	checks      *connectorchecks.Repository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	protector   *secretvalue.Protector
}

func NewCheckService(
	hierarchy *etcd.HierarchyRepository,
	connectors *etcd.ConnectorRepository,
	secrets *etcd.SecretRepository,
	checks *connectorchecks.Repository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	protector *secretvalue.Protector,
) (*CheckService, error) {
	if hierarchy == nil || connectors == nil || secrets == nil || checks == nil || idempotency == nil ||
		coordinator == nil ||
		protector == nil {
		return nil, errs.New(errs.KindInternal, "connector check admission dependencies are required")
	}
	return &CheckService{hierarchy: hierarchy, connectors: connectors, secrets: secrets, checks: checks,
		idempotency: idempotency, coordinator: coordinator, protector: protector}, nil
}

func (service *CheckService) CheckConnection(ctx context.Context, environmentID string,
	input apiTypes.ConnectorCheckRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	intent, err := connectorCheckIntent(environmentID, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: http.MethodPost, Route: connectorCheckRoute, Key: key}
	if err := idempotencyrecord.ValidateIdempotencyLocator(locator); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || existing {
		return resolution.Response, err
	}
	return service.admitConnectionCheck(ctx, environmentID, input, locator, evidence)
}

func connectorCheckIntent(
	environmentID string,
	input apiTypes.ConnectorCheckRequest,
) (requestidempotency.CanonicalIntentV1, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return requestidempotency.CanonicalIntentV1{}, errs.New(
			errs.KindValidationFailed,
			"connector check Environment id is invalid",
		)
	}
	draft, changes := requestidempotency.Null(), requestidempotency.Null()
	if input.Draft != nil {
		if input.ConnectorID != "" || input.Revision != 0 || input.Changes != nil {
			return requestidempotency.CanonicalIntentV1{}, errs.New(
				errs.KindValidationFailed,
				"connector check requires a draft or a saved Connector revision, not both",
			)
		}
		draft = connectorCreationBody(*input.Draft)
	} else {
		if ids.Validate(ids.KindConnector, input.ConnectorID) != nil || input.Revision <= 0 {
			return requestidempotency.CanonicalIntentV1{}, errs.New(errs.KindValidationFailed, "saved connector check requires its id and current revision")
		}
		if input.Changes != nil {
			changes = connectorEditBody(*input.Changes)
		}
	}
	return requestidempotency.CanonicalIntentV1{Method: http.MethodPost, Route: connectorCheckRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "environment", Value: requestidempotency.String(environmentID)},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "connector_id", Value: requestidempotency.String(input.ConnectorID)},
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{
				Name:  "draft",
				Value: draft,
			},
			requestidempotency.Field{Name: "changes", Value: changes},
		))}, nil
}

func (service *CheckService) admitConnectionCheck(ctx context.Context, environmentID string,
	input apiTypes.ConnectorCheckRequest, locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
) (idempotencyrecord.IdempotencyResponse, error) {
	environment, err := service.hierarchy.GetEnvironment(ctx, environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var candidate connectorrecord.Record
	var replacements map[string]string
	if input.Draft != nil {
		candidate, replacements, err = prepareConnectorCreation(ids.New(ids.KindConnector), environmentID, *input.Draft)
	} else {
		current, loadErr := service.connectors.GetConnector(ctx, input.ConnectorID)
		if loadErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, loadErr
		}
		if current.Revision != input.Revision || current.Record.Connector.EnvironmentID != environmentID {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "connector check revision or Environment does not match; refresh the Connector")
		}
		candidate = current.Record
		replacements = make(map[string]string, 2)
		if input.Changes != nil {
			candidate, err = applyConnectorEdit(candidate, *input.Changes)
			for name, credential := range input.Changes.Credentials {
				if credential.Value != "" {
					replacements[name] = credential.Value
				}
			}
		}
	}
	defer clearConnectorDirectValues(replacements)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	source, err := service.connectors.LoadConnectorCheckSource(ctx, environment, project, candidate, input.Revision,
		input.Draft != nil || input.Changes != nil)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer source.Clear()
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	checkOwner := connectorchecks.Owner{
		TaskID:        ids.New(ids.KindTask),
		WorkItemID:    ids.New(ids.KindWorkItem),
		PlanID:        ids.New(ids.KindPlan),
		OperationID:   ids.New(ids.KindOperation),
		EnvironmentID: environmentID,
	}
	credentials, sources, err := service.captureCredentials(ctx, source, checkOwner.OperationID, replacements)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(credentials.Ciphertext)
	now := time.Now().UTC()
	check, err := connectorchecks.NewRecord(
		checkOwner,
		source.Connector(),
		source.Revision(),
		source.Draft(),
		credentials,
		sources,
		now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer check.Clear()
	planHash, err := connectorchecks.PlanHash(check)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := workexecution.Task{ID: checkOwner.TaskID,
		Intent: workexecution.Input{ID: checkOwner.PlanID, SHA256: planHash},
		Scope: workexecution.Scope{
			TenantID:      owner.TenantID,
			ProjectID:     owner.ProjectID,
			EnvironmentID: owner.EnvironmentID,
		},
		Actor: workexecution.OperatorActor, Action: workexecution.ActionCheck,
		TargetID: candidate.Connector.ID, TargetName: candidate.Connector.Name,
		ExpectedItems: 1, Deadline: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	return service.publishConnectionCheck(ctx, source, check, task, locator, evidence)
}

func (service *CheckService) publishConnectionCheck(ctx context.Context, source etcd.ConnectorCheckSource,
	check connectorchecks.Record, task workexecution.Task, locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
) (idempotencyrecord.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker := idempotencyrecord.IdempotencyMarker{Kind: idempotencyrecord.IdempotencyMarkerTask,
		State: idempotencyrecord.IdempotencyMarkerPending, Locator: locator, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	result, err := service.connectors.CreateConnectorCheck(ctx, source, check, task, marker)
	if err != nil {
		if !isUnknownConnectorCreationOutcome(err) {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, evidence, err)
		return resolution.Response, resolveErr
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return resolution.Response, nil
}
