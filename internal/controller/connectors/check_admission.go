package connectors

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
	submitter   *acceptedaction.Submitter
}

func NewCheckService(
	hierarchy *etcd.HierarchyRepository,
	connectors *etcd.ConnectorRepository,
	secrets *etcd.SecretRepository,
	checks *connectorchecks.Repository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	protector *secretvalue.Protector,
	submitter *acceptedaction.Submitter,
) (*CheckService, error) {
	if hierarchy == nil || connectors == nil || secrets == nil || checks == nil || idempotency == nil ||
		coordinator == nil ||
		protector == nil || submitter == nil {
		return nil, errs.New(errs.KindInternal, "connector check admission dependencies are required")
	}
	return &CheckService{hierarchy: hierarchy, connectors: connectors, secrets: secrets, checks: checks,
		idempotency: idempotency, coordinator: coordinator, protector: protector, submitter: submitter}, nil
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
		if ids.Validate(ids.KindConnector, input.ConnectorID) != nil || input.Revision < 0 || input.Changes != nil && input.Revision == 0 {
			return requestidempotency.CanonicalIntentV1{}, errs.New(errs.KindValidationFailed, "saved Connector checks require its id; unsaved changes also require their exact base revision")
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
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
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
	connectorID := input.ConnectorID
	if input.Draft != nil {
		connectorID = ids.New(ids.KindConnector)
	}
	candidate, replacements, _, err := service.checkCandidate(ctx, environmentID, connectorID, input)
	defer clearConnectorDirectValues(replacements)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var secretIDs map[string]string
	var retained []string
	if input.Draft != nil || input.Changes != nil || input.Revision != 0 {
		secretIDs, retained, err = service.checkSecretIDs(ctx, project.Record.ID, candidate)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionCheck,
		TargetID:   candidate.Connector.ID,
		TargetName: candidate.Connector.Name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environmentID,
		},
		After: append(
			[]string(nil),
			input.After...),
		CreatedAt: now,
		UpdatedAt: now,
		Deadline:  now.Add(acceptedaction.QueueTimeout),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(
		ctx,
		service.submitter,
		acceptedaction.Submission{Task: task, Kind: "connector-check", Locator: locator,
			Intent: durable, Evidence: evidence, SecretIDs: retained},
		acceptedConnectorCheck{Request: input, SecretIDs: secretIDs},
	)
}

func (service *CheckService) checkCandidate(
	ctx context.Context,
	environmentID, connectorID string,
	input apiTypes.ConnectorCheckRequest,
) (connectorrecord.Record, map[string]string, int64, error) {
	var candidate connectorrecord.Record
	var err error
	var revision int64
	var replacements map[string]string
	if input.Draft != nil {
		candidate, replacements, err = prepareConnectorCreation(connectorID, environmentID, *input.Draft)
	} else {
		current, loadErr := service.connectors.GetConnector(ctx, input.ConnectorID)
		if loadErr != nil {
			return connectorrecord.Record{}, nil, 0, loadErr
		}
		if input.Revision != 0 && current.Revision != input.Revision || current.Record.Connector.EnvironmentID != environmentID {
			return connectorrecord.Record{}, nil, 0, errs.New(errs.KindStateConflict, "connector check revision or Environment does not match; refresh the Connector")
		}
		revision = current.Revision
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
	if err != nil {
		clearConnectorDirectValues(replacements)
		return connectorrecord.Record{}, nil, 0, err
	}
	return candidate, replacements, revision, nil
}
