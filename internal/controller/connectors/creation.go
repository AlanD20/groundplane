package connectors

import (
	"context"
	"encoding/json"
	"errors"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"net/http"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type connectorCreationRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[hierarchyrecord.ProjectRecord], error)
	ResolveSecret(context.Context, string, string) (etcd.Versioned[secretrecord.Record], error)
	CreateConnectorIdempotent(
		context.Context,
		etcd.Versioned[hierarchyrecord.EnvironmentRecord],
		etcd.Versioned[hierarchyrecord.ProjectRecord],
		connectorrecord.Record,
		connectorrecord.EncryptedCredentials,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type connectorCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
}

type connectorCreationIdempotency interface {
	Prepare(context.Context, string, apiTypes.ConnectorCreateRequest) (connectorCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		connectorCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	NewMarker(
		connectorCreationEvidence,
		etcd.IdempotencyLocator,
		etcd.IdempotencyResponse,
		time.Time,
	) (etcd.IdempotencyMarker, error)
	ResolveKnown(
		context.Context,
		connectorCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		connectorCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableConnectorCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableConnectorCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Connector creation idempotency dependencies are required")
	}
	return &durableConnectorCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableConnectorCreationIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
) (connectorCreationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(
		ctx,
		connectorCreationIntent(environmentID, input),
	)
	if err != nil {
		return connectorCreationEvidence{}, err
	}
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return connectorCreationEvidence{}, err
	}
	return connectorCreationEvidence{candidate: evidence}, nil
}

func (service *durableConnectorCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence connectorCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (*durableConnectorCreationIdempotency) NewMarker(
	evidence connectorCreationEvidence,
	locator etcd.IdempotencyLocator,
	response etcd.IdempotencyResponse,
	now time.Time,
) (etcd.IdempotencyMarker, error) {
	intent, err := evidence.candidate.DurableRecord()
	if err != nil {
		return etcd.IdempotencyMarker{}, err
	}
	defer clear(intent.Ciphertext)
	return etcd.NewCompletedDirectIdempotencyMarker(locator, intent, response, now)
}

func (service *durableConnectorCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence connectorCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableConnectorCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence connectorCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}

type connectorCreationService struct {
	repository  connectorCreationRepository
	protector   *secretvalue.Protector
	idempotency connectorCreationIdempotency
	now         func() time.Time
}

func NewCreationService(
	repository connectorCreationRepository,
	protector *secretvalue.Protector,
	idempotency connectorCreationIdempotency,
) (*connectorCreationService, error) {
	if repository == nil || protector == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Connector creation dependencies are required")
	}
	return &connectorCreationService{
		repository: repository, protector: protector, idempotency: idempotency,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (service *connectorCreationService) CreateConnector(
	ctx context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	resolution, err := service.resolveConnectorCreation(ctx, environmentID, input, idempotencyKey)
	return resolution.Response, err
}

func (service *connectorCreationService) resolveConnectorCreation(
	ctx context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
	idempotencyKey string,
) (requestidempotency.Resolution, error) {
	evidence, err := service.idempotency.Prepare(ctx, environmentID, input)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	locator := connectorCreationLocator(environmentID, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil || existing {
		return resolution, err
	}
	return service.createConnectorOnce(ctx, environmentID, input, locator, evidence)
}

func (service *connectorCreationService) createConnectorOnce(
	ctx context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
	locator etcd.IdempotencyLocator,
	evidence connectorCreationEvidence,
) (requestidempotency.Resolution, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	record, direct, err := prepareConnectorCreation(ids.New(ids.KindConnector), environmentID, input)
	defer clearConnectorDirectValues(direct)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	credentialNames := make([]string, 0, len(record.Connector.Credentials))
	for name := range record.Connector.Credentials {
		credentialNames = append(credentialNames, string(name))
	}
	sort.Strings(credentialNames)
	for _, name := range credentialNames {
		credential := record.Connector.Credentials[core.ConnectorCredentialName(name)]
		if credential.Kind != core.ConnectorCredentialSecretRef {
			continue
		}
		secret, resolveErr := service.repository.ResolveSecret(
			ctx,
			project.Record.ID,
			credential.SecretRef,
		)
		if resolveErr != nil {
			return requestidempotency.Resolution{}, resolveErr
		}
		if secret.Record.Secret.Kind != core.SecretKindEnvVar {
			return requestidempotency.Resolution{}, errs.New(
				errs.KindValidationFailed,
				"Connector credential secret_ref must name an env_var Secret",
			)
		}
	}
	plaintext, err := json.Marshal(direct)
	if err != nil {
		return requestidempotency.Resolution{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(plaintext)
	envelope, err := service.protector.Seal(ctx, plaintext)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	credentials, err := connectorrecord.NewEncryptedCredentials(record.Connector.ID, ciphertext)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	defer clear(credentials.Ciphertext)
	responseBody, err := json.Marshal(connectorResponse(record))
	if err != nil {
		return requestidempotency.Resolution{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	defer clear(response.Body)
	marker, err := service.idempotency.NewMarker(evidence, locator, response, service.now())
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateConnectorIdempotent(
		ctx,
		environment,
		project,
		record,
		credentials,
		marker,
	)
	if createErr != nil {
		if isUnknownConnectorCreationOutcome(createErr) {
			return service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
		}
		return requestidempotency.Resolution{}, createErr
	}
	resolution, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return requestidempotency.Resolution{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		resolution.Response = etcd.IdempotencyResponse{
			Status: response.Status, ContentKind: response.ContentKind,
			Body: append([]byte(nil), response.Body...),
		}
	}
	return resolution, nil
}

func connectorCreationLocator(environmentID string, key string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: "/api/v1/connectors", Key: key,
	}
}

func connectorCreationIntent(
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
) requestidempotency.CanonicalIntentV1 {
	pathStyle := requestidempotency.Null()
	if input.PathStyle != nil {
		pathStyle = requestidempotency.Bool(*input.PathStyle)
	}
	credentialNames := make([]string, 0, len(input.Credentials))
	for name := range input.Credentials {
		credentialNames = append(credentialNames, name)
	}
	sort.Strings(credentialNames)
	credentials := make([]requestidempotency.Value, 0, len(credentialNames))
	for _, name := range credentialNames {
		credential := input.Credentials[name]
		credentials = append(credentials, requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(name)},
			requestidempotency.Field{Name: "secret_ref", Value: requestidempotency.String(credential.SecretRef)},
			requestidempotency.Field{Name: "value", Value: requestidempotency.String(credential.Value)},
		))
	}
	return requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: "/api/v1/connectors",
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "environment", Value: requestidempotency.String(environmentID)},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(input.Name)},
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(input.Kind)},
			requestidempotency.Field{Name: "endpoint", Value: requestidempotency.String(input.Endpoint)},
			requestidempotency.Field{Name: "bucket", Value: requestidempotency.String(input.Bucket)},
			requestidempotency.Field{Name: "prefix", Value: requestidempotency.String(input.Prefix)},
			requestidempotency.Field{Name: "region", Value: requestidempotency.String(input.Region)},
			requestidempotency.Field{Name: "path_style", Value: pathStyle},
			requestidempotency.Field{Name: "credentials", Value: requestidempotency.List(credentials...)},
		)),
	}
}

func isUnknownConnectorCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

type durableConnectorCreationRepository struct {
	hierarchy  *etcd.HierarchyRepository
	secrets    *etcd.SecretRepository
	connectors *etcd.ConnectorRepository
}

func NewCreationRepository(
	hierarchy *etcd.HierarchyRepository,
	secrets *etcd.SecretRepository,
	connectors *etcd.ConnectorRepository,
) (*durableConnectorCreationRepository, error) {
	if hierarchy == nil || secrets == nil || connectors == nil {
		return nil, errs.New(errs.KindInternal, "Connector creation repository dependencies are required")
	}
	return &durableConnectorCreationRepository{
		hierarchy: hierarchy, secrets: secrets, connectors: connectors,
	}, nil
}

func (repository *durableConnectorCreationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableConnectorCreationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableConnectorCreationRepository) ResolveSecret(
	ctx context.Context,
	projectID string,
	reference string,
) (etcd.Versioned[secretrecord.Record], error) {
	return repository.secrets.ResolveSecret(ctx, projectID, reference)
}

func (repository *durableConnectorCreationRepository) CreateConnectorIdempotent(
	ctx context.Context,
	environment etcd.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcd.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.connectors.CreateConnectorIdempotent(
		ctx,
		environment,
		project,
		record,
		credentials,
		marker,
	)
}
