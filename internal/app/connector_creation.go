package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type connectorCreationRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	ResolveSecret(context.Context, string, string) (etcd.Versioned[etcd.SecretRecord], error)
	CreateConnectorIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.ConnectorRecord,
		etcd.ConnectorEncryptedCredentials,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type connectorCreationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
}

type connectorCreationIdempotency interface {
	Prepare(context.Context, string, apiTypes.ConnectorCreateRequest) (connectorCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		connectorCreationEvidence,
	) (idempotentintent.Resolution, bool, error)
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
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		connectorCreationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableConnectorCreationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableConnectorCreationIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	version, digest, err := idempotentintent.Canonicalize(
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
) (idempotentintent.Resolution, bool, error) {
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
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableConnectorCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence connectorCreationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
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

func newConnectorCreationService(
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
) (idempotentintent.Resolution, error) {
	evidence, err := service.idempotency.Prepare(ctx, environmentID, input)
	if err != nil {
		return idempotentintent.Resolution{}, err
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
) (idempotentintent.Resolution, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	record, direct, err := prepareConnectorCreation(ids.New(ids.KindConnector), environmentID, input)
	defer clearConnectorDirectValues(direct)
	if err != nil {
		return idempotentintent.Resolution{}, err
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
			return idempotentintent.Resolution{}, resolveErr
		}
		if secret.Record.Secret.Kind != core.SecretKindEnvVar {
			return idempotentintent.Resolution{}, errs.New(
				errs.KindValidationFailed,
				"Connector credential secret_ref must name an env_var Secret",
			)
		}
	}
	plaintext, err := json.Marshal(direct)
	if err != nil {
		return idempotentintent.Resolution{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(plaintext)
	envelope, err := service.protector.Seal(ctx, plaintext)
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	credentials, err := etcd.NewConnectorEncryptedCredentials(record.Connector.ID, ciphertext)
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	defer clear(credentials.Ciphertext)
	responseBody, err := json.Marshal(connectorResponse(record))
	if err != nil {
		return idempotentintent.Resolution{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	defer clear(response.Body)
	marker, err := service.idempotency.NewMarker(evidence, locator, response, service.now())
	if err != nil {
		return idempotentintent.Resolution{}, err
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
		return idempotentintent.Resolution{}, createErr
	}
	resolution, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionApplied {
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
) idempotentintent.CanonicalIntentV1 {
	pathStyle := idempotentintent.Null()
	if input.PathStyle != nil {
		pathStyle = idempotentintent.Bool(*input.PathStyle)
	}
	credentialNames := make([]string, 0, len(input.Credentials))
	for name := range input.Credentials {
		credentialNames = append(credentialNames, name)
	}
	sort.Strings(credentialNames)
	credentials := make([]idempotentintent.Value, 0, len(credentialNames))
	for _, name := range credentialNames {
		credential := input.Credentials[name]
		credentials = append(credentials, idempotentintent.Object(
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(name)},
			idempotentintent.Field{Name: "secret_ref", Value: idempotentintent.String(credential.SecretRef)},
			idempotentintent.Field{Name: "value", Value: idempotentintent.String(credential.Value)},
		))
	}
	return idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: "/api/v1/connectors",
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Query: idempotentintent.Object(
			idempotentintent.Field{Name: "environment", Value: idempotentintent.String(environmentID)},
		),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(input.Name)},
			idempotentintent.Field{Name: "kind", Value: idempotentintent.String(input.Kind)},
			idempotentintent.Field{Name: "endpoint", Value: idempotentintent.String(input.Endpoint)},
			idempotentintent.Field{Name: "bucket", Value: idempotentintent.String(input.Bucket)},
			idempotentintent.Field{Name: "prefix", Value: idempotentintent.String(input.Prefix)},
			idempotentintent.Field{Name: "region", Value: idempotentintent.String(input.Region)},
			idempotentintent.Field{Name: "path_style", Value: pathStyle},
			idempotentintent.Field{Name: "credentials", Value: idempotentintent.List(credentials...)},
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

func newDurableConnectorCreationRepository(
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
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableConnectorCreationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableConnectorCreationRepository) ResolveSecret(
	ctx context.Context,
	projectID string,
	reference string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	return repository.secrets.ResolveSecret(ctx, projectID, reference)
}

func (repository *durableConnectorCreationRepository) CreateConnectorIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	record etcd.ConnectorRecord,
	credentials etcd.ConnectorEncryptedCredentials,
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
