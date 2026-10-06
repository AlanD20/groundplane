package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type connectorMutationService struct {
	*connectorCreationService
	records *durableConnectorCreationRepository
	intents *durableConnectorCreationIdempotency
}

func NewMutationService(
	repository *durableConnectorCreationRepository,
	protector *secretvalue.Protector,
	idempotency *durableConnectorCreationIdempotency,
) (*connectorMutationService, error) {
	creation, err := NewCreationService(repository, protector, idempotency)
	if err != nil {
		return nil, err
	}
	return &connectorMutationService{connectorCreationService: creation, records: repository, intents: idempotency}, nil
}

func (service *connectorMutationService) EditConnector(
	ctx context.Context, id string, revision int64, input apiTypes.ConnectorEditRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.records.connectors.GetConnector(ctx, id)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := connectorEditIntent(current.Record.Connector.EnvironmentID, id, revision, input)
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	protected, err := service.intents.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	evidence := connectorCreationEvidence{candidate: protected}
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: current.Record.Connector.EnvironmentID,
		Method: http.MethodPatch, Route: "/api/v1/connectors/{id}", Key: key,
	}
	resolution, existing, err := service.intents.ResolveExisting(ctx, locator, evidence)
	if err != nil || existing {
		return resolution.Response, err
	}
	if revision <= 0 || current.Revision != revision {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"connector changed; reload it before editing",
		)
	}
	environment, err := service.records.GetEnvironment(ctx, current.Record.Connector.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.records.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	credentials, err := service.records.connectors.GetConnectorCredentials(ctx, current)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(credentials.Ciphertext)
	credentialDigest := credentials.CiphertextSHA256
	record, err := applyConnectorEdit(current.Record, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if len(input.Credentials) != 0 {
		replacement, replaceErr := service.editCredentials(ctx, current.Record, credentials, input.Credentials)
		if replaceErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, replaceErr
		}
		clear(credentials.Ciphertext)
		credentials = replacement
		defer clear(replacement.Ciphertext)
	}
	body, err := json.Marshal(connectorResponse(record))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusOK,
		ContentKind: "application/json",
		Body:        body,
	}
	marker, err := service.intents.NewMarker(evidence, locator, response, service.now())
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, err := service.records.connectors.EditConnectorIdempotent(
		ctx,
		environment,
		project,
		current,
		credentialDigest,
		record,
		credentials,
		marker,
	)
	if err != nil {
		if isUnknownConnectorCreationOutcome(err) {
			resolved, resolveErr := service.intents.ResolveUnknown(ctx, locator, evidence, err)
			return resolved.Response, resolveErr
		}
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	resolution, err = service.intents.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return response, nil
	}
	return resolution.Response, nil
}

func (service *connectorMutationService) editCredentials(
	ctx context.Context, current connectorrecord.Record, encrypted connectorrecord.EncryptedCredentials,
	changes map[string]apiTypes.ConnectorCredentialInput,
) (connectorrecord.EncryptedCredentials, error) {
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(
			encrypted.EnvelopeVersion,
		), Cipher: secretvalue.CipherSuite(encrypted.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(encrypted.DigestAlgorithm),
			Value:     encrypted.CiphertextSHA256,
		},
	}, encrypted.Ciphertext)
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, err
	}
	defer envelope.Clear()
	direct := make(map[string]string, 2)
	defer clearConnectorDirectValues(direct)
	err = service.protector.Open(ctx, envelope, func(plaintext []byte) error {
		if decodeErr := json.Unmarshal(plaintext, &direct); decodeErr != nil {
			return errs.New(errs.KindInternal, "connector encrypted credentials are invalid")
		}
		return nil
	})
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, err
	}
	for name, change := range changes {
		delete(direct, name)
		if change.Value != "" {
			direct[name] = change.Value
		}
	}
	plaintext, err := json.Marshal(direct)
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(plaintext)
	replacement, err := service.protector.Seal(ctx, plaintext)
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, err
	}
	defer replacement.Clear()
	ciphertext := replacement.Ciphertext()
	defer clear(ciphertext)
	return connectorrecord.NewEncryptedCredentials(current.Connector.ID, ciphertext)
}

func connectorEditIntent(
	environmentID, id string,
	revision int64,
	input apiTypes.ConnectorEditRequest,
) requestidempotency.CanonicalIntentV1 {
	optional := func(value *string) requestidempotency.Value {
		if value == nil {
			return requestidempotency.Null()
		}
		return requestidempotency.String(*value)
	}
	pathStyle := requestidempotency.Null()
	if input.PathStyle != nil {
		pathStyle = requestidempotency.Bool(*input.PathStyle)
	}
	names := make([]string, 0, len(input.Credentials))
	for name := range input.Credentials {
		names = append(names, name)
	}
	sort.Strings(names)
	credentials := make([]requestidempotency.Value, 0, len(names))
	for _, name := range names {
		credential := input.Credentials[name]
		credentials = append(credentials, requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(name)},
			requestidempotency.Field{Name: "secret_ref", Value: requestidempotency.String(credential.SecretRef)},
			requestidempotency.Field{Name: "value", Value: requestidempotency.String(credential.Value)},
		))
	}
	return requestidempotency.CanonicalIntentV1{
		Method: http.MethodPatch, Route: "/api/v1/connectors/{id}",
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: id}},
		Query: requestidempotency.Object(
			requestidempotency.Field{
				Name:  "revision",
				Value: requestidempotency.String(strconv.FormatInt(revision, 10)),
			},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: optional(input.Name)},
			requestidempotency.Field{Name: "endpoint", Value: optional(input.Endpoint)},
			requestidempotency.Field{Name: "bucket", Value: optional(input.Bucket)},
			requestidempotency.Field{Name: "prefix", Value: optional(input.Prefix)},
			requestidempotency.Field{Name: "region", Value: optional(input.Region)},
			requestidempotency.Field{Name: "path_style", Value: pathStyle},
			requestidempotency.Field{Name: "credentials", Value: requestidempotency.List(credentials...)},
		)),
	}
}
