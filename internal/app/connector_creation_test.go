package app

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeConnectorCreationRepository struct {
	environment  etcd.Versioned[etcd.EnvironmentRecord]
	project      etcd.Versioned[etcd.ProjectRecord]
	secretKind   core.SecretKind
	resolveCalls int
	createCalls  int
	record       etcd.ConnectorRecord
	credentials  etcd.ConnectorEncryptedCredentials
	marker       etcd.IdempotencyMarker
}

func (repository *fakeConnectorCreationRepository) GetEnvironment(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.environment, nil
}

func (repository *fakeConnectorCreationRepository) GetProject(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.project, nil
}

func (repository *fakeConnectorCreationRepository) ResolveSecret(
	_ context.Context,
	projectID string,
	reference string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	repository.resolveCalls++
	return etcd.Versioned[etcd.SecretRecord]{Record: etcd.SecretRecord{Secret: core.Secret{
		ID: ids.New(ids.KindSecret), Scope: core.SecretScopeProject, ProjectID: projectID,
		Key: reference, Kind: repository.secretKind,
	}}}, nil
}

func (repository *fakeConnectorCreationRepository) CreateConnectorIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	record etcd.ConnectorRecord,
	credentials etcd.ConnectorEncryptedCredentials,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.createCalls++
	repository.record = record
	repository.credentials = credentials
	repository.credentials.Ciphertext = append([]byte(nil), credentials.Ciphertext...)
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeConnectorCreationIdempotency struct {
	existing bool
}

func (*fakeConnectorCreationIdempotency) Prepare(
	_ context.Context,
	_ string,
	_ apiTypes.ConnectorCreateRequest,
) (connectorCreationEvidence, error) {
	return connectorCreationEvidence{}, nil
}

func (idempotency *fakeConnectorCreationIdempotency) ResolveExisting(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ connectorCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay}, idempotency.existing, nil
}

func (*fakeConnectorCreationIdempotency) NewMarker(
	_ connectorCreationEvidence,
	locator etcd.IdempotencyLocator,
	response etcd.IdempotencyResponse,
	_ time.Time,
) (etcd.IdempotencyMarker, error) {
	return etcd.IdempotencyMarker{Locator: locator, Response: response}, nil
}

func (*fakeConnectorCreationIdempotency) ResolveKnown(
	_ context.Context,
	_ connectorCreationEvidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*fakeConnectorCreationIdempotency) ResolveUnknown(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ connectorCreationEvidence,
	_ error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

type connectorCreationCipher struct{}

func (connectorCreationCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte("sealed:"), plaintext...), nil
}

func (connectorCreationCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return bytes.TrimPrefix(append([]byte(nil), ciphertext...), []byte("sealed:")), nil
}

func TestConnectorCreationResolvesReferencesAndSealsOnlyDirectValues(t *testing.T) {
	// Rationale: reusable references remain dynamic metadata while direct
	// plaintext is confined to the encrypted subordinate bundle.
	repository, environmentID := testConnectorCreationRepository(t, core.SecretKindEnvVar)
	protector, err := secretvalue.NewProtector(connectorCreationCipher{}, connectorCreationCipher{})
	if err != nil {
		t.Fatalf("secretvalue.NewProtector() error = %v", err)
	}
	service, err := newConnectorCreationService(
		repository, protector, &fakeConnectorCreationIdempotency{},
	)
	if err != nil {
		t.Fatalf("newConnectorCreationService() error = %v", err)
	}
	pathStyle := true
	response, err := service.CreateConnector(
		context.Background(), environmentID, apiTypes.ConnectorCreateRequest{
			Name: "primary-backups", Kind: "s3-compatible",
			Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
			Prefix: "production/", Region: "auto", PathStyle: &pathStyle,
			Credentials: map[string]apiTypes.ConnectorCredentialInput{
				string(core.ConnectorCredentialAccessKey): {SecretRef: "S3_ACCESS_KEY"},
				string(core.ConnectorCredentialSecretKey): {Value: "direct-secret"},
			},
		}, "connector-create-key-0001",
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	defer clear(response.Body)
	defer clear(repository.credentials.Ciphertext)
	defer clear(repository.marker.Intent.Ciphertext)
	defer clear(repository.marker.Response.Body)
	if response.Status != 201 || repository.resolveCalls != 1 ||
		repository.createCalls != 1 || repository.marker.Response.Status != 201 {
		t.Fatalf("CreateConnector() response/repository = %#v/%#v", response, repository)
	}
	if !bytes.Contains(repository.credentials.Ciphertext, []byte(`"secret_key":"direct-secret"`)) ||
		bytes.Contains(repository.credentials.Ciphertext, []byte("S3_ACCESS_KEY")) {
		t.Fatalf("sealed direct bundle = %q", repository.credentials.Ciphertext)
	}
	if repository.record.Connector.Credentials[core.ConnectorCredentialSecretKey].SecretRef != "" ||
		repository.record.Connector.Credentials[core.ConnectorCredentialAccessKey].SecretRef != "S3_ACCESS_KEY" {
		t.Fatalf("durable Connector credentials = %#v", repository.record.Connector.Credentials)
	}
	var shown apiTypes.Connector
	if err := json.Unmarshal(response.Body, &shown); err != nil {
		t.Fatalf("json.Unmarshal(response) error = %v", err)
	}
	if shown.ID == "" || bytes.Contains(response.Body, []byte("direct-secret")) {
		t.Fatalf("CreateConnector() response = %s", response.Body)
	}
}

func TestConnectorCreationRejectsNonEnvironmentSecretReference(t *testing.T) {
	repository, environmentID := testConnectorCreationRepository(t, core.SecretKindFile)
	protector, err := secretvalue.NewProtector(connectorCreationCipher{}, connectorCreationCipher{})
	if err != nil {
		t.Fatalf("secretvalue.NewProtector() error = %v", err)
	}
	service, err := newConnectorCreationService(
		repository, protector, &fakeConnectorCreationIdempotency{},
	)
	if err != nil {
		t.Fatalf("newConnectorCreationService() error = %v", err)
	}
	pathStyle := true
	_, err = service.CreateConnector(
		context.Background(), environmentID, apiTypes.ConnectorCreateRequest{
			Name: "primary-backups", Kind: "s3-compatible",
			Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
			Region: "auto", PathStyle: &pathStyle,
			Credentials: map[string]apiTypes.ConnectorCredentialInput{
				string(core.ConnectorCredentialAccessKey): {SecretRef: "S3_ACCESS_KEY"},
				string(core.ConnectorCredentialSecretKey): {Value: "direct-secret"},
			},
		}, "connector-create-key-0002",
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed || repository.createCalls != 0 {
		t.Fatalf("CreateConnector(file Secret) error/calls = %v/%d", err, repository.createCalls)
	}
}

func testConnectorCreationRepository(
	t *testing.T,
	secretKind core.SecretKind,
) (*fakeConnectorCreationRepository, string) {
	t.Helper()
	now := time.Date(2026, 8, 23, 19, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	return &fakeConnectorCreationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{
			ID: environmentID, ProjectID: projectID,
		}},
		project: etcd.Versioned[etcd.ProjectRecord]{Record: etcd.ProjectRecord{
			ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant,
		}},
		secretKind: secretKind,
	}, environmentID
}
