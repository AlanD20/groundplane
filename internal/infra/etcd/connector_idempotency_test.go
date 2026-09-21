package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestConnectorIdempotentCreateCommitsCredentialsAndReplays(t *testing.T) {
	// Rationale: a retry must return the original completed response without
	// creating another Connector or rewriting its encrypted credentials.
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	record := testConnectorRecord(t, environment.Record.ID, now, 50, "idempotent-backups")
	credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("encrypted-value"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     "/api/v1/connectors",
		Key:       "connector-create-key-0001",
	}
	marker.Response.Status = http.StatusCreated

	result, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials, marker,
	)
	if err != nil {
		t.Fatalf("CreateConnectorIdempotent() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateConnectorIdempotent() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	stored, err := repository.GetConnector(context.Background(), record.Connector.ID)
	if err != nil || stored.Record.Connector.ID != record.Connector.ID {
		t.Fatalf("GetConnector() = %#v, %v", stored, err)
	}
	storedCredentials, err := repository.GetConnectorCredentials(context.Background(), stored)
	if err != nil || string(storedCredentials.Ciphertext) != "encrypted-value" {
		t.Fatalf("GetConnectorCredentials() = %#v, %v", storedCredentials, err)
	}
	clear(storedCredentials.Ciphertext)

	replay, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials, marker,
	)
	if err != nil {
		t.Fatalf("CreateConnectorIdempotent(replay) error = %v", err)
	}
	outcome, existing, conflict, err := replay.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting || existing.Locator != marker.Locator {
		t.Fatalf(
			"CreateConnectorIdempotent(replay) outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			err,
		)
	}
}

// Rationale: a platform fallback is valid only while the Project key remains
// absent and the selected Secret metadata, ciphertext, and tombstone remain
// exactly the evidence resolved before Connector creation commits.
func TestConnectorIdempotentCreateFencesPlatformSecretFallback(t *testing.T) {
	store, environment, project := testConnectorHierarchy(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, now, 70)
	secret, err := testsecrets.NewPlatformRecord(secretID, "S3_ACCESS_KEY", core.SecretKindEnvVar, "", now)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	if _, err := secrets.CreateSecret(
		context.Background(), testsecrets.PlatformOwner(), secret,
		testSecretEncryptedValue(secretID, "platform-access-key"),
	); err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	record := testConnectorRecord(t, environment.Record.ID, now, 71, "platform-secret-backups")
	record.Connector.Credentials[core.ConnectorCredentialAccessKey] = core.ConnectorCredential{
		Kind: core.ConnectorCredentialSecretRef, SecretRef: "S3_ACCESS_KEY",
	}
	credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("encrypted-direct-values"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	marker := connectorCreateMarker(environment.Record.ID, "connector-create-key-0002")
	if _, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials, marker,
	); err != nil {
		t.Fatalf("CreateConnectorIdempotent(platform fallback) error = %v", err)
	}
}

// Rationale: Secret rotation or deletion racing the final Connector CAS must
// fail the whole creation rather than committing metadata against stale
// credential authority.
func TestConnectorIdempotentCreateRejectsSecretValueRace(t *testing.T) {
	base, environment, project := testConnectorHierarchy(t)
	secrets, err := newSecretRepository(base)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 24, 9, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, now, 80)
	secret, err := testsecrets.NewProjectRecord(
		secretID, project.Record.ID, "S3_ACCESS_KEY", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	if _, err := secrets.CreateSecret(
		context.Background(), testsecrets.ProjectOwner(project), secret,
		testSecretEncryptedValue(secretID, "project-access-key"),
	); err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	racing := &connectorSecretRaceStore{hierarchyStore: base}
	racing.beforeTransact = func() {
		changed, encodeErr := testsecrets.EncodeEncryptedValue(testSecretEncryptedValue(secretID, "rotated-access-key"))
		if encodeErr != nil {
			t.Fatalf("encodeSecretEncryptedValue() error = %v", encodeErr)
		}
		defer clear(changed)
		result, mutateErr := base.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testsecrets.ValueKey(secretID), Value: changed,
		}})
		if mutateErr != nil || !result.Succeeded {
			t.Fatalf("rotate Secret = %#v, %v", result, mutateErr)
		}
	}
	repository, err := newConnectorRepository(racing)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	record := testConnectorRecord(t, environment.Record.ID, now, 81, "racing-secret-backups")
	record.Connector.Credentials[core.ConnectorCredentialAccessKey] = core.ConnectorCredential{
		Kind: core.ConnectorCredentialSecretRef, SecretRef: "S3_ACCESS_KEY",
	}
	credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("encrypted-direct-values"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	result, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials,
		connectorCreateMarker(environment.Record.ID, "connector-create-key-0003"),
	)
	if err != nil {
		t.Fatalf("CreateConnectorIdempotent(Secret race) error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if kind, ok := errs.KindOf(conflict); err != nil || !ok || kind != errs.KindStateConflict ||
		outcome != IdempotencyKnownConflict {
		t.Fatalf("CreateConnectorIdempotent(Secret race) result = %v/%v/%v", outcome, conflict, err)
	}
	stored, getErr := base.Get(context.Background(), testconnectors.RecordKey(record.Connector.ID))
	if getErr != nil || stored.Entry != nil {
		t.Fatalf("racing Connector primary = %#v, %v", stored, getErr)
	}
}

type connectorSecretRaceStore struct {
	hierarchyStore
	beforeTransact func()
}

func (store *connectorSecretRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.beforeTransact != nil {
		before := store.beforeTransact
		store.beforeTransact = nil
		before()
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

func connectorCreateMarker(environmentID string, key string) testidempotency.IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPost,
		Route:     "/api/v1/connectors",
		Key:       key,
	}
	marker.Response.Status = http.StatusCreated
	return marker
}
