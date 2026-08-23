package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"
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
	credentials, err := NewConnectorEncryptedCredentials(record.Connector.ID, []byte("encrypted-value"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
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
