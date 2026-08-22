package etcd

import (
	"bytes"
	"context"
	"net/http"
	"testing"
)

// Rationale: a successful Agent config replacement must commit the desired
// config and completed exact-response marker in the same protected transaction.
func TestLocalAgentConfigReplacementCommitsWithIdempotencyMarker(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newLocalAgentRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	record := localAgentTestRecord(localAgentTestToken(31), taskJournalTime())
	current, err := repository.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatalf("CreateSingleton() error = %v", err)
	}
	responseBody := []byte(`{"pull_interval_seconds":5,"max_concurrent_tasks":2,"labels":{"zone":"edge"}}`)
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    http.MethodPut,
		Route:     "/agents/{id}/config",
		Key:       "agent-config-key-0001",
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: responseBody,
	}
	desired := LocalAgentConfig{
		PullIntervalSeconds: 5,
		MaxConcurrentTasks:  2,
		Labels:              map[string]string{"zone": "edge"},
	}
	updated, result, err := repository.UpdateConfigIdempotent(ctx, current, desired, marker)
	if err != nil {
		t.Fatalf("UpdateConfigIdempotent() error = %v", err)
	}
	if updated.Record.Config.Labels["zone"] != "edge" || result.kind != idempotencyTransactionApplied {
		t.Fatalf("UpdateConfigIdempotent() = %#v/%#v", updated, result)
	}
	stored, err := repository.GetSingleton(ctx)
	if err != nil || !equalLocalAgentConfig(stored.Record.Config, desired) {
		t.Fatalf("GetSingleton() = %#v, %v", stored, err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil || evidence == nil || !bytes.Equal(evidence.marker.Response.Body, responseBody) {
		t.Fatalf("Read(marker) = %#v, %v", evidence, err)
	}
}
