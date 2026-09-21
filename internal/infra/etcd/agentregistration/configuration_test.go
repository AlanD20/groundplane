package agentregistration

import (
	"bytes"
	"context"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"maps"
	"net/http"
	"testing"
)

// Rationale: a successful Agent config replacement must commit the desired
// config and completed exact-response marker in the same protected transaction.
func TestLocalAgentConfigReplacementCommitsWithIdempotencyMarker(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
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
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    http.MethodPut,
		Route:     "/agents/{id}/config",
		Key:       "agent-config-key-0001",
	}
	marker.Response = testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: responseBody,
	}
	desired := testlocalagents.LocalAgentConfig{
		PullIntervalSeconds: 5,
		MaxConcurrentTasks:  2,
		Labels:              map[string]string{"zone": "edge"},
	}
	updated, result, err := repository.UpdateConfigIdempotent(ctx, current, desired, marker)
	if err != nil {
		t.Fatalf("UpdateConfigIdempotent() error = %v", err)
	}
	outcome, _, conflict, classificationErr := result.Classify()
	if updated.Record.Config.Labels["zone"] != "edge" || outcome != base.IdempotencyKnownApplied || conflict != nil ||
		classificationErr != nil {
		t.Fatalf("UpdateConfigIdempotent() = %#v/%#v", updated, result)
	}
	stored, err := repository.GetSingleton(ctx)
	if err != nil || stored.Record.Config.PullIntervalSeconds != desired.PullIntervalSeconds ||
		stored.Record.Config.MaxConcurrentTasks != desired.MaxConcurrentTasks ||
		!maps.Equal(stored.Record.Config.Labels, desired.Labels) {
		t.Fatalf("GetSingleton() = %#v, %v", stored, err)
	}
	idempotency, err := base.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil || evidence == nil {
		t.Fatalf("Read(marker) = %#v, %v", evidence, err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || !bytes.Equal(storedMarker.Response.Body, responseBody) {
		t.Fatalf("Marker() = %#v, %v", storedMarker, err)
	}
}
