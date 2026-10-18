package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
)

// Rationale: a successful Agent config replacement must commit the desired
// config and completed exact-response marker in the same protected transaction.
func TestLocalAgentConfigReplacementCommitsWithIdempotencyMarker(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	store.revision = 1
	repository, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	record := agentConfigurationTestRecord()
	current, err := repository.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatalf("CreateSingleton() error = %v", err)
	}
	responseBody := []byte(`{"pull_interval_seconds":5,"max_concurrent_tasks":2,"labels":{"zone":"edge"}}`)
	marker := agentConfigurationTestMarker()
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
	prepared, err := repository.PrepareConfigurationUpdate(ctx, current, desired, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	conditions, mutations, err := prepared.Changes()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := base.NewIdempotencyMutationPlan(conditions, mutations, prepared.ClassifyConflict)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := base.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	updated := prepared.Stored(result.Revision())
	if err != nil {
		t.Fatalf("Apply(Agent configuration) error = %v", err)
	}
	outcome, _, conflict, classificationErr := result.Classify()
	if updated.Record.Config.Labels["zone"] != "edge" || outcome != base.IdempotencyKnownApplied || conflict != nil ||
		classificationErr != nil {
		t.Fatalf("Apply(Agent configuration) = %#v/%#v", updated, result)
	}
	stored, err := repository.GetSingleton(ctx)
	if err != nil || stored.Record.Config.PullIntervalSeconds != desired.PullIntervalSeconds ||
		stored.Record.Config.MaxConcurrentTasks != desired.MaxConcurrentTasks ||
		!maps.Equal(stored.Record.Config.Labels, desired.Labels) {
		t.Fatalf("GetSingleton() = %#v, %v", stored, err)
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

func agentConfigurationTestRecord() testlocalagents.LocalAgentRecord {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("test-registration-token"))
	return testlocalagents.LocalAgentRecord{
		ID: ids.New(ids.KindAgent), EnrollmentTaskID: ids.New(ids.KindTask),
		Image: "ghcr.io/groundplane/agent@sha256:" + strings.Repeat("a", 64), Generation: 1,
		Phase: testlocalagents.LocalAgentPhaseProvisioning,
		Config: testlocalagents.LocalAgentConfig{
			PullIntervalSeconds: 5,
			MaxConcurrentTasks:  4,
			Labels:              map[string]string{"role": "local"},
		},
		EncryptedToken: []byte("age-encrypted-token"), TokenDigest: base64.RawURLEncoding.EncodeToString(digest[:]),
		CreatedAt: at, TokenUpdatedAt: at,
	}
}

func agentConfigurationTestMarker() testidempotency.IdempotencyMarker {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	ciphertext := []byte("protected-intent")
	digest := sha256.Sum256(ciphertext)
	return testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerDirect, State: testidempotency.IdempotencyMarkerCompleted,
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		CreatedAt: at, UpdatedAt: at, TerminalAt: at, RetainUntil: at.Add(testidempotency.MarkerRetention),
	}
}
