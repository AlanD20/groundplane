package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: a successful protected create must make redacted metadata,
// ciphertext, scope indexes, and exact replay evidence visible atomically.
func TestSecretIdempotentCreateCommitsValueAndReplays(t *testing.T) {
	t.Parallel()
	store, project := testSecretRepositoryProject(t)
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	id := ids.NewAt(ids.KindSecret, now, 50)
	record, err := NewProjectSecretRecord(
		id,
		project.Record.ID,
		"API_TOKEN",
		core.SecretKindEnvVar,
		"",
		now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeProject, ScopeID: project.Record.ID,
		Method: http.MethodPost, Route: "/secrets", Key: "secret-create-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	value := testSecretEncryptedValue(id, "encrypted-value")
	result, err := repository.CreateSecretIdempotent(
		context.Background(), ProjectSecretOwner(project), record, value, marker,
	)
	if err != nil {
		t.Fatalf("CreateSecretIdempotent() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateSecretIdempotent() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	stored, err := repository.GetSecret(context.Background(), id)
	if err != nil || stored.Record != record {
		t.Fatalf("GetSecret() = %#v, %v", stored, err)
	}
	storedValue, err := repository.GetSecretValue(context.Background(), stored)
	if err != nil || string(storedValue.Ciphertext) != "encrypted-value" {
		t.Fatalf("GetSecretValue() metadata/error = %q/%v", storedValue.SecretID, err)
	}
	clear(storedValue.Ciphertext)
	replay, err := repository.CreateSecretIdempotent(
		context.Background(), ProjectSecretOwner(project), record, value, marker,
	)
	if err != nil {
		t.Fatalf("CreateSecretIdempotent(replay) error = %v", err)
	}
	outcome, existing, conflict, err := replay.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting || existing.Locator != marker.Locator {
		t.Fatalf("CreateSecretIdempotent(replay) outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
}

func TestSecretIdempotentPlatformCreateCommitsValueAndReplays(t *testing.T) {
	t.Parallel()
	store := newMemoryHierarchyStore()
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 18, 30, 0, 0, time.UTC)
	id := ids.NewAt(ids.KindSecret, now, 51)
	record, err := NewPlatformSecretRecord(id, "API_TOKEN", core.SecretKindEnvVar, "", now)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: "/secrets", Key: "secret-create-key-0002",
	}
	marker.Response.Status = http.StatusCreated
	value := testSecretEncryptedValue(id, "encrypted-platform-value")
	result, err := repository.CreateSecretIdempotent(
		context.Background(), PlatformSecretOwner(), record, value, marker,
	)
	if err != nil {
		t.Fatalf("CreateSecretIdempotent() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateSecretIdempotent() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	stored, err := repository.GetSecret(context.Background(), id)
	if err != nil || stored.Record != record {
		t.Fatalf("GetSecret() = %#v, %v", stored, err)
	}
	storedValue, err := repository.GetSecretValue(context.Background(), stored)
	if err != nil || string(storedValue.Ciphertext) != "encrypted-platform-value" {
		t.Fatalf("GetSecretValue() metadata/error = %q/%v", storedValue.SecretID, err)
	}
	clear(storedValue.Ciphertext)
	replay, err := repository.CreateSecretIdempotent(
		context.Background(), PlatformSecretOwner(), record, value, marker,
	)
	if err != nil {
		t.Fatalf("CreateSecretIdempotent(replay) error = %v", err)
	}
	outcome, existing, conflict, err := replay.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting || existing.Locator != marker.Locator {
		t.Fatalf("CreateSecretIdempotent(replay) outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
}
