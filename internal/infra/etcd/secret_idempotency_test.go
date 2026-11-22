package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
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
	record, err := testsecrets.NewProjectRecord(
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
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeProject, ScopeID: project.Record.ID,
		Method: http.MethodPost, Route: "/secrets", Key: "secret-create-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	value := testSecretEncryptedValue(id, "encrypted-value")
	result, err := repository.CreateSecretIdempotent(
		context.Background(), testsecrets.ProjectOwner(project), record, value, marker,
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
		context.Background(), testsecrets.ProjectOwner(project), record, value, marker,
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

// Rationale: changing at-rest encryption is one CAS value replacement under
// the same Secret identity; exact retries replay and fixed-revision readers
// retain the prior metadata/value pair.
func TestSecretIdempotentEditRotatesEncryptionAndPreservesPriorRevision(t *testing.T) {
	t.Parallel()
	store, project := testSecretRepositoryProject(t)
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 22, 12, 30, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, now, 60)
	record, err := testsecrets.NewProjectRecord(secretID, project.Record.ID, "API_TOKEN",
		core.SecretKindEnvVar, "", now)
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.CreateSecret(t.Context(), testsecrets.ProjectOwner(project), record,
		testSecretEncryptedValue(secretID, "old-ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	replacement := current.Record
	replacement.Secret.Encryption = "none"
	replacement.Secret.UpdatedAt = now.Add(time.Minute)
	plaintext := []byte("rotated-value")
	digest := sha256.Sum256(plaintext)
	value := testsecrets.StoredValue{SecretID: secretID, EnvelopeVersion: 1, Cipher: "none",
		DigestAlgorithm: "sha256", CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: plaintext}
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{ScopeKind: testidempotency.IdempotencyScopeProject,
		ScopeID: project.Record.ID, Method: http.MethodPatch, Route: "/secrets/{id}", Key: "secret-edit-key-0001"}
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetSecret, ID: secretID,
	}
	result, err := repository.ReplaceSecretValueIdempotent(t.Context(), testsecrets.ProjectOwner(project),
		current, replacement, value, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("edit outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	updated, err := repository.GetSecret(t.Context(), secretID)
	if err != nil || updated.Record.Secret.Encryption != "none" ||
		updated.Record.Secret.ID != current.Record.Secret.ID {
		t.Fatalf("updated Secret = %#v, %v", updated, err)
	}
	updatedValue, err := repository.GetSecretValue(t.Context(), updated)
	if err != nil || updatedValue.Cipher != "none" || string(updatedValue.Ciphertext) != "rotated-value" {
		t.Fatalf("updated value = %#v, %v", updatedValue, err)
	}
	clear(updatedValue.Ciphertext)
	priorValue, err := repository.GetSecretValue(t.Context(), current)
	if err != nil || priorValue.Cipher != "age-x25519" || string(priorValue.Ciphertext) != "old-ciphertext" {
		t.Fatalf("prior fixed-revision value = %#v, %v", priorValue, err)
	}
	clear(priorValue.Ciphertext)
	replay, err := repository.ReplaceSecretValueIdempotent(t.Context(), testsecrets.ProjectOwner(project),
		current, replacement, value, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, existing, conflict, err := replay.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("replay outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
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
	record, err := testsecrets.NewPlatformRecord(id, "API_TOKEN", core.SecretKindEnvVar, "", now)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: "/secrets", Key: "secret-create-key-0002",
	}
	marker.Response.Status = http.StatusCreated
	value := testSecretEncryptedValue(id, "encrypted-platform-value")
	result, err := repository.CreateSecretIdempotent(
		context.Background(), testsecrets.PlatformOwner(), record, value, marker,
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
		context.Background(), testsecrets.PlatformOwner(), record, value, marker,
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
