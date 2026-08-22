package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestSecretRepositoryResolvesProjectBeforePlatformAndDeletesAtomically(t *testing.T) {
	// Rationale: a project override and platform fallback with the same key
	// must resolve deterministically without exposing or orphaning ciphertext.
	store, project := testSecretRepositoryProject(t)
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	platformID := ids.NewAt(ids.KindSecret, now, 10)
	platform, err := NewPlatformSecretRecord(
		platformID, "API_TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	platformValue := testSecretEncryptedValue(platformID, "platform-ciphertext")
	if _, err := repository.CreateSecret(
		context.Background(), PlatformSecretOwner(), platform, platformValue,
	); err != nil {
		t.Fatalf("CreateSecret(platform) error = %v", err)
	}

	projectID := ids.NewAt(ids.KindSecret, now.Add(time.Second), 11)
	projectSecret, err := NewProjectSecretRecord(
		projectID, project.Record.ID, "API_TOKEN", core.SecretKindEnvVar, "", now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	projectValue := testSecretEncryptedValue(projectID, "project-ciphertext")
	created, err := repository.CreateSecret(
		context.Background(), ProjectSecretOwner(project), projectSecret, projectValue,
	)
	if err != nil {
		t.Fatalf("CreateSecret(project) error = %v", err)
	}
	resolved, err := repository.ResolveSecret(context.Background(), project.Record.ID, "API_TOKEN")
	if err != nil || resolved.Record.Secret.ID != projectID {
		t.Fatalf("ResolveSecret(project override) = %#v, %v", resolved, err)
	}
	value, err := repository.GetSecretValue(context.Background(), resolved)
	if err != nil || string(value.Ciphertext) != "project-ciphertext" {
		t.Fatalf("GetSecretValue() = %#v, %v", value, err)
	}
	clear(value.Ciphertext)
	page, err := repository.ListSecrets(
		context.Background(), core.SecretScopeProject, project.Record.ID, PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Secret.ID != projectID {
		t.Fatalf("ListSecrets() = %#v, %v", page, err)
	}
	if _, err := repository.DeleteSecret(
		context.Background(), ProjectSecretOwner(project), created,
	); err != nil {
		t.Fatalf("DeleteSecret() error = %v", err)
	}
	resolved, err = repository.ResolveSecret(context.Background(), project.Record.ID, "API_TOKEN")
	if err != nil || resolved.Record.Secret.ID != platformID {
		t.Fatalf("ResolveSecret(platform fallback) = %#v, %v", resolved, err)
	}
	deletedValue, err := store.Get(context.Background(), secretValueKey(projectID))
	if err != nil || deletedValue.Entry != nil {
		t.Fatalf("deleted ciphertext = %#v, %v", deletedValue, err)
	}
}

func TestSecretRepositoryRejectsCrossProjectStableIDResolution(t *testing.T) {
	// Rationale: a stable id must not bypass project ownership even though
	// project key fallback also permits explicitly platform-owned records.
	store, project := testSecretRepositoryProject(t)
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC)
	id := ids.NewAt(ids.KindSecret, now, 20)
	record, err := NewProjectSecretRecord(
		id, project.Record.ID, "PRIVATE_KEY", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	if _, err := repository.CreateSecret(
		context.Background(), ProjectSecretOwner(project), record,
		testSecretEncryptedValue(id, "ciphertext"),
	); err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	otherProjectID := ids.NewAt(ids.KindProject, now, 21)
	_, err = repository.ResolveSecret(context.Background(), otherProjectID, id)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindSecretNotFound {
		t.Fatalf("ResolveSecret(cross-project id) error = %v", err)
	}
}

func TestSecretRecordDerivesCanonicalReferencesAndRejectsUnsafeFilePaths(t *testing.T) {
	// Rationale: env-file names derive from stable owner ids and inherited file
	// Secrets must never escape an Environment volume during materialization.
	now := time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, now, 30)
	envRecord, err := NewProjectSecretRecord(
		ids.NewAt(ids.KindSecret, now, 31), projectID, "TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil || envRecord.Secret.Ref != "secrets/.env."+projectID {
		t.Fatalf("NewProjectSecretRecord(env) = %#v, %v", envRecord, err)
	}
	_, err = NewPlatformSecretRecord(
		ids.NewAt(ids.KindSecret, now, 32), "certificate", core.SecretKindFile, "../ca.pem", now,
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("NewPlatformSecretRecord(unsafe path) error = %v", err)
	}
}

func testSecretRepositoryProject(t *testing.T) (*memoryHierarchyStore, Versioned[ProjectRecord]) {
	t.Helper()
	now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	project := ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2), TenantID: ids.NewAt(ids.KindTenant, now, 1),
		Slug: "secret-project", Name: "Secret Project", Kind: ProjectKindTenant,
	}
	store := newMemoryHierarchyStore()
	result, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: projectKey(project.ID), Value: []byte("parent")},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed project = %#v, %v", result, err)
	}
	return store, Versioned[ProjectRecord]{
		Record: project, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func testSecretEncryptedValue(secretID string, ciphertext string) SecretEncryptedValue {
	digest := sha256.Sum256([]byte(ciphertext))
	return SecretEncryptedValue{
		SecretID: secretID, EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: []byte(ciphertext),
	}
}
