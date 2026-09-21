package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
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
	platform, err := testsecrets.NewPlatformRecord(
		platformID, "API_TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	platformValue := testSecretEncryptedValue(platformID, "platform-ciphertext")
	if _, err := repository.CreateSecret(
		context.Background(), testsecrets.PlatformOwner(), platform, platformValue,
	); err != nil {
		t.Fatalf("CreateSecret(platform) error = %v", err)
	}

	projectID := ids.NewAt(ids.KindSecret, now.Add(time.Second), 11)
	projectSecret, err := testsecrets.NewProjectRecord(
		projectID, project.Record.ID, "API_TOKEN", core.SecretKindEnvVar, "", now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	projectValue := testSecretEncryptedValue(projectID, "project-ciphertext")
	created, err := repository.CreateSecret(
		context.Background(), testsecrets.ProjectOwner(project), projectSecret, projectValue,
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
		context.Background(), core.SecretScopeProject, project.Record.ID, testkeyvalue.PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Secret.ID != projectID {
		t.Fatalf("ListSecrets() = %#v, %v", page, err)
	}
	if _, err := repository.DeleteSecret(
		context.Background(), testsecrets.ProjectOwner(project), created,
	); err != nil {
		t.Fatalf("DeleteSecret() error = %v", err)
	}
	resolved, err = repository.ResolveSecret(context.Background(), project.Record.ID, "API_TOKEN")
	if err != nil || resolved.Record.Secret.ID != platformID {
		t.Fatalf("ResolveSecret(platform fallback) = %#v, %v", resolved, err)
	}
	// A Script prepared before removal must still resolve its exact snapshot,
	// not silently substitute the now-visible platform fallback.
	for _, reference := range []string{"API_TOKEN", projectID} {
		pinned, err := repository.ResolveSecretAtRevision(
			context.Background(), project.Record.ID, reference, created.ReadRevision,
		)
		if err != nil || pinned.Record.Secret.ID != projectID || pinned.ReadRevision != created.ReadRevision {
			t.Fatalf("pinned Secret resolution = %#v, %v", pinned, err)
		}
	}
	deletedValue, err := store.Get(context.Background(), testsecrets.ValueKey(projectID))
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
	record, err := testsecrets.NewProjectRecord(
		id, project.Record.ID, "PRIVATE_KEY", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	if _, err := repository.CreateSecret(
		context.Background(), testsecrets.ProjectOwner(project), record,
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
	envRecord, err := testsecrets.NewProjectRecord(
		ids.NewAt(ids.KindSecret, now, 31), projectID, "TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil || envRecord.Secret.Ref != "secrets/.env."+projectID {
		t.Fatalf("NewProjectSecretRecord(env) = %#v, %v", envRecord, err)
	}
	_, err = testsecrets.NewPlatformRecord(
		ids.NewAt(ids.KindSecret, now, 32), "certificate", core.SecretKindFile, "../ca.pem", now,
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("NewPlatformSecretRecord(unsafe path) error = %v", err)
	}
}

func testSecretRepositoryProject(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	project := testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2), TenantID: ids.NewAt(ids.KindTenant, now, 1),
		Slug: "secret-project", Name: "Secret Project", Kind: testhierarchy.ProjectKindTenant,
	}
	store := newMemoryHierarchyStore()
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.ProjectKey(project.ID), Value: []byte("parent")},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed project = %#v, %v", result, err)
	}
	return store, testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: project, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func testSecretEncryptedValue(secretID string, ciphertext string) testsecrets.EncryptedValue {
	digest := sha256.Sum256([]byte(ciphertext))
	return testsecrets.EncryptedValue{
		SecretID: secretID, EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: []byte(ciphertext),
	}
}
