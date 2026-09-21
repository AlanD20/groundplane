package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testsecretmutations "github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (fixture *ExecutedArtifactFixture) CreateManualJourneySecret(t *testing.T, ciphertext []byte) string {
	t.Helper()
	ctx := context.Background()
	project, err := fixture.Hierarchy.GetProject(ctx, fixture.Project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := newSecretRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	record, err := testsecrets.NewProjectRecord(ids.New(ids.KindSecret), project.Record.ID,
		"manual-journey", core.SecretKindFile, "secrets/manual-value", fixture.Environment.Record.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(ciphertext)
	if _, err := repository.CreateSecret(ctx, testsecrets.ProjectOwner(project), record, testsecrets.EncryptedValue{
		SecretID: record.Secret.ID, EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}); err != nil {
		t.Fatal(err)
	}
	return record.Secret.ID
}

func (fixture *ExecutedArtifactFixture) CheckManualJourneySecretDeletion(t *testing.T, secretID string, active bool) {
	t.Helper()
	ctx := context.Background()
	// The base hierarchy fake only compares exact keys. Reuse the existing
	// prefix-aware transaction fake with its injection disabled for this proof.
	repository, err := newSecretRepository(&connectorReferenceRaceStore{
		memoryHierarchyStore: fixture.store.memoryHierarchyStore, injected: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.GetSecret(ctx, secretID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := fixture.Hierarchy.GetProject(ctx, fixture.Project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	at := fixture.Environment.Record.CreatedAt.Add(5 * time.Hour)
	task, marker, tombstone := secretDeletionTestTask(t, current, project, at, 140)
	before := fixture.store.revision
	result, err := repository.BeginSecretDeletionWithTask(
		ctx, testsecrets.ProjectOwner(project), current,
		tombstone,
		task,
		marker,
	)
	if active {
		assertManualJourneySecretSource(t, fixture.store, current)
		if err != nil || !isKind(result.conflict, errs.KindResourceInUse) || fixture.store.revision != before {
			t.Fatalf("active manual Secret deletion should reject without writes: %v / %v", err, result.conflict)
		}
		return
	}
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("released manual Secret deletion = %v", err)
	}
	claim, found, err := fixture.Tasks.ClaimNextControllerTask(ctx, at.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("Secret removal claim = %t, %v", found, err)
	}
	if _, err := fixture.Tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.GetSecret(ctx, secretID); !isKind(err, errs.KindSecretNotFound) {
		t.Fatalf("removed Secret remains readable: %v", err)
	}
}

func assertManualJourneySecretSource(
	t *testing.T,
	store hierarchyStore,
	secret testkeyvalue.Versioned[testsecrets.Record],
) {
	t.Helper()
	ctx := context.Background()
	conditions := testsecretmutations.SecretScriptAbsenceConditions(secret.Record.Secret.ID)
	members, err := store.Range(ctx, testkeyvalue.RangeRequest{Prefix: conditions[1].Key, Limit: 2})
	if err != nil || members == nil || len(members.Values) != 1 || members.More {
		t.Fatal("manual Secret source membership coverage is inconsistent")
	}
	reference, err := testrecordcodec.Decode[testscriptsourcereference.Reference](
		members.Values[0].Value,
		"script-source-reference",
	)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(ctx, testsecrets.ValueKey(secret.Record.Secret.ID))
	if err != nil || stored == nil || stored.Entry == nil {
		t.Fatal("manual Secret ciphertext source disappeared")
	}
	value, err := testsecrets.DecodeEncryptedValue(stored.Entry.Value)
	defer clear(value.Ciphertext)
	wantSource := testscriptsourcereference.SourceIdentity{
		Kind: testscriptsourcereference.SourceSecretValue, SecretID: secret.Record.Secret.ID,
		ValueGenerationID: secret.Record.Secret.ID,
	}
	if err != nil || reference.Source != wantSource ||
		reference.SourceDigest != value.CiphertextSHA256 || reference.SourceModRevision != stored.Entry.ModRevision ||
		reference.SourceOwnerID != secret.Record.Secret.ProjectID {
		t.Fatal("manual Secret membership substituted its ciphertext generation or Project owner")
	}
}
