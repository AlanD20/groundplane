package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: after failed removal makes a Secret visible again, a Script may
// reserve it. Retrying the old deletion must not reacquire authority over it.
func TestSecretDeletionRetryCannotReacquireAfterScriptPreparation(t *testing.T) {
	ctx := context.Background()
	store, project := secretDeletionTestStore(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)
	secretID := ids.New(ids.KindSecret)
	record, err := testsecrets.NewProjectRecord(secretID, project.Record.ID, "TOKEN", core.SecretKindEnvVar, "", at)
	if err != nil {
		t.Fatal(err)
	}
	value := testSecretEncryptedValue(secretID, "opaque-ciphertext")
	current, err := secrets.CreateSecret(ctx, testsecrets.ProjectOwner(project), record, value)
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone := secretDeletionTestTask(t, current, project, at.Add(time.Second), 170)
	result, err := secrets.BeginSecretDeletionWithTask(
		ctx, testsecrets.ProjectOwner(project), current,
		tombstone,
		task,
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("initial deletion = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, at.Add(2*time.Second)); err != nil || !found {
		t.Fatalf("initial removal claim = %t, %v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusFailed, at.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.New(ids.KindOperation)
	members := []testscriptsourceevidence.ScriptSourcePreparationMember{{
		Reference: testscriptsourcereference.Reference{
			OperationID: operationID, ScriptExecutionID: ids.NewULID(), Source: testscriptsourcereference.SourceIdentity{Kind: testscriptsourcereference.SourceSecretValue, SecretID: secretID, ValueGenerationID: secretID},
			SourceOwnerID: project.Record.ID, SourceModRevision: current.Revision, SourceDigest: value.CiphertextSHA256,
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: testsecrets.ValueKey(secretID)},
		},
	}}
	if _, err := authority.Prepare(ctx, operationID, members); err != nil {
		t.Fatal(err)
	}
	retryAt := at.Add(4 * time.Second)
	retryID := ids.New(ids.KindTask)
	retryMarker := pendingRetryMarker(task, retryID, retryAt, "secret-script-protected-retry")
	before := store.revision
	if _, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker); !isKind(
		err,
		errs.KindResourceInUse,
	) ||
		store.revision != before {
		t.Fatalf("Secret retry stole prepared Script source: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, members); err != nil {
		t.Fatal(err)
	}
	result, err = tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("Secret retry remained blocked after reference release: %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, retryAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("retry removal claim = %t, %v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, testtaskjournal.TaskStatusCompleted, retryAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.GetSecret(ctx, secretID); !isKind(err, errs.KindSecretNotFound) {
		t.Fatalf("Secret remained after unblocked removal: %v", err)
	}
}
