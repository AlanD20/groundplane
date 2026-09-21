package etcd

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: dispatch must hide a Project Secret at the same revision while
// preserving platform fallback, and successful Task acknowledgement must
// remove metadata, indexes, ciphertext, and the fence atomically.
func TestSecretDeletionTaskFencesAndFinalizesTheCompleteSecret(t *testing.T) {
	// Rationale: secret removal must publish its Task with the exact current Project and Tenant ancestry.
	t.Parallel()
	ctx := context.Background()
	store, project := secretDeletionTestStore(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	platformID := ids.NewAt(ids.KindSecret, now, 1)
	platform, err := testsecrets.NewPlatformRecord(platformID, "TOKEN", core.SecretKindEnvVar, "", now)
	if err != nil {
		t.Fatalf("NewPlatformSecretRecord() error = %v", err)
	}
	if _, err := secrets.CreateSecret(
		ctx, testsecrets.PlatformOwner(), platform, testSecretEncryptedValue(platformID, "platform"),
	); err != nil {
		t.Fatalf("CreateSecret(platform) error = %v", err)
	}
	secretID := ids.NewAt(ids.KindSecret, now.Add(time.Second), 2)
	record, err := testsecrets.NewProjectRecord(
		secretID, project.Record.ID, "TOKEN", core.SecretKindEnvVar, "", now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	current, err := secrets.CreateSecret(
		ctx, testsecrets.ProjectOwner(project), record, testSecretEncryptedValue(secretID, "project"),
	)
	if err != nil {
		t.Fatalf("CreateSecret(project) error = %v", err)
	}
	task, marker, tombstone := secretDeletionTestTask(t, current, project, now.Add(2*time.Second), 10)
	result, err := secrets.BeginSecretDeletionWithTask(
		ctx, testsecrets.ProjectOwner(project), current, tombstone, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginSecretDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("deletion outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	assertSecretDeletionHidden(t, secrets, secretID)
	page, err := secrets.ListSecrets(
		ctx,
		core.SecretScopeProject,
		project.Record.ID,
		testkeyvalue.PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("ListSecrets(fenced) = %#v, %v", page, err)
	}
	resolved, err := secrets.ResolveSecret(ctx, project.Record.ID, "TOKEN")
	if err != nil || resolved.Record.Secret.ID != platformID {
		t.Fatalf("ResolveSecret(fenced override) = %#v, %v", resolved, err)
	}
	replayRepository, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	locator, found, err := replayRepository.ResolveReplayLocator(
		ctx,
		testidempotency.IdempotencyReplayTarget{Kind: testidempotency.IdempotencyReplayTargetSecret, ID: secretID},
		http.MethodDelete,
		"/secrets/{id}",
		marker.Locator.Key,
	)
	if err != nil || !found || locator != marker.Locator {
		t.Fatalf("ResolveReplayLocator() = %#v/%v/%v", locator, found, err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(3*time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claim, found, err)
	}
	terminal, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, now.Add(4*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	for _, key := range []string{testsecrets.RecordKey(secretID), testsecrets.SecretOwnerKey(record.Secret), testsecrets.SecretScopedKey(record.Secret), testsecrets.ValueKey(secretID), testdeletions.TombstoneKey(string(testdeletions.DeletionTargetSecret), secretID)} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, now.Add(4*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
}

// Rationale: every non-success terminal path must restore visibility, while a
// retry must atomically reacquire the fence before it can be queued again.
func TestSecretDeletionFailureTimeoutAbortAndRetryRestoreVisibility(t *testing.T) {
	// Rationale: secret retry paths must preserve durable owner ancestry while restoring the encrypted record lifecycle.
	t.Parallel()
	ctx := context.Background()
	store, project := secretDeletionTestStore(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, now, 20)
	record, err := testsecrets.NewProjectRecord(
		secretID, project.Record.ID, "PRIVATE_KEY", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	current, err := secrets.CreateSecret(
		ctx, testsecrets.ProjectOwner(project), record, testSecretEncryptedValue(secretID, "ciphertext"),
	)
	if err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	task, marker, tombstone := secretDeletionTestTask(t, current, project, now.Add(time.Second), 30)
	if _, err := secrets.BeginSecretDeletionWithTask(
		ctx, testsecrets.ProjectOwner(project), current, tombstone, task, marker,
	); err != nil {
		t.Fatalf("BeginSecretDeletionWithTask() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(2*time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx,
		task.ID,
		testtaskjournal.TaskStatusFailed,
		now.Add(3*time.Second),
	)
	if err != nil || failed.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(failed) = %#v/%v", failed, err)
	}
	assertSecretDeletionVisible(t, secrets, secretID)

	retryID := ids.NewAt(ids.KindTask, now.Add(4*time.Second), 31)
	retryMarker := pendingRetryMarker(task, retryID, now.Add(4*time.Second), "secret-retry-key-0001")
	if _, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	assertSecretDeletionHidden(t, secrets, secretID)
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(5*time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextControllerTask(retry) = %#v/%v/%v", claim, found, err)
	}
	expired, err := tasks.ExpireTimedOutTasks(ctx, claim.Assignment.Record.Deadline.Add(time.Second))
	if err != nil || expired != 1 {
		t.Fatalf("ExpireTimedOutTasks() = %d/%v", expired, err)
	}
	assertSecretDeletionVisible(t, secrets, secretID)

	timedOut, err := tasks.GetTask(ctx, retryID)
	if err != nil || timedOut.Record.Status != testtaskjournal.TaskStatusTimedOut {
		t.Fatalf("GetTask(timed out) = %#v/%v", timedOut, err)
	}
	abortID := ids.NewAt(ids.KindTask, now.Add(6*time.Second), 32)
	abortMarker := pendingRetryMarker(
		timedOut.Record, abortID, now.Add(6*time.Second), "secret-retry-key-0002",
	)
	if _, err := tasks.RetryTask(ctx, retryID, abortID, testtaskjournal.TaskActorOperator, abortMarker); err != nil {
		t.Fatalf("RetryTask(after timeout) error = %v", err)
	}
	assertSecretDeletionHidden(t, secrets, secretID)
	if _, err := tasks.AbortPendingTask(ctx, abortID, now.Add(7*time.Second)); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	assertSecretDeletionVisible(t, secrets, secretID)
}

func secretDeletionTestStore(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	now := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	project := testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2), TenantID: ids.NewAt(ids.KindTenant, now, 1),
		Slug: "secret-delete", Name: "Secret Delete", Kind: testhierarchy.ProjectKindTenant,
	}
	tenant := testhierarchy.TenantRecord{ID: project.TenantID, Slug: "secret-tenant", Name: "Secret Tenant"}
	tenantValue, err := testhierarchy.EncodeTenant(tenant)
	if err != nil {
		t.Fatalf("encodeTenant() error = %v", err)
	}
	defer clear(tenantValue)
	value, err := testhierarchy.EncodeProject(project)
	if err != nil {
		t.Fatalf("encodeProject() error = %v", err)
	}
	store := newMemoryHierarchyStore()
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.TenantKey(tenant.ID), Value: tenantValue},
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.ProjectKey(project.ID), Value: value},
	})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Project = %#v/%v", result, err)
	}
	return store, testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: project, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func secretDeletionTestTask(
	t *testing.T,
	current testkeyvalue.Versioned[testsecrets.Record],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	createdAt time.Time,
	entropy int64,
) (TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord) {
	task := validTaskRecord(createdAt)
	task.Owner = mustProjectTaskOwner(t, project.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, entropy)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, entropy+1)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, entropy+2)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskRemove
	task.Target = current.Record.Secret.ID
	task.Params = map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceSecret}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "secret-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeProject, ScopeID: project.Record.ID,
		Method: http.MethodDelete, Route: "/secrets/{id}", Key: task.IdempotencyKey,
	}
	target := testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetSecret,
		ID:   task.Target,
	}
	marker.ReplayTarget = &target
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetSecret, TargetID: task.Target,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: testdeletions.DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone
}

func assertSecretDeletionHidden(t *testing.T, repository *SecretRepository, secretID string) {
	t.Helper()
	_, err := repository.GetSecret(context.Background(), secretID)
	if !errors.Is(err, errs.New(errs.KindSecretNotFound, "")) {
		t.Fatalf("GetSecret(fenced) error = %v", err)
	}
}

func assertSecretDeletionVisible(t *testing.T, repository *SecretRepository, secretID string) {
	t.Helper()
	current, err := repository.GetSecret(context.Background(), secretID)
	if err != nil || current.Record.Secret.ID != secretID {
		t.Fatalf("GetSecret(visible) = %#v/%v", current, err)
	}
}
