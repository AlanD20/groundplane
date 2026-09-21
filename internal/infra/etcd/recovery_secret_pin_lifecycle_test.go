package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SEC-07/SVC-15: actual desired publication must atomically activate recovery
// pins with its Task; successful acknowledgement authorizes release, not a
// timer or preparation. Secret deletion must then remove its real stored value.
func TestRecoverySecretPinsFollowPublishedTaskSuccess(t *testing.T) {
	ctx := context.Background()
	store, project, secret, task := publishRecoverySecretTask(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	root := store.valueAt(tasksecretpins.RootKey(task.OperationID), store.revision)
	published := store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision)
	if task.Configuration == nil || task.Configuration.SecretPins == nil || root == nil ||
		published == nil || root.ModRevision != published.ModRevision {
		t.Fatal("Task and exact pin binding were not published atomically")
	}
	assertRecoverySecretDeletionBlocked(t, secrets, project, secret)
	terminal := acknowledgeRecoverySecretTask(t, tasks, task, testtaskjournal.TaskStatusCompleted)
	root = store.valueAt(tasksecretpins.RootKey(task.OperationID), store.revision)
	if root == nil || root.ModRevision != terminal.Revision {
		t.Fatal("successful Task did not atomically authorize pin release")
	}
	assertRecoverySecretDeletionBlocked(t, secrets, project, secret)
	// Simulate a Controller restart between terminal commit and background cleanup.
	if err := RecoverTaskSecretPinSources(ctx, store); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), secret); err != nil {
		t.Fatalf("released Secret deletion = %v", err)
	}
	if store.valueAt(testsecrets.RecordKey(secret.Record.Secret.ID), store.revision) != nil ||
		store.valueAt(testsecrets.ValueKey(secret.Record.Secret.ID), store.revision) != nil {
		t.Fatal("successful deletion retained Secret metadata or ciphertext")
	}
	if stored, err := tasks.GetTask(ctx, task.ID); err != nil ||
		stored.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("source release changed Task history: %v", err)
	}
}

// SEC-07: a failed attempt retains exact values through restart and an actual
// Retry. A successful Retry releases the operation's original pin set, without
// changing the failed attempt or creating a new copy of its Secret value.
func TestRecoverySecretPinsSurviveFailedTaskAndRetry(t *testing.T) {
	ctx := context.Background()
	store, project, secret, task := publishRecoverySecretTask(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	failed := acknowledgeRecoverySecretTask(t, tasks, task, testtaskjournal.TaskStatusFailed)
	if err := RecoverTaskSecretPinSources(ctx, store); err != nil {
		t.Fatal(err)
	}
	if progressed, err := tasks.ResumeTaskSourceReleases(ctx); err != nil || progressed {
		t.Fatalf("failed Task authorized source release: %t/%v", progressed, err)
	}
	assertRecoverySecretDeletionBlocked(t, secrets, project, secret)
	retryAt := task.CreatedAt.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 802)
	marker := pendingRetryMarker(failed.Record, retryID, retryAt, "pinned-recovery-retry-0001")
	result, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, marker)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Retry publication = %v/%v/%v", outcome, conflict, err)
	}
	retry, err := tasks.GetTask(ctx, retryID)
	if err != nil || retry.Record.Configuration == nil || retry.Record.Configuration.SecretPins == nil ||
		*retry.Record.Configuration.SecretPins != *task.Configuration.SecretPins {
		t.Fatalf("Retry lost original pin authority: %v", err)
	}
	acknowledgeRecoverySecretTask(t, tasks, retry.Record, testtaskjournal.TaskStatusCompleted)
	if progressed, err := tasks.ResumeTaskSourceReleases(ctx); err != nil || !progressed {
		t.Fatalf("successful Retry release = %t/%v", progressed, err)
	}
	if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), secret); err != nil {
		t.Fatal(err)
	}
	if original, err := tasks.GetTask(ctx, task.ID); err != nil ||
		original.Record.Status != testtaskjournal.TaskStatusFailed ||
		original.Revision != failed.Revision {
		t.Fatalf("Retry rewrote failed Task history: %v", err)
	}
}

// SEC-07: operation ownership outlives an old attempt's retention window.
// Expiry cannot release a later failed Retry or win an ABA race in which that
// Retry both starts and finishes before the old expiry transaction commits.
func TestRecoverySecretPinsExpiryWaitsForLatestAttempt(t *testing.T) {
	ctx := context.Background()
	store, project, secret, task := publishRecoverySecretTask(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	failed := acknowledgeRecoverySecretTask(t, tasks, task, testtaskjournal.TaskStatusFailed)
	pins, err := tasksecretpins.NewEtcdRepository(store, project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldRoot, found, err := pins.LoadActive(ctx, task.OperationID)
	if err != nil || !found {
		t.Fatal("initial active pin root missing")
	}
	staleExpiry, err := tasksecretpins.BeginRelease(oldRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer staleExpiry.Clear()
	retryAt := task.CreatedAt.Add(time.Hour)
	retryID := ids.NewAt(ids.KindTask, retryAt, 803)
	marker := pendingRetryMarker(failed.Record, retryID, retryAt, "pin-expiry-retry-0001")
	result, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, marker)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Retry = %v/%v/%v", outcome, conflict, err)
	}
	retry, err := tasks.GetTask(ctx, retryID)
	if err != nil {
		t.Fatal(err)
	}
	latest := acknowledgeRecoverySecretTask(t, tasks, retry.Record, testtaskjournal.TaskStatusFailed)
	conditions, writes, err := tasksecretpins.EtcdFragment(staleExpiry)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := store.Transact(ctx, conditions, writes)
	if err != nil || commit.Succeeded {
		t.Fatalf("expiry won Retry ABA race: %#v/%v", commit, err)
	}
	if count, err := tasks.PruneExpiredTasks(ctx, failed.Record.RetainUntil.Add(time.Nanosecond)); err != nil ||
		count != 0 {
		t.Fatalf("early expiry = %d/%v", count, err)
	}
	assertRecoverySecretDeletionBlocked(t, secrets, project, secret)
	if _, err := tasks.PruneExpiredTasks(ctx, latest.Record.RetainUntil.Add(time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), secret); err != nil {
		t.Fatal(err)
	}
	lateAt := latest.Record.RetainUntil.Add(time.Second)
	lateID := ids.NewAt(ids.KindTask, lateAt, 804)
	lateMarker := pendingRetryMarker(latest.Record, lateID, lateAt, "expired-pin-retry-0001")
	if _, err := tasks.RetryTask(ctx, retryID, lateID, testtaskjournal.TaskActorOperator, lateMarker); !isKind(
		err,
		errs.KindTaskNotRetryable,
	) {
		t.Fatalf("released source allowed Retry: %v", err)
	}
}

// SEC-07: rejecting publication or restarting with unpublished preparation
// must not permanently prevent deletion. The real adapter must reject a
// foreign Project and changed ciphertext rather than pinning a latest value.
func TestRecoverySecretPreparationRejectsWrongSourcesAndReclaimsUnpublishedPins(t *testing.T) {
	ctx := context.Background()
	store, project, secrets, secret, encrypted := secretRecoveryPinFixture(t)
	task := configurationTaskFixture()
	task.Owner.ProjectID = project.Record.ID
	task.Owner.TenantID = project.Record.TenantID
	file := task.Materializations[3]
	file.Source.GeneratedEnvironment.Values = []testtaskmaterialization.GeneratedEnvironmentEntryReference{
		{Name: "TOKEN",
			Secret: &testtaskmaterialization.SecretValueReference{SecretID: secret.Record.Secret.ID,
				Revision: secret.Revision, CiphertextSHA256: encrypted.CiphertextSHA256}},
	}
	task.Materializations = []testtaskmaterialization.Record{file}
	prepared, err := prepareRuntimeConfigurationTask(ctx, store, task, store.revision)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := tasksecretpins.NewEtcdRepository(store, ids.New(ids.KindProject))
	if err != nil {
		t.Fatal(err)
	}
	pin := tasksecretpinrecord.Record{OperationID: ids.New(ids.KindOperation), SecretID: secret.Record.Secret.ID,
		MetadataRevision: secret.Revision, CiphertextSHA256: encrypted.CiphertextSHA256}
	if _, err := foreign.Prepare(ctx, pin.OperationID, ids.New(ids.KindTask), []tasksecretpinrecord.Record{pin}); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("foreign Project pin = %v", err)
	}
	owned, err := tasksecretpins.NewEtcdRepository(store, project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	pin.OperationID = ids.New(ids.KindOperation)
	pin.CiphertextSHA256 = strings.Repeat("0", 64)
	if _, err := owned.Prepare(ctx, pin.OperationID, ids.New(ids.KindTask), []tasksecretpinrecord.Record{pin}); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("changed ciphertext pin = %v", err)
	}
	prepared, pins, err := prepareRecoverySecretPins(ctx, store, prepared)
	if err != nil || pins.IsZero() {
		t.Fatalf("prepare = %v", err)
	}
	assertRecoverySecretDeletionBlocked(t, secrets, project, secret)
	if err := RecoverTaskSecretPinSources(ctx, store); err != nil {
		t.Fatal(err)
	}
	activation, err := recoverySecretPinActivation(pins)
	if err != nil {
		t.Fatal(err)
	}
	defer clearTaskMaterializationProjectionChange(activation)
	value, err := EncodeTaskRecord(prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	activation.mutations = append(
		activation.mutations,
		testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskStorageKey(task.ID),
			Value: value,
		},
	)
	commit, err := store.Transact(ctx, activation.conditions, activation.mutations)
	if err != nil || commit.Succeeded || store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision) != nil {
		t.Fatalf("abandoned preparation published a Task: %#v/%v", commit, err)
	}
	if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), secret); err != nil {
		t.Fatal(err)
	}
}

func publishRecoverySecretTask(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testsecrets.Record], TaskRecord) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 550)
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, err := testsecrets.NewProjectRecord(
		ids.New(ids.KindSecret),
		project.Record.ID,
		"RECOVERY_TOKEN",
		core.SecretKindEnvVar,
		"",
		task.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := testSecretEncryptedValue(record.Secret.ID, "opaque-lifecycle-ciphertext")
	secret, err := secrets.CreateSecret(ctx, testsecrets.ProjectOwner(project), record, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	file := taskWithMaterializationReferences().Materializations[3]
	file.StepID, file.EnvironmentID = task.Steps[0].ID, environment.Record.ID
	file.ServiceID, file.ServiceName = projection.DesiredServices[0].Desired.ID, "api"
	file.Destination = "secrets/.env." + environment.Record.ID + ".api"
	file.Source.GeneratedEnvironment.Values = []testtaskmaterialization.GeneratedEnvironmentEntryReference{
		{Name: "TOKEN",
			Secret: &testtaskmaterialization.SecretValueReference{SecretID: record.Secret.ID,
				Revision: secret.Revision, CiphertextSHA256: encrypted.CiphertextSHA256}},
	}
	task.Materializations = []testtaskmaterialization.Record{file}
	result := publishEnvironmentBlueprintTestRevision(
		t,
		hierarchy,
		project,
		environment,
		0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		projection,
		environmentBlueprintTestZoneChanges(t, hierarchy, projection),
		environmentBlueprintTestServiceChanges(t, hierarchy, projection),
		environmentBlueprintTestRouteChanges(
			t,
			hierarchy,
			projection,
		),
		testcomponentplanning.ComponentTaskPreparation{},
		task,
		environmentBlueprintTestMarker(task, environment.Record.ID),
	)
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Task publication = %v/%v/%v", outcome, conflict, err)
	}
	stored := store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision)
	if stored == nil {
		t.Fatal("published Task is missing")
	}
	task, err = DecodeTaskRecord(stored.Value)
	if err != nil {
		t.Fatal(err)
	}
	return store, project, secret, task
}

func acknowledgeRecoverySecretTask(
	t *testing.T,
	tasks *TaskRepository,
	task TaskRecord,
	status testtaskjournal.TaskStatus,
) testkeyvalue.Versioned[TaskRecord] {
	t.Helper()
	ctx := context.Background()
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 801)
	assignment, found, err := tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found || assignment.Task.Record.ID != task.ID {
		t.Fatalf("claim = %t/%v", found, err)
	}
	result := completedComposeTaskResult()
	if status != testtaskjournal.TaskStatusCompleted {
		result.ExitCode = 1
	}
	terminal, err := tasks.AcknowledgeTask(ctx, agentID, 1, task.ID,
		taskAssignmentIDForTest(t, tasks, task.ID), status, result, task.CreatedAt.Add(2*time.Second))
	if err != nil {
		t.Fatalf("acknowledge %s = %v", status, err)
	}
	return terminal
}

func assertRecoverySecretDeletionBlocked(
	t *testing.T,
	secrets *SecretRepository,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	secret testkeyvalue.Versioned[testsecrets.Record],
) {
	t.Helper()
	if _, err := secrets.DeleteSecret(context.Background(), testsecrets.ProjectOwner(project), secret); !isKind(
		err,
		errs.KindResourceInUse,
	) {
		t.Fatalf("recoverable Task did not protect Secret deletion: %v", err)
	}
}
