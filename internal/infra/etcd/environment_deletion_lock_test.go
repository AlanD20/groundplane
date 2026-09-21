package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testenvironmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	http "net/http"
	strings "strings"
	testing "testing"
	time "time"
)

// Rationale: Environment deletion must lose before publishing host effects when
// another persistence operation already owns the canonical lock.
func TestEnvironmentDeletionRejectsHeldOperationLock(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	other := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8010),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8011),
		Kind:          testbackupruntime.BackupOperationBackup,
		CreatedAt:     fixture.now,
		UpdatedAt:     fixture.now,
	}
	fixture.putLock(t, other)
	result, err := fixture.begin()
	if err != nil {
		t.Fatalf("BeginEnvironmentDeletionWithTask(held lock) error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindResourceInUse) {
		t.Fatalf("BeginEnvironmentDeletionWithTask(held lock) = %#v, %v", result, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, testtaskjournal.TaskStorageKey(fixture.task.ID), false)
}

// Rationale: stale and corrupt mutation epochs must fail closed instead of
// publishing a deletion Task against persistence evidence from another epoch.
func TestEnvironmentDeletionRejectsStaleAndCorruptMutationEpoch(t *testing.T) {
	t.Parallel()
	t.Run("stale", func(t *testing.T) {
		fixture := newEnvironmentDeletionLockFixture(t)
		fixture.rewriteEpoch(t)
		result, err := fixture.begin()
		if err != nil {
			t.Fatalf("BeginEnvironmentDeletionWithTask(stale) error = %v", err)
		}
		_, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) {
			t.Fatalf("stale epoch classification = %#v/%v", result, classifyErr)
		}
	})

	t.Run("corrupt", func(t *testing.T) {
		fixture := newEnvironmentDeletionLockFixture(t)
		fixture.putRaw(
			t, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID), []byte("corrupt"),
		)
		current, err := fixture.hierarchy.GetEnvironment(
			context.Background(),
			fixture.environment.Record.ID,
		)
		if err != nil {
			t.Fatalf("GetEnvironment() error = %v", err)
		}
		fixture.environment = current
		result, err := fixture.begin()
		if !isKind(err, errs.KindInternal) {
			t.Fatalf("BeginEnvironmentDeletionWithTask(corrupt) = %#v, %v", result, err)
		}
	})
}

// Rationale: every destructive Blueprint batch must retain exact deletion lock
// ownership and advance the Environment mutation epoch in the same transaction.
func TestEnvironmentDeletionBlueprintBatchFencesOwnedLockAndAdvancesEpoch(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	revisionID := ids.NewAt(ids.KindTask, fixture.now, 8020)
	fixture.putRaw(
		t,
		testblueprints.EnvironmentBlueprintRevisionsPrefix(fixture.environment.Record.ID)+revisionID+"/manifest",
		[]byte("revision"),
	)
	other := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8021),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8022),
		Kind:          testbackupruntime.BackupOperationRestore,
		CreatedAt:     fixture.now,
		UpdatedAt:     fixture.now,
	}
	fixture.putLock(t, other)
	if _, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch(other owner) error = %v", err)
	}
	owned := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   fixture.task.OperationID,
		TaskID:        fixture.task.ID,
		Kind:          testbackupruntime.BackupOperationDeletion,
		CreatedAt:     fixture.task.CreatedAt,
		UpdatedAt:     fixture.task.CreatedAt,
	}
	fixture.putLock(t, owned)
	before := fixture.mustGet(t, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID))
	changed, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(2*time.Second),
	)
	if err != nil || !changed {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch() = %t, %v", changed, err)
	}
	after := fixture.mustGet(t, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID))
	if after.ModRevision <= before.ModRevision {
		t.Fatalf("mutation epoch revision = %d, want > %d", after.ModRevision, before.ModRevision)
	}
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		testblueprints.EnvironmentBlueprintRevisionsPrefix(fixture.environment.Record.ID)+revisionID+"/manifest",
		false,
	)
}

// Rationale: owned deletion work must fail closed when its tombstone is
// missing, belongs to another Task, or changes after the fixed-revision read.
func TestEnvironmentDeletionOwnedFenceValidatesExactTombstone(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *environmentDeletionLockFixture)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, fixture *environmentDeletionLockFixture) {
				result, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
					Type: testkeyvalue.MutationDelete,
					Key: testdeletions.TombstoneKey(
						string(testdeletions.DeletionTargetEnvironment),
						fixture.environment.Record.ID,
					),
				}})
				if err != nil || !result.Succeeded {
					t.Fatalf("delete tombstone = %#v, %v", result, err)
				}
			},
		},
		{
			name: "wrong task",
			mutate: func(t *testing.T, fixture *environmentDeletionLockFixture) {
				tombstone := fixture.tombstone
				tombstone.TaskID = ids.NewAt(ids.KindTask, fixture.now, 8050)
				value, err := testdeletions.EncodeDeletionTombstone(tombstone)
				if err != nil {
					t.Fatalf("encodeDeletionTombstone() error = %v", err)
				}
				defer clear(value)
				fixture.putRaw(
					t, testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID), value,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			test.mutate(t, fixture)
			_, err := testenvironmentfence.LoadOwned(
				context.Background(),
				fixture.store,
				fixture.environment.Record.ID,
				fixture.store.revision, testenvironmentfence.Owner{
					Kind:        testbackupruntime.BackupOperationDeletion,
					OperationID: fixture.task.OperationID,
					TaskID:      fixture.task.ID,
				},
			)
			if !isKind(err, errs.KindStateConflict) {
				t.Fatalf("loadOwnedEnvironmentMutationFence() error = %v", err)
			}
		})
	}

	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	owner := testenvironmentfence.Owner{
		Kind:        testbackupruntime.BackupOperationDeletion,
		OperationID: fixture.task.OperationID,
		TaskID:      fixture.task.ID,
	}
	evidence, err := testenvironmentfence.LoadOwned(
		context.Background(),
		fixture.store,
		fixture.environment.Record.ID,
		fixture.store.revision,
		owner,
	)
	if err != nil {
		t.Fatalf("loadOwnedEnvironmentMutationFence() error = %v", err)
	}
	tombstone := fixture.mustGet(
		t, testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	fixture.putRaw(t, tombstone.Key, tombstone.Value)
	epochMutation, err := evidence.EpochRewriteMutation()
	if err != nil {
		t.Fatalf("epochRewriteMutation() error = %v", err)
	}
	defer clear(epochMutation.Value)
	result, err := fixture.store.Transact(
		context.Background(), evidence.TransactionConditions(), []testkeyvalue.Mutation{epochMutation},
	)
	if err != nil || result.Succeeded {
		t.Fatalf("Transact(stale tombstone) = %#v, %v", result, err)
	}
	conflict := evidence.ClassifyConflict(result.FailureReads)
	testkeyvalue.ClearValues(result.FailureReads)
	if !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("classifyCAS(stale tombstone) error = %v", conflict)
	}
}

// Rationale: Environment deletion publication must make the immutable intent
// visible at the same revision as its Task, tombstone, and exact operation lock.
func TestEnvironmentDeletionPublishesIntentAtomically(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	stored, err := fixture.store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskStorageKey(fixture.task.ID),
				testdeletions.TombstoneKey(
					string(testdeletions.DeletionTargetEnvironment),
					fixture.environment.Record.ID,
				),
				testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID),
				testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID),
			},
		},
	)
	if err != nil || stored == nil || len(stored.Values) != 4 {
		t.Fatalf("Environment deletion publication = %#v, %v", stored, err)
	}
	for _, value := range stored.Values {
		if value == nil || value.ModRevision != stored.Values[0].ModRevision {
			t.Fatalf("Environment deletion publication revisions = %#v", stored.Values)
		}
	}
	intent, err := testdeletions.DecodeEnvironmentDeletionIntent(stored.Values[3].Value)
	if err != nil || intent.EnvironmentID != fixture.environment.Record.ID ||
		intent.OperationID != fixture.task.OperationID || intent.TaskID != fixture.task.ID ||
		intent.TargetRevision != fixture.environment.Revision ||
		intent.CleanupPhase != testdeletions.EnvironmentDeletionCleanupComplete {
		t.Fatalf("Environment deletion intent = %#v, %v", intent, err)
	}
}

// Rationale: Backup authority may appear after the caller reads the
// Environment but before deletion publication reads its transaction evidence;
// initial cleanup classification must use that later fixed revision.
func TestEnvironmentDeletionInitialCleanupUsesTransactionReadRevision(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.putRaw(
		t,
		testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID),
		[]byte("authority-created-after-environment-read"),
	)
	fixture.mustBegin(t)
	entry := fixture.mustGet(t, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := testdeletions.DecodeEnvironmentDeletionIntent(entry.Value)
	if err != nil || intent.CleanupPhase != testdeletions.EnvironmentDeletionCleanupEnumerating {
		t.Fatalf("inter-read Environment deletion intent = %#v, %v", intent, err)
	}
}

// Rationale: retry must replace attempt ownership on the retained tombstone
// lock and intent in the same revision that publishes the new Task, while
// preserving the operation identity, checkpoint, and cleanup phase.
func TestEnvironmentDeletionRetryTransfersFenceWithoutUnlockedInterval(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	ctx := context.Background()
	tombstoneEntry := fixture.mustGet(
		t, testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	tombstoneBefore, err := testdeletions.DecodeDeletionTombstone(tombstoneEntry.Value)
	if err != nil {
		t.Fatalf("decodeDeletionTombstone() error = %v", err)
	}
	tombstoneBefore.Phase = testdeletions.DeletionPhaseFinalizing
	tombstoneBefore.Checkpoint = testdeletions.DeletionCheckpoint{
		ResourceKind: "backup_policy", StableID: fixture.environment.Record.ID,
	}
	tombstoneBefore.UpdatedAt = fixture.now.Add(time.Nanosecond)
	tombstoneValue, err := testdeletions.EncodeDeletionTombstone(tombstoneBefore)
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	defer clear(tombstoneValue)
	fixture.putRaw(t, tombstoneEntry.Key, tombstoneValue)
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8060)
	if _, found, err := fixture.tasks.ClaimNextTask(
		ctx,
		agentID,
		1,
		fixture.now.Add(time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ExitCode: 1,
	}
	failed, err := fixture.tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID), testtaskjournal.TaskStatusFailed, result,
		fixture.now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(failed) error = %v", err)
	}
	fixture.mustGet(t, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID))

	retryAt := fixture.now.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 8061)
	marker := pendingRetryMarker(
		failed.Record,
		retryID,
		retryAt,
		"environment-delete-retry-key-0001",
	)
	retryResult, err := fixture.tasks.RetryTask(
		ctx, fixture.task.ID, retryID, testtaskjournal.TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatalf("RetryTask(Environment deletion) error = %v", err)
	}
	outcome, _, conflict, err := retryResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask(Environment deletion) = %#v, %v", retryResult, err)
	}
	stored, err := fixture.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskStorageKey(retryID),
				testdeletions.TombstoneKey(
					string(testdeletions.DeletionTargetEnvironment),
					fixture.environment.Record.ID,
				),
				testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID),
				testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID),
				testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID),
			},
		},
	)
	if err != nil || stored == nil || len(stored.Values) != 5 {
		t.Fatalf("Environment deletion retry state = %#v, %v", stored, err)
	}
	for _, value := range stored.Values {
		if value == nil || value.ModRevision != stored.Values[0].ModRevision {
			t.Fatalf("Environment deletion retry revisions = %#v", stored.Values)
		}
	}
	tombstone, err := testdeletions.DecodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TaskID != retryID || tombstone.Phase != testdeletions.DeletionPhaseFinalizing ||
		tombstone.Checkpoint != tombstoneBefore.Checkpoint {
		t.Fatalf("transferred Environment deletion tombstone = %#v, %v", tombstone, err)
	}
	lock, err := decodeEnvironmentOperationLock(stored.Values[2], fixture.environment.Record.ID)
	if err != nil || lock.TaskID != retryID || lock.OperationID != fixture.task.OperationID {
		t.Fatalf("transferred Environment deletion lock = %#v, %v", lock, err)
	}
	intent, err := testdeletions.DecodeEnvironmentDeletionIntent(stored.Values[4].Value)
	if err != nil || intent.TaskID != retryID ||
		intent.CleanupPhase != testdeletions.EnvironmentDeletionCleanupComplete {
		t.Fatalf("transferred Environment deletion intent = %#v, %v", intent, err)
	}
}

// Rationale: retry must compare exact old attempt ownership; a concurrent
// tombstone owner change must prevent both the new Task and fence transfer.
func TestEnvironmentDeletionRetryLosesOwnerRaceAtomically(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	ctx := context.Background()
	terminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		fixture.task.ID,
		fixture.now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	tombstoneValue := fixture.mustGet(
		t, testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	tombstone, err := testdeletions.DecodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		t.Fatalf("decodeDeletionTombstone() error = %v", err)
	}
	tombstone.TaskID = ids.NewAt(ids.KindTask, fixture.now, 8070)
	tombstone.UpdatedAt = fixture.now.Add(2 * time.Second)
	changedValue, err := testdeletions.EncodeDeletionTombstone(tombstone)
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	defer clear(changedValue)
	racingStore := &environmentDeletionRetryRaceStore{
		memoryHierarchyStore: fixture.store, key: tombstoneValue.Key, value: changedValue,
	}
	racingTasks, err := newTaskRepository(racingStore)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	retryAt := fixture.now.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 8071)
	marker := pendingRetryMarker(
		terminal.Record,
		retryID,
		retryAt,
		"environment-delete-race-key-0001",
	)
	retryResult, err := racingTasks.RetryTask(
		ctx, fixture.task.ID, retryID, testtaskjournal.TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatalf("RetryTask(owner race) error = %v", err)
	}
	_, _, conflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("RetryTask(owner race) = %#v, %v", retryResult, classifyErr)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, testtaskjournal.TaskStorageKey(retryID), false)
}

// Rationale: successful Environment deletion must fail closed while any
// Backup reverse membership or operation-owned cleanup work remains.
func TestEnvironmentDeletionCompletionRejectsRetainedBackupState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		key  func(*environmentDeletionLockFixture) string
	}{
		{
			name: "policy",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "key metadata",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackuppolicy.BackupKeyKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "wrapped key identity",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackuppolicy.BackupKeyValueKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "source membership",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackuppolicy.BackupSourceEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "connector membership",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testconnectors.ConnectorEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "schedule cursor",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupScheduleCursorPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "due authority",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupDueOutcomePrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "recovery point",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupRecoveryPointEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "run",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupRunEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "orphan",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupOrphanEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "restore",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupRestoreEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "key rotation",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testbackupruntime.BackupKeyRotationEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "cleanup work",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testdeletions.EnvironmentDeletionWorkOperationPrefix(fixture.task.OperationID) + "retained"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			fixture.putRaw(t, test.key(fixture), []byte("retained"))
			agentID := ids.NewAt(ids.KindAgent, fixture.now, 8080)
			if _, found, err := fixture.tasks.ClaimNextTask(
				context.Background(), agentID, 1, fixture.now.Add(time.Second),
			); err != nil || !found {
				t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
			}
			result := testtaskjournal.TaskResultRecord{
				Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			}
			if _, err := fixture.tasks.AcknowledgeTask(
				context.Background(), agentID, 1, fixture.task.ID,
				taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
			); !isKind(err, errs.KindStateConflict) {
				t.Fatalf("AcknowledgeTask(retained %s) error = %v", test.name, err)
			}
			assertEnvironmentDeletionCompanion(
				t, fixture.store, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID), true,
			)
			assertEnvironmentDeletionCompanion(
				t, fixture.store, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID), true,
			)
		})
	}
}

// Rationale: Environment finalization must not orphan any implemented durable
// child or active Environment authority, even if it appears after deletion
// publication but before the final atomic transaction.
func TestEnvironmentDeletionCompletionRejectsRetainedDurableChildren(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		key  func(*environmentDeletionLockFixture) string
	}{
		{
			name: "zone reservation",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testnetworkreservations.ZonePoolRegistryKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "Blueprint revision",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testblueprints.EnvironmentBlueprintRevisionsPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "route",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testroutes.OwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "entry",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testentries.EntryOwnerCollectionPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "script",
			key: func(fixture *environmentDeletionLockFixture) string {
				active, err := testscripts.ReadActiveScriptSet(
					context.Background(), fixture.store, fixture.environment.Record.ID, 0,
				)
				if err != nil {
					panic(err)
				}
				return testscripts.ScriptSetOwnerKey(fixture.environment.Record.ID, active.Record.GenerationID, "retained")
			},
		},
		{
			name: "attach",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testattachments.AttachOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "component",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testcomponents.EnvironmentOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "connector",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testconnectors.ConnectorEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "release group",
			key: func(fixture *environmentDeletionLockFixture) string {
				return "/v1/indexes/release-groups/by-owner/environment/" +
					fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "component task authority",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testenvironmentchanges.ComponentTaskActiveEnvironmentKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "coordination authority",
			key: func(fixture *environmentDeletionLockFixture) string {
				return testenvironmentcoordination.Key(fixture.environment.Record.ID)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			agentID := ids.NewAt(ids.KindAgent, fixture.now, 8081)
			if _, found, err := fixture.tasks.ClaimNextTask(
				context.Background(), agentID, 1, fixture.now.Add(time.Second),
			); err != nil || !found {
				t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
			}
			fixture.putRaw(t, test.key(fixture), []byte("retained"))
			result := testtaskjournal.TaskResultRecord{
				Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			}
			_, err := fixture.tasks.AcknowledgeTask(
				context.Background(),
				agentID,
				1,
				fixture.task.ID,
				taskAssignmentIDForTest(
					t,
					fixture.tasks,
					fixture.task.ID,
				),
				testtaskjournal.TaskStatusCompleted,
				result,
				fixture.now.Add(2*time.Second),
			)
			assertExactStateConflict(t, err, "AcknowledgeTask(retained "+test.name+")")
			assertEnvironmentDeletionCompanion(
				t, fixture.store, testhierarchy.EnvironmentKey(fixture.environment.Record.ID), true,
			)
			for _, key := range []string{testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID), testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID), testtaskjournal.TaskStorageKey(fixture.task.ID), testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID), testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID), testnetworkreservations.EnvironmentPoolRegistryKey} {
				assertEnvironmentDeletionCompanion(t, fixture.store, key, true)
			}
		})
	}
}

// Rationale: a child created after the fixed-revision absence read must lose
// the terminal transaction's empty-prefix compare, retaining both parent and
// child instead of committing an orphan.
func TestEnvironmentDeletionCompletionFencesChildInsertionAtTerminalCommit(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	projectionTask := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 84)
	projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, projectionTask, 1)
	desired := serviceRecordTestDesired()
	desired.ID = ids.NewAt(ids.KindService, projectionTask.CreatedAt, 70)
	desired.Name = "api"
	projection.DesiredServices = []testservices.EnvironmentServiceProjection{
		{EnvironmentID: fixture.environment.Record.ID, Desired: desired},
	}
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encode Environment projection = %v", err)
	}
	defer clear(projectionValue)
	headValue, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatalf("encode Environment desired head = %v", err)
	}
	defer clear(headValue)
	if transaction, transactErr := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID), Value: headValue},
		{Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.environment.Record.ID), Value: projectionValue},
	}); transactErr != nil || !transaction.Succeeded {
		t.Fatalf("seed Environment projection = %#v, %v", transaction, transactErr)
	}
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8084)
	if _, found, err := fixture.tasks.ClaimNextTask(context.Background(), agentID, 1, fixture.now.Add(time.Second)); err != nil ||
		!found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	childKey := testservices.ServiceRuntimeKey(desired.ID)
	racingStore := &environmentDeletionFinalizationRaceStore{
		memoryHierarchyStore: fixture.store,
		environmentID:        fixture.environment.Record.ID,
		childKey:             childKey,
	}
	racingTasks, err := newTaskRepository(racingStore)
	if err != nil {
		t.Fatalf("newTaskRepository(racing) error = %v", err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	_, err = racingTasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		fixture.task.ID,
		taskAssignmentIDForTest(
			t,
			fixture.tasks,
			fixture.task.ID,
		),
		testtaskjournal.TaskStatusCompleted,
		result,
		fixture.now.Add(2*time.Second),
	)
	assertExactStateConflict(t, err, "AcknowledgeTask(child insertion race)")
	if !racingStore.raced {
		t.Fatal("terminal transaction did not reach the child insertion race")
	}
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		testhierarchy.EnvironmentKey(fixture.environment.Record.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), fixture.environment.Record.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(t, fixture.store, childKey, true)
}

// Rationale: successful Environment finalization removes the selected desired
// head and projection before removing their owning Environment parent.
func TestEnvironmentDeletionCompletionRemovesDesiredProjectionBeforeParent(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	projectionTask := environmentBlueprintTestTask(
		t, fixture.project.Record, fixture.environment.Record, 84,
	)
	projection := environmentBlueprintTestProjection(
		fixture.environment.Record.ID, projectionTask, 1,
	)
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encode Environment projection = %v", err)
	}
	defer clear(projectionValue)
	headValue, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatalf("encode Environment desired head = %v", err)
	}
	defer clear(headValue)
	if transaction, transactErr := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID), Value: headValue},
		{Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.environment.Record.ID), Value: projectionValue},
	}); transactErr != nil || !transaction.Succeeded {
		t.Fatalf("seed Environment projection = %#v, %v", transaction, transactErr)
	}
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8083)
	if _, found, err := fixture.tasks.ClaimNextTask(
		context.Background(), agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	recordingStore := &environmentDeletionMutationRecordingStore{
		memoryHierarchyStore: fixture.store,
		environmentID:        fixture.environment.Record.ID,
	}
	recordingTasks, err := newTaskRepository(recordingStore)
	if err != nil {
		t.Fatalf("newTaskRepository(recording) error = %v", err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	if _, err := recordingTasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, recordingTasks, fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	headKey := testblueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID)
	projectionKey := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.environment.Record.ID)
	parentKey := testhierarchy.EnvironmentKey(fixture.environment.Record.ID)
	headIndex, projectionIndex, parentIndex := -1, -1, -1
	for index, key := range recordingStore.terminalMutationKeys {
		switch key {
		case headKey:
			headIndex = index
		case projectionKey:
			projectionIndex = index
		case parentKey:
			parentIndex = index
		}
	}
	if headIndex < 0 || projectionIndex < 0 || parentIndex < 0 ||
		headIndex >= parentIndex || projectionIndex >= parentIndex {
		t.Fatalf(
			"terminal deletion order head/projection/parent = %d/%d/%d, mutations = %v",
			headIndex, projectionIndex, parentIndex, recordingStore.terminalMutationKeys,
		)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, headKey, false)
	assertEnvironmentDeletionCompanion(t, fixture.store, projectionKey, false)
	assertEnvironmentDeletionCompanion(t, fixture.store, parentKey, false)
}

// Rationale: immutable Task journals are historical audit records, not live
// Environment children, and must survive successful owner finalization.
func TestEnvironmentDeletionCompletionRetainsHistoricalTaskJournal(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8082)
	if _, found, err := fixture.tasks.ClaimNextTask(
		context.Background(), agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	if _, err := fixture.tasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, testhierarchy.EnvironmentKey(fixture.environment.Record.ID), false,
	)
	assertEnvironmentDeletionCompanion(t, fixture.store, testtaskjournal.TaskStorageKey(fixture.task.ID), true)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store, testtaskjournal.TaskEnvironmentIndexKey(fixture.environment.Record.ID, fixture.task.ID), true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		testtaskjournal.TaskWorkspaceTenantIndexKey(fixture.project.Record.TenantID, fixture.task.ID),
		true,
	)
}

// Rationale: non-empty Backup authority at deletion initiation must persist an
// enumerating phase; later key absence alone cannot manufacture completion,
// which requires an explicit owned cleanup-completion transaction.
func TestEnvironmentDeletionRequiresAffirmativeCleanupCompletion(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	ctx := context.Background()
	policyKey := testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID)
	fixture.putRaw(t, policyKey, []byte("retained"))
	current, err := fixture.hierarchy.GetEnvironment(ctx, fixture.environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	fixture.environment = current
	fixture.mustBegin(t)
	intentEntry := fixture.mustGet(t, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := testdeletions.DecodeEnvironmentDeletionIntent(intentEntry.Value)
	if err != nil || intent.CleanupPhase != testdeletions.EnvironmentDeletionCleanupEnumerating {
		t.Fatalf("initial Environment deletion cleanup intent = %#v, %v", intent, err)
	}
	transaction, err := fixture.store.Transact(
		ctx,
		[]testkeyvalue.Condition{{Key: policyKey, ModRevision: fixture.mustGet(t, policyKey).ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: policyKey}},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("delete retained Backup policy = %#v, %v", transaction, err)
	}
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8085)
	if _, found, err := fixture.tasks.ClaimNextTask(
		ctx, agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	if _, err := fixture.tasks.AcknowledgeTask(
		ctx, agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("AcknowledgeTask(before cleanup completion) error = %v", err)
	}
	currentTask, err := fixture.tasks.GetTask(ctx, fixture.task.ID)
	if err != nil {
		t.Fatalf("GetTask(cleanup owner) error = %v", err)
	}
	completed, err := fixture.tasks.CompleteEnvironmentDeletionCleanupEnumeration(
		ctx,
		currentTask.Record,
	)
	if err != nil || completed.Record.CleanupPhase != testdeletions.EnvironmentDeletionCleanupComplete {
		t.Fatalf("CompleteEnvironmentDeletionCleanupEnumeration() = %#v, %v", completed, err)
	}
	terminal, err := fixture.tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		fixture.task.ID,
		taskAssignmentIDForTest(
			t,
			fixture.tasks,
			fixture.task.ID,
		),
		testtaskjournal.TaskStatusCompleted,
		result,
		fixture.now.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(after cleanup completion) = %#v, %v", terminal, err)
	}
}

// Rationale: a terminal attempt is retry evidence, not an active cleanup
// driver, and therefore cannot advance an enumerating deletion intent.
func TestEnvironmentDeletionTerminalTaskCannotCompleteCleanupEnumeration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newEnvironmentDeletionLockFixture(t)
	policyKey := testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID)
	fixture.putRaw(t, policyKey, []byte("retained"))
	fixture.mustBegin(t)
	if transaction, err := fixture.store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: policyKey}},
	); err != nil || !transaction.Succeeded {
		t.Fatalf("delete retained Backup policy = %#v, %v", transaction, err)
	}
	terminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		fixture.task.ID,
		fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if _, err := fixture.tasks.CompleteEnvironmentDeletionCleanupEnumeration(
		ctx,
		terminal.Record,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("CompleteEnvironmentDeletionCleanupEnumeration(terminal) error = %v", err)
	}
	entry := fixture.mustGet(t, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := testdeletions.DecodeEnvironmentDeletionIntent(entry.Value)
	if err != nil || intent.CleanupPhase != testdeletions.EnvironmentDeletionCleanupEnumerating {
		t.Fatalf("terminal cleanup intent = %#v, %v", intent, err)
	}
}

// Rationale: assignment timeout is a failed deletion attempt, not authority
// cancellation, so its exact fence and immutable cleanup intent must survive.
func TestEnvironmentDeletionTimeoutRetainsFenceAndIntent(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	assignment, found, err := fixture.tasks.ClaimNextTask(
		context.Background(),
		ids.NewAt(ids.KindAgent, fixture.now, 8090),
		1,
		fixture.now.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	count, err := fixture.tasks.ExpireTimedOutTasks(
		context.Background(),
		assignment.Assignment.Record.Deadline,
	)
	if err != nil || count != 1 {
		t.Fatalf("ExpireTimedOutTasks() = %d, %v", count, err)
	}
	terminal, err := fixture.tasks.GetTask(context.Background(), fixture.task.ID)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
		t.Fatalf("timed-out Environment deletion = %#v, %v", terminal, err)
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID), true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store, testdeletions.TombstoneKey(
			string(testdeletions.DeletionTargetEnvironment),
			fixture.environment.Record.ID,
		), true,
	)
	assertEnvironmentDeletionCompanion(
		t, fixture.store, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID), true,
	)
}

type environmentDeletionRetryRaceStore struct {
	*memoryHierarchyStore
	key   string
	value []byte
	raced bool
}

func (store *environmentDeletionRetryRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.raced {
		store.raced = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: store.key, Value: store.value,
		}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type environmentDeletionFinalizationRaceStore struct {
	*memoryHierarchyStore
	environmentID string
	childKey      string
	raced         bool
}

func (store *environmentDeletionFinalizationRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.raced {
		for _, mutation := range mutations {
			if mutation.Type != testkeyvalue.MutationDelete || mutation.Prefix ||
				mutation.Key != testhierarchy.EnvironmentKey(store.environmentID) {
				continue
			}
			inserted, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: store.childKey, Value: []byte("retained"),
			}})
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			if !inserted.Succeeded {
				return testkeyvalue.TransactionResult{}, errs.New(
					errs.KindInternal,
					"child insertion race did not commit",
				)
			}
			store.raced = true
			for _, condition := range conditions {
				if condition.Key == store.childKey ||
					(condition.Prefix && strings.HasPrefix(store.childKey, condition.Key)) {
					return testkeyvalue.TransactionResult{Succeeded: false, Revision: inserted.Revision}, nil
				}
			}
			return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal,
				"terminal transaction did not compare the child authority")
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type environmentDeletionMutationRecordingStore struct {
	*memoryHierarchyStore
	environmentID        string
	terminalMutationKeys []string
}

func (store *environmentDeletionMutationRecordingStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationDelete && !mutation.Prefix &&
			mutation.Key == testhierarchy.EnvironmentKey(store.environmentID) {
			store.terminalMutationKeys = make([]string, 0, len(mutations))
			for _, terminalMutation := range mutations {
				store.terminalMutationKeys = append(store.terminalMutationKeys, terminalMutation.Key)
			}
			break
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type environmentDeletionLockFixture struct {
	store       *memoryHierarchyStore
	hierarchy   *HierarchyRepository
	tasks       *TaskRepository
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	task        TaskRecord
	tombstone   testdeletions.DeletionTombstoneRecord
	marker      testidempotency.IdempotencyMarker
	now         time.Time
}

func newEnvironmentDeletionLockFixture(t *testing.T) *environmentDeletionLockFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 8000)
	if _, err := hierarchy.CreateTenant(ctx, testhierarchy.TenantRecord{
		ID: tenantID, Slug: "deletion-tenant", Name: "Deletion Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 8001), TenantID: tenantID,
		Slug: "deletion-project", Name: "Deletion Project", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 8002)
	environment, err := hierarchy.CreateEnvironment(ctx, testhierarchy.EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.Record.ID,
		Name:              "production",
		NetworkPool:       "10.248.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 8003),
		CreatedAt:         now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := validTaskRecord(now.Add(time.Minute))
	task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, 8004)
	task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, 8005)
	task.IdempotencyKey = "environment-delete-key-0001"
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorOperator
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.Type = testtaskjournal.TaskRemove
	task.Target = environment.Record.ID
	task.Params = map[string]string{testtaskjournal.TaskMaterializationEnvironmentParam: environment.Record.ID}
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: "/environments/{id}", Key: task.IdempotencyKey,
	}
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetEnvironment, TargetID: environment.Record.ID,
		TargetRevision: environment.Revision, TaskID: task.ID,
		Phase: testdeletions.DeletionPhaseHostEffects, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	fixture := &environmentDeletionLockFixture{
		store:       store,
		hierarchy:   hierarchy,
		tasks:       tasks,
		project:     project,
		environment: environment,
		task:        task,
		tombstone:   tombstone,
		marker:      marker,
		now:         task.CreatedAt,
	}
	poolRegistryValue, err := testrecordcodec.Encode(
		"environment_pool_registry",
		testnetworkreservations.EnvironmentPoolRegistry{
			Reservations: map[string]string{environmentID: environment.Record.NetworkPool},
		},
	)
	if err != nil {
		t.Fatalf("encode Environment pool registry error = %v", err)
	}
	defer clear(poolRegistryValue)
	fixture.putRaw(t, testnetworkreservations.EnvironmentPoolRegistryKey, poolRegistryValue)
	return fixture
}

func (fixture *environmentDeletionLockFixture) begin() (IdempotencyTransactionResult, error) {
	return fixture.hierarchy.BeginEnvironmentDeletionWithTask(
		context.Background(), fixture.project, fixture.environment, 0,
		fixture.tombstone, fixture.task, fixture.marker,
	)
}

func (fixture *environmentDeletionLockFixture) mustBegin(t *testing.T) {
	t.Helper()
	result, err := fixture.begin()
	if err != nil {
		t.Fatalf("BeginEnvironmentDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginEnvironmentDeletionWithTask() = %#v/%v", result, classifyErr)
	}
}

func (fixture *environmentDeletionLockFixture) putLock(
	t *testing.T,
	record testbackupruntime.BackupOperationLockRecord,
) {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupOperationLockRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupOperationLockRecord() error = %v", err)
	}
	defer clear(value)
	fixture.putRaw(t, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID), value)
}

func (fixture *environmentDeletionLockFixture) rewriteEpoch(t *testing.T) {
	t.Helper()
	value := fixture.mustGet(t, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID))
	fixture.putRaw(t, value.Key, value.Value)
}

func (fixture *environmentDeletionLockFixture) putRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	transaction, err := fixture.store.Transact(
		context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("put %s = %#v, %v", key, transaction, err)
	}
}

func (fixture *environmentDeletionLockFixture) mustGet(t *testing.T, key string) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := fixture.store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("Get(%s) = %#v, %v", key, result, err)
	}
	return result.Entry
}

func assertEnvironmentDeletionCompanion(
	t *testing.T,
	store *memoryHierarchyStore,
	key string,
	want bool,
) {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", key, err)
	}
	if got := result.Entry != nil; got != want {
		t.Fatalf("Get(%s) present = %t, want %t", key, got, want)
	}
}

func assertExactStateConflict(t *testing.T, err error, operation string) {
	t.Helper()
	kind, ok := errs.KindOf(err)
	var domainError *errs.Error
	if !ok || kind != errs.KindStateConflict || !errors.As(err, &domainError) ||
		domainError.ToProblem().Code != errs.CodeStateConflict {
		t.Fatalf("%s error = %v, want %d/%s", operation, err, errs.KindStateConflict, errs.CodeStateConflict)
	}
}

type environmentDeletionUnknownStore struct {
	*memoryHierarchyStore
	failNext error
}

func (store *environmentDeletionUnknownStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return testkeyvalue.TransactionResult{}, failure
	}
	return result, err
}
