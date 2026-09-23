package etcd

import (
	context "context"
	testing "testing"
	time "time"

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
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
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
