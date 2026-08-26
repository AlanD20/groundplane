package etcd

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Environment deletion must lose before publishing host effects when
// another persistence operation already owns the canonical lock.
func TestEnvironmentDeletionRejectsHeldOperationLock(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	other := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8010),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8011),
		Kind:          BackupOperationBackup,
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
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(fixture.task.ID), false)
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
			t,
			environmentMutationEpochKey(fixture.environment.Record.ID),
			[]byte("corrupt"),
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
		environmentBlueprintManifestKey(fixture.environment.Record.ID, revisionID),
		[]byte("revision"),
	)
	other := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8021),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8022),
		Kind:          BackupOperationRestore,
		CreatedAt:     fixture.now,
		UpdatedAt:     fixture.now,
	}
	fixture.putLock(t, other)
	if _, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch(other owner) error = %v", err)
	}
	owned := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   fixture.task.OperationID,
		TaskID:        fixture.task.ID,
		Kind:          BackupOperationDeletion,
		CreatedAt:     fixture.task.CreatedAt,
		UpdatedAt:     fixture.task.CreatedAt,
	}
	fixture.putLock(t, owned)
	before := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	changed, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(2*time.Second),
	)
	if err != nil || !changed {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch() = %t, %v", changed, err)
	}
	after := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	if after.ModRevision <= before.ModRevision {
		t.Fatalf("mutation epoch revision = %d, want > %d", after.ModRevision, before.ModRevision)
	}
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		environmentBlueprintManifestKey(fixture.environment.Record.ID, revisionID),
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
				result, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
					Type: MutationDelete,
					Key: deletionTombstoneKey(
						string(DeletionTargetEnvironment),
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
				value, err := encodeDeletionTombstone(tombstone)
				if err != nil {
					t.Fatalf("encodeDeletionTombstone() error = %v", err)
				}
				defer clear(value)
				fixture.putRaw(
					t,
					deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
					value,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			test.mutate(t, fixture)
			_, err := loadOwnedEnvironmentMutationFence(
				context.Background(),
				fixture.store,
				fixture.environment.Record.ID,
				fixture.store.revision,
				environmentMutationFenceOwner{
					Kind:        BackupOperationDeletion,
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
	owner := environmentMutationFenceOwner{
		Kind:        BackupOperationDeletion,
		OperationID: fixture.task.OperationID,
		TaskID:      fixture.task.ID,
	}
	evidence, err := loadOwnedEnvironmentMutationFence(
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
		t,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	fixture.putRaw(t, tombstone.Key, tombstone.Value)
	epochMutation, err := evidence.epochRewriteMutation()
	if err != nil {
		t.Fatalf("epochRewriteMutation() error = %v", err)
	}
	defer clear(epochMutation.Value)
	result, err := fixture.store.Transact(
		context.Background(), evidence.transactionConditions(), []Mutation{epochMutation},
	)
	if err != nil || result.Succeeded {
		t.Fatalf("Transact(stale tombstone) = %#v, %v", result, err)
	}
	conflict := evidence.classifyCAS(result.FailureReads)
	clearKeyValues(result.FailureReads)
	if !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("classifyCAS(stale tombstone) error = %v", conflict)
	}
}

// Rationale: a non-successful Environment deletion must retain its exact
// operation fence and immutable cleanup intent so retry cannot resnapshot or
// expose an unlocked interval; only success may remove them.
func TestEnvironmentDeletionTerminalOutcomesCleanCompanionsAndReplay(t *testing.T) {
	t.Parallel()
	for _, terminalStatus := range []TaskStatus{TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted} {
		t.Run(string(terminalStatus), func(t *testing.T) {
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			agentID := ids.NewAt(ids.KindAgent, fixture.now, 8030)
			var terminal Versioned[TaskRecord]
			var err error
			if terminalStatus == TaskStatusAborted {
				terminal, err = fixture.tasks.AbortPendingTask(
					context.Background(), fixture.task.ID, fixture.now.Add(time.Second),
				)
			} else {
				if _, found, claimErr := fixture.tasks.ClaimNextTask(
					context.Background(), agentID, 1, fixture.now.Add(time.Second),
				); claimErr != nil || !found {
					t.Fatalf("ClaimNextTask() found/error = %t/%v", found, claimErr)
				}
				result := TaskResultRecord{
					Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
				}
				if terminalStatus == TaskStatusFailed {
					result.ExitCode = 1
				}
				terminal, err = fixture.tasks.AcknowledgeTask(
					context.Background(),
					agentID,
					1,
					fixture.task.ID,
					taskAssignmentIDForTest(t, fixture.tasks,
						fixture.task.ID),
					terminalStatus,
					result,
					fixture.now.Add(2*time.Second),
				)

			}
			if err != nil {
				t.Fatalf("terminalize Environment deletion error = %v", err)
			}
			if terminalStatus == TaskStatusCompleted {
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					environmentOperationLockKey(fixture.environment.Record.ID),
					false,
				)
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					deletionTombstoneKey(
						string(DeletionTargetEnvironment),
						fixture.environment.Record.ID,
					),
					false,
				)
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentDeletionIntentKey(fixture.task.OperationID), false,
				)
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentKey(fixture.environment.Record.ID), false,
				)
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					environmentMutationEpochKey(fixture.environment.Record.ID),
					false,
				)
			} else {
				lockValue := fixture.mustGet(
					t,
					environmentOperationLockKey(fixture.environment.Record.ID),
				)
				lock, decodeErr := decodeOwnedEnvironmentDeletionLock(lockValue, fixture.task)
				if decodeErr != nil || lock.OperationID != fixture.task.OperationID {
					t.Fatalf("retained Environment deletion lock = %#v, %v", lock, decodeErr)
				}
				tombstoneValue := fixture.mustGet(
					t,
					deletionTombstoneKey(
						string(DeletionTargetEnvironment),
						fixture.environment.Record.ID,
					),
				)
				tombstone, decodeErr := decodeDeletionTombstone(tombstoneValue.Value)
				if decodeErr != nil || tombstone.TaskID != fixture.task.ID ||
					tombstone.Checkpoint != fixture.tombstone.Checkpoint {
					t.Fatalf(
						"retained Environment deletion tombstone = %#v, %v",
						tombstone,
						decodeErr,
					)
				}
				intentValue := fixture.mustGet(
					t,
					environmentDeletionIntentKey(fixture.task.OperationID),
				)
				intent, decodeErr := decodeEnvironmentDeletionIntent(intentValue.Value)
				if decodeErr != nil || intent.EnvironmentID != fixture.environment.Record.ID ||
					intent.OperationID != fixture.task.OperationID ||
					intent.TargetRevision != fixture.environment.Revision {
					t.Fatalf("retained Environment deletion intent = %#v, %v", intent, decodeErr)
				}
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentKey(fixture.environment.Record.ID), true,
				)
				epoch := fixture.mustGet(
					t,
					environmentMutationEpochKey(fixture.environment.Record.ID),
				)
				if epoch.ModRevision != terminal.Revision {
					t.Fatalf(
						"terminal epoch revision = %d, want %d",
						epoch.ModRevision,
						terminal.Revision,
					)
				}
			}
			var replay Versioned[TaskRecord]
			if terminalStatus == TaskStatusAborted {
				replay, err = fixture.tasks.AbortPendingTask(
					context.Background(), fixture.task.ID, fixture.now.Add(3*time.Second),
				)
			} else {
				replay, err = fixture.tasks.AcknowledgeTask(
					context.Background(),
					agentID,
					1,
					fixture.task.ID,
					taskAssignmentIDForTest(t, fixture.tasks,
						fixture.task.ID),
					terminalStatus,
					*terminal.Record.Result,
					fixture.now.Add(2*time.Second),
				)

			}
			if err != nil || replay.Revision != terminal.Revision {
				t.Fatalf("terminal replay = %#v, %v", replay, err)
			}
		})
	}
}

// Rationale: when etcd commits terminal cleanup but the response is lost, the
// exact acknowledgement retry must validate companion cleanup and return replay.
func TestEnvironmentDeletionTerminalUnknownOutcomeReplaysCommittedCleanup(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8040)
	if _, found, err := fixture.tasks.ClaimNextTask(
		context.Background(), agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	unknown := errs.New(errs.KindStorageUnavailable, "unknown Environment deletion outcome")
	failingStore := &environmentDeletionUnknownStore{
		memoryHierarchyStore: fixture.store,
		failNext:             unknown,
	}
	failingTasks, err := newTaskRepository(failingStore)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	result := TaskResultRecord{
		Kind:       TaskResultEnvironmentDirectory,
		Diagnostic: TaskResultDiagnosticNone,
	}
	terminalAt := fixture.now.Add(2 * time.Second)
	if _, err := failingTasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, failingTasks,
			fixture.task.ID),
		TaskStatusCompleted, result, terminalAt); !errors.Is(err, unknown) {
		t.Fatalf("AcknowledgeTask(unknown) error = %v", err)
	}
	replay, err := fixture.tasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, fixture.tasks,
			fixture.task.ID),
		TaskStatusCompleted, result, terminalAt)

	if err != nil || replay.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(after unknown) = %#v, %v", replay, err)
	}
}

// Rationale: Environment deletion publication must make the immutable intent
// visible at the same revision as its Task, tombstone, and exact operation lock.
func TestEnvironmentDeletionPublishesIntentAtomically(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	stored, err := fixture.store.GetMany(context.Background(), GetManyRequest{Keys: []string{
		taskKey(fixture.task.ID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
		environmentOperationLockKey(fixture.environment.Record.ID),
		environmentDeletionIntentKey(fixture.task.OperationID),
	}})
	if err != nil || stored == nil || len(stored.Values) != 4 {
		t.Fatalf("Environment deletion publication = %#v, %v", stored, err)
	}
	for _, value := range stored.Values {
		if value == nil || value.ModRevision != stored.Values[0].ModRevision {
			t.Fatalf("Environment deletion publication revisions = %#v", stored.Values)
		}
	}
	intent, err := decodeEnvironmentDeletionIntent(stored.Values[3].Value)
	if err != nil || intent.EnvironmentID != fixture.environment.Record.ID ||
		intent.OperationID != fixture.task.OperationID || intent.TaskID != fixture.task.ID ||
		intent.TargetRevision != fixture.environment.Revision ||
		intent.CleanupPhase != EnvironmentDeletionCleanupComplete {
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
		backupPolicyKey(fixture.environment.Record.ID),
		[]byte("authority-created-after-environment-read"),
	)
	fixture.mustBegin(t)
	entry := fixture.mustGet(t, environmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := decodeEnvironmentDeletionIntent(entry.Value)
	if err != nil || intent.CleanupPhase != EnvironmentDeletionCleanupEnumerating {
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
		t,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	tombstoneBefore, err := decodeDeletionTombstone(tombstoneEntry.Value)
	if err != nil {
		t.Fatalf("decodeDeletionTombstone() error = %v", err)
	}
	tombstoneBefore.Phase = DeletionPhaseFinalizing
	tombstoneBefore.Checkpoint = DeletionCheckpoint{
		ResourceKind: "backup_policy", StableID: fixture.environment.Record.ID,
	}
	tombstoneBefore.UpdatedAt = fixture.now.Add(time.Nanosecond)
	tombstoneValue, err := encodeDeletionTombstone(tombstoneBefore)
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
	result := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone, ExitCode: 1,
	}
	failed, err := fixture.tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
		TaskStatusFailed,
		result,
		fixture.now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(failed) error = %v", err)
	}
	fixture.mustGet(t, environmentOperationLockKey(fixture.environment.Record.ID))

	retryAt := fixture.now.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 8061)
	marker := pendingRetryMarker(
		failed.Record,
		retryID,
		retryAt,
		"environment-delete-retry-key-0001",
	)
	retryResult, err := fixture.tasks.RetryTask(
		ctx, fixture.task.ID, retryID, TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatalf("RetryTask(Environment deletion) error = %v", err)
	}
	outcome, _, conflict, err := retryResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask(Environment deletion) = %#v, %v", retryResult, err)
	}
	stored, err := fixture.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(retryID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
		environmentOperationLockKey(fixture.environment.Record.ID),
		environmentMutationEpochKey(fixture.environment.Record.ID),
		environmentDeletionIntentKey(fixture.task.OperationID),
	}})
	if err != nil || stored == nil || len(stored.Values) != 5 {
		t.Fatalf("Environment deletion retry state = %#v, %v", stored, err)
	}
	for _, value := range stored.Values {
		if value == nil || value.ModRevision != stored.Values[0].ModRevision {
			t.Fatalf("Environment deletion retry revisions = %#v", stored.Values)
		}
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TaskID != retryID || tombstone.Phase != DeletionPhaseFinalizing ||
		tombstone.Checkpoint != tombstoneBefore.Checkpoint {
		t.Fatalf("transferred Environment deletion tombstone = %#v, %v", tombstone, err)
	}
	lock, err := decodeEnvironmentOperationLock(stored.Values[2], fixture.environment.Record.ID)
	if err != nil || lock.TaskID != retryID || lock.OperationID != fixture.task.OperationID {
		t.Fatalf("transferred Environment deletion lock = %#v, %v", lock, err)
	}
	intent, err := decodeEnvironmentDeletionIntent(stored.Values[4].Value)
	if err != nil || intent.TaskID != retryID ||
		intent.CleanupPhase != EnvironmentDeletionCleanupComplete {
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
		t,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		t.Fatalf("decodeDeletionTombstone() error = %v", err)
	}
	tombstone.TaskID = ids.NewAt(ids.KindTask, fixture.now, 8070)
	tombstone.UpdatedAt = fixture.now.Add(2 * time.Second)
	changedValue, err := encodeDeletionTombstone(tombstone)
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
		ctx, fixture.task.ID, retryID, TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatalf("RetryTask(owner race) error = %v", err)
	}
	_, _, conflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("RetryTask(owner race) = %#v, %v", retryResult, classifyErr)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(retryID), false)
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
				return backupPolicyKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "key metadata",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupKeyKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "wrapped key identity",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupKeyValueKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "source membership",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupSourceEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "connector membership",
			key: func(fixture *environmentDeletionLockFixture) string {
				return connectorEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "schedule cursor",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupScheduleCursorPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "due authority",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupDueOutcomePrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "recovery point",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupRecoveryPointEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "run",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupRunEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "orphan",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupOrphanEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "restore",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupRestoreEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "key rotation",
			key: func(fixture *environmentDeletionLockFixture) string {
				return backupKeyRotationEnvironmentPrefix + fixture.environment.Record.ID + "/retained"
			},
		},
		{
			name: "cleanup work",
			key: func(fixture *environmentDeletionLockFixture) string {
				return environmentDeletionWorkOperationPrefix(fixture.task.OperationID) + "retained"
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
			result := TaskResultRecord{
				Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
			}
			if _, err := fixture.tasks.AcknowledgeTask(
				context.Background(), agentID, 1, fixture.task.ID,
				taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
				TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
			); !isKind(err, errs.KindStateConflict) {
				t.Fatalf("AcknowledgeTask(retained %s) error = %v", test.name, err)
			}
			assertEnvironmentDeletionCompanion(
				t, fixture.store, environmentOperationLockKey(fixture.environment.Record.ID), true,
			)
			assertEnvironmentDeletionCompanion(
				t, fixture.store, environmentDeletionIntentKey(fixture.task.OperationID), true,
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
			name: "zone",
			key: func(fixture *environmentDeletionLockFixture) string {
				return zoneOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "zone reservation",
			key: func(fixture *environmentDeletionLockFixture) string {
				return zonePoolRegistryKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "service",
			key: func(fixture *environmentDeletionLockFixture) string {
				return serviceOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "route",
			key: func(fixture *environmentDeletionLockFixture) string {
				return routeOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "entry",
			key: func(fixture *environmentDeletionLockFixture) string {
				return entryOwnerCollectionPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "script",
			key: func(fixture *environmentDeletionLockFixture) string {
				return scriptOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "attach",
			key: func(fixture *environmentDeletionLockFixture) string {
				return attachOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "component",
			key: func(fixture *environmentDeletionLockFixture) string {
				return componentEnvironmentOwnerPrefix(fixture.environment.Record.ID) + "retained"
			},
		},
		{
			name: "connector",
			key: func(fixture *environmentDeletionLockFixture) string {
				return connectorEnvironmentPrefix(fixture.environment.Record.ID) + "retained"
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
				return componentTaskActiveEnvironmentKey(fixture.environment.Record.ID)
			},
		},
		{
			name: "coordination authority",
			key: func(fixture *environmentDeletionLockFixture) string {
				return environmentCoordinationKey(fixture.environment.Record.ID)
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
			result := TaskResultRecord{
				Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
			}
			_, err := fixture.tasks.AcknowledgeTask(
				context.Background(), agentID, 1, fixture.task.ID,
				taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
				TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
			)
			assertExactStateConflict(t, err, "AcknowledgeTask(retained "+test.name+")")
			assertEnvironmentDeletionCompanion(
				t, fixture.store, environmentKey(fixture.environment.Record.ID), true,
			)
			for _, key := range []string{
				deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
				environmentDeletionIntentKey(fixture.task.OperationID),
				taskKey(fixture.task.ID),
				environmentOperationLockKey(fixture.environment.Record.ID),
				environmentMutationEpochKey(fixture.environment.Record.ID),
				environmentPoolRegistryKey,
			} {
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
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8084)
	if _, found, err := fixture.tasks.ClaimNextTask(
		context.Background(), agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	childKey := serviceOwnerPrefix(fixture.environment.Record.ID) + "commit-race"
	racingStore := &environmentDeletionFinalizationRaceStore{
		memoryHierarchyStore: fixture.store,
		environmentID:        fixture.environment.Record.ID,
		childKey:             childKey,
	}
	racingTasks, err := newTaskRepository(racingStore)
	if err != nil {
		t.Fatalf("newTaskRepository(racing) error = %v", err)
	}
	result := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	_, err = racingTasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
		TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	)
	assertExactStateConflict(t, err, "AcknowledgeTask(child insertion race)")
	if !racingStore.raced {
		t.Fatal("terminal transaction did not reach the child insertion race")
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, environmentKey(fixture.environment.Record.ID), true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(t, fixture.store, childKey, true)
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
	result := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	if _, err := fixture.tasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
		TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, environmentKey(fixture.environment.Record.ID), false,
	)
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(fixture.task.ID), true)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		taskEnvironmentIndexKey(fixture.environment.Record.ID, fixture.task.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		taskWorkspaceTenantIndexKey(fixture.project.Record.TenantID, fixture.task.ID),
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
	policyKey := backupPolicyKey(fixture.environment.Record.ID)
	fixture.putRaw(t, policyKey, []byte("retained"))
	current, err := fixture.hierarchy.GetEnvironment(ctx, fixture.environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	fixture.environment = current
	fixture.mustBegin(t)
	intentEntry := fixture.mustGet(t, environmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := decodeEnvironmentDeletionIntent(intentEntry.Value)
	if err != nil || intent.CleanupPhase != EnvironmentDeletionCleanupEnumerating {
		t.Fatalf("initial Environment deletion cleanup intent = %#v, %v", intent, err)
	}
	transaction, err := fixture.store.Transact(
		ctx,
		[]Condition{{Key: policyKey, ModRevision: fixture.mustGet(t, policyKey).ModRevision}},
		[]Mutation{{Type: MutationDelete, Key: policyKey}},
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
	result := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	if _, err := fixture.tasks.AcknowledgeTask(
		ctx, agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
		TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
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
	if err != nil || completed.Record.CleanupPhase != EnvironmentDeletionCleanupComplete {
		t.Fatalf("CompleteEnvironmentDeletionCleanupEnumeration() = %#v, %v", completed, err)
	}
	terminal, err := fixture.tasks.AcknowledgeTask(
		ctx, agentID, 1, fixture.task.ID,
		taskAssignmentIDForTest(t, fixture.tasks, fixture.task.ID),
		TaskStatusCompleted, result, fixture.now.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(after cleanup completion) = %#v, %v", terminal, err)
	}
}

// Rationale: a terminal attempt is retry evidence, not an active cleanup
// driver, and therefore cannot advance an enumerating deletion intent.
func TestEnvironmentDeletionTerminalTaskCannotCompleteCleanupEnumeration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newEnvironmentDeletionLockFixture(t)
	policyKey := backupPolicyKey(fixture.environment.Record.ID)
	fixture.putRaw(t, policyKey, []byte("retained"))
	fixture.mustBegin(t)
	if transaction, err := fixture.store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationDelete, Key: policyKey}},
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
	entry := fixture.mustGet(t, environmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := decodeEnvironmentDeletionIntent(entry.Value)
	if err != nil || intent.CleanupPhase != EnvironmentDeletionCleanupEnumerating {
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
	if err != nil || terminal.Record.Status != TaskStatusTimedOut {
		t.Fatalf("timed-out Environment deletion = %#v, %v", terminal, err)
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, environmentOperationLockKey(fixture.environment.Record.ID), true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		deletionTombstoneKey(
			string(DeletionTargetEnvironment),
			fixture.environment.Record.ID,
		),
		true,
	)
	assertEnvironmentDeletionCompanion(
		t, fixture.store, environmentDeletionIntentKey(fixture.task.OperationID), true,
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
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if !store.raced {
		store.raced = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
			Type: MutationPut, Key: store.key, Value: store.value,
		}}); err != nil {
			return TransactionResult{}, err
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
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if !store.raced {
		for _, mutation := range mutations {
			if mutation.Type != MutationDelete || mutation.Prefix ||
				mutation.Key != environmentKey(store.environmentID) {
				continue
			}
			inserted, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
				Type: MutationPut, Key: store.childKey, Value: []byte("retained"),
			}})
			if err != nil {
				return TransactionResult{}, err
			}
			if !inserted.Succeeded {
				return TransactionResult{}, errs.New(errs.KindInternal, "child insertion race did not commit")
			}
			store.raced = true
			for _, condition := range conditions {
				if condition.Prefix && strings.HasPrefix(store.childKey, condition.Key) {
					return TransactionResult{Succeeded: false, Revision: inserted.Revision}, nil
				}
			}
			return TransactionResult{}, errs.New(
				errs.KindInternal,
				"terminal transaction did not compare the child prefix",
			)
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

type environmentDeletionLockFixture struct {
	store       *memoryHierarchyStore
	hierarchy   *HierarchyRepository
	tasks       *TaskRepository
	project     Versioned[ProjectRecord]
	environment Versioned[EnvironmentRecord]
	task        TaskRecord
	tombstone   DeletionTombstoneRecord
	marker      IdempotencyMarker
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
	if _, err := hierarchy.CreateTenant(ctx, TenantRecord{
		ID: tenantID, Slug: "deletion-tenant", Name: "Deletion Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 8001), TenantID: tenantID,
		Slug: "deletion-project", Name: "Deletion Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 8002)
	environment, err := hierarchy.CreateEnvironment(ctx, EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.Record.ID,
		Name:              "production",
		NetworkPool:       "10.248.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 8003),
		CreatedAt:         now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	owner, err := EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := validTaskRecord(now.Add(time.Minute))
	task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, 8004)
	task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, 8005)
	task.IdempotencyKey = "environment-delete-key-0001"
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.Executor = TaskExecutorAgent
	task.Type = TaskRemove
	task.Target = environment.Record.ID
	task.Params = map[string]string{TaskMaterializationEnvironmentParam: environment.Record.ID}
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: "/environments/{id}", Key: task.IdempotencyKey,
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetEnvironment, TargetID: environment.Record.ID,
		TargetRevision: environment.Revision, TaskID: task.ID,
		Phase: DeletionPhaseHostEffects, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
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
	poolRegistryValue, err := encodeEnvelope("environment_pool_registry", EnvironmentPoolRegistry{
		Reservations: map[string]string{environmentID: environment.Record.NetworkPool},
	})
	if err != nil {
		t.Fatalf("encode Environment pool registry error = %v", err)
	}
	defer clear(poolRegistryValue)
	fixture.putRaw(t, environmentPoolRegistryKey, poolRegistryValue)
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
	record BackupOperationLockRecord,
) {
	t.Helper()
	value, err := encodeBackupOperationLockRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupOperationLockRecord() error = %v", err)
	}
	defer clear(value)
	fixture.putRaw(t, environmentOperationLockKey(fixture.environment.Record.ID), value)
}

func (fixture *environmentDeletionLockFixture) rewriteEpoch(t *testing.T) {
	t.Helper()
	value := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	fixture.putRaw(t, value.Key, value.Value)
}

func (fixture *environmentDeletionLockFixture) putRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	transaction, err := fixture.store.Transact(
		context.Background(), nil, []Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("put %s = %#v, %v", key, transaction, err)
	}
}

func (fixture *environmentDeletionLockFixture) mustGet(t *testing.T, key string) *KeyValue {
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
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return TransactionResult{}, failure
	}
	return result, err
}
