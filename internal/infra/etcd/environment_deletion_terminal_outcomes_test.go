package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

// Rationale: a non-successful Environment deletion must retain its exact
// operation fence and immutable cleanup intent so retry cannot resnapshot or
// expose an unlocked interval; only success may remove them.
func TestEnvironmentDeletionTerminalOutcomesCleanCompanionsAndReplay(t *testing.T) {
	t.Parallel()
	for _, terminalStatus := range []testtaskjournal.TaskStatus{testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskStatusFailed, testtaskjournal.TaskStatusAborted} {
		t.Run(string(terminalStatus), func(t *testing.T) {
			fixture := newEnvironmentDeletionLockFixture(t)
			collectionValue, encodeErr := encodeReleaseGroupEpochFixture(fixture.environment.Record.ID)
			if encodeErr != nil {
				t.Fatalf("encodeReleaseGroupEpochFixture() error = %v", encodeErr)
			}
			fixture.putRaw(
				t, testreleasegroups.ReleaseGroupCollectionEpochKey(fixture.environment.Record.ID), collectionValue,
			)
			fixture.mustBegin(t)
			agentID := ids.NewAt(ids.KindAgent, fixture.now, 8030)
			var terminal testkeyvalue.Versioned[TaskRecord]
			var err error
			if terminalStatus == testtaskjournal.TaskStatusAborted {
				terminal, err = fixture.tasks.AbortPendingTask(
					context.Background(), fixture.task.ID, fixture.now.Add(time.Second),
				)
			} else {
				if _, found, claimErr := fixture.tasks.ClaimNextTask(
					context.Background(), agentID, 1, fixture.now.Add(time.Second),
				); claimErr != nil || !found {
					t.Fatalf("ClaimNextTask() found/error = %t/%v", found, claimErr)
				}
				result := testtaskjournal.TaskResultRecord{
					Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
				}
				if terminalStatus == testtaskjournal.TaskStatusFailed {
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
			if terminalStatus == testtaskjournal.TaskStatusCompleted {
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID), false,
				)
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store, testdeletions.TombstoneKey(
						string(testdeletions.DeletionTargetEnvironment),
						fixture.environment.Record.ID,
					), false,
				)
				assertEnvironmentDeletionCompanion(
					t, fixture.store, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID), false,
				)
				assertEnvironmentDeletionCompanion(
					t, fixture.store, testhierarchy.EnvironmentKey(fixture.environment.Record.ID), false,
				)
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID), false,
				)
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					testreleasegroups.ReleaseGroupCollectionEpochKey(fixture.environment.Record.ID),
					false,
				)
			} else {
				lockValue := fixture.mustGet(
					t, testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID),
				)
				lock, decodeErr := decodeOwnedEnvironmentDeletionLock(lockValue, fixture.task)
				if decodeErr != nil || lock.OperationID != fixture.task.OperationID {
					t.Fatalf("retained Environment deletion lock = %#v, %v", lock, decodeErr)
				}
				tombstoneValue := fixture.mustGet(
					t, testdeletions.TombstoneKey(
						string(testdeletions.DeletionTargetEnvironment),
						fixture.environment.Record.ID,
					),
				)
				tombstone, decodeErr := testdeletions.DecodeDeletionTombstone(tombstoneValue.Value)
				if decodeErr != nil || tombstone.TaskID != fixture.task.ID ||
					tombstone.Checkpoint != fixture.tombstone.Checkpoint {
					t.Fatalf(
						"retained Environment deletion tombstone = %#v, %v",
						tombstone,
						decodeErr,
					)
				}
				intentValue := fixture.mustGet(
					t, testdeletions.EnvironmentDeletionIntentKey(fixture.task.OperationID),
				)
				intent, decodeErr := testdeletions.DecodeEnvironmentDeletionIntent(intentValue.Value)
				if decodeErr != nil || intent.EnvironmentID != fixture.environment.Record.ID ||
					intent.OperationID != fixture.task.OperationID ||
					intent.TargetRevision != fixture.environment.Revision {
					t.Fatalf("retained Environment deletion intent = %#v, %v", intent, decodeErr)
				}
				assertEnvironmentDeletionCompanion(
					t, fixture.store, testhierarchy.EnvironmentKey(fixture.environment.Record.ID), true,
				)
				epoch := fixture.mustGet(
					t, testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID),
				)
				if epoch.ModRevision != terminal.Revision {
					t.Fatalf(
						"terminal epoch revision = %d, want %d",
						epoch.ModRevision,
						terminal.Revision,
					)
				}
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store, testreleasegroups.ReleaseGroupCollectionEpochKey(fixture.environment.Record.ID), true,
				)
			}
			var replay testkeyvalue.Versioned[TaskRecord]
			if terminalStatus == testtaskjournal.TaskStatusAborted {
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
	result := testtaskjournal.TaskResultRecord{
		Kind:       testtaskjournal.TaskResultEnvironmentDirectory,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	terminalAt := fixture.now.Add(2 * time.Second)
	if _, err := failingTasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, failingTasks,
			fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, terminalAt); !errors.Is(err, unknown) {
		t.Fatalf("AcknowledgeTask(unknown) error = %v", err)
	}
	replay, err := fixture.tasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, fixture.tasks,
			fixture.task.ID), testtaskjournal.TaskStatusCompleted, result, terminalAt)

	if err != nil || replay.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(after unknown) = %#v, %v", replay, err)
	}
}
