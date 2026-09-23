package etcd

import (
	context "context"
	errors "errors"
	testing "testing"
	time "time"

	testbackupconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a generic idempotency marker cannot make a Config run replay
// successful after its immutable cursor or either direction of its reference
// pair is missing or rewritten at another revision.
func TestBackupRuntimeRepositoryConfigReplayRequiresExactCompanions(t *testing.T) {
	checks := []struct {
		name   string
		key    func(testbackupruntime.BackupRunRecord) string
		remove bool
	}{
		{
			name: "missing immutable reference",
			key: func(run testbackupruntime.BackupRunRecord) string {
				return testbackupconfiguration.BackupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID)
			},
			remove: true,
		},
		{
			name: "rewritten cursor",
			key: func(run testbackupruntime.BackupRunRecord) string {
				return testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID)
			},
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
			fixedRevision := configureBackupRuntimeConfigRun(t, store, &run)
			first, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run), fixedRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer first.clear()
			second, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run), fixedRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer second.clear()
			task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
			firstPlan, err := first.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			secondPlan, err := second.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			unknown := &backupRuntimeUnknownOutcomeStore{hierarchyStore: store, failNext: true}
			unknownIdempotency, err := NewIdempotencyRepository(unknown)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := unknownIdempotency.Apply(
				context.Background(), marker, firstPlan,
			); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("Apply(Config unknown outcome) error = %v", err)
			}
			key := check.key(run)
			entry := mustOptionalKey(t, store, key)
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}
			if check.remove {
				mutation = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]testkeyvalue.Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("change Config companion = %#v, %v", changed, err)
			}
			idempotency, err := NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := idempotency.Apply(
				context.Background(), marker, secondPlan,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Apply(Config corrupt replay) error = %v", err)
			}
		})
	}
}

// Rationale: unknown-outcome replay must reject a membership rewritten after
// publication even when its raw value still names the same run.
func TestBackupRuntimeRepositoryCreateRunReplayRejectsChangedMembershipRevision(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	if _, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	); err != nil {
		t.Fatalf("createBackupRunForTest() error = %v", err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, membership)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: membership, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: membership, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("rewrite run membership = %#v, %v", result, err)
	}
	if _, err := repository.GetBackupRun(
		context.Background(), run.TaskID,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("GetBackupRun(changed membership) error = %v", err)
	}
}

// Rationale: cursors are meaningful only with the fixed revision whose
// snapshot they continue.
func TestBackupRuntimeRepositoryRejectsCursorWithoutRevision(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	_, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{
			Limit:          1,
			StartExclusive: testbackupruntime.BackupRunEnvironmentPrefix + run.EnvironmentID + "/" + run.TaskID,
		},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ListBackupRunsByEnvironment(cursor without revision) error = %v", err)
	}
}

// Rationale: the full twelve-source contract must remain composable after
// primary-owned derived membership removes redundant absence compares.
func TestBackupRuntimeRepositoryPreparesTwelveSourcePublicationWithinBounds(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeBareFixture(t)
	for ordinal := uint32(1); ordinal < testbackuppolicy.MaximumBackupPolicySources; ordinal++ {
		source := testBackupLaterSource(run.CreatedAt, ordinal, testbackupruntime.BackupSourceAttemptPending)
		source.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		run.Sources = append(run.Sources, source)
	}
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		lock,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("prepareBackupRunPublication(12 sources) error = %v", err)
	}
	defer plan.clear()
	if len(plan.conditions)+len(plan.mutations) > testkeyvalue.MaximumOperations {
		t.Fatalf(
			"12-source publication operations = %d, want <= %d",
			len(plan.conditions)+len(plan.mutations), testkeyvalue.MaximumOperations,
		)
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil || len(exclusions) != testbackuppolicy.MaximumBackupPolicySources {
		t.Fatalf("12-source exclusions = %d, %v", len(exclusions), err)
	}
}

// Rationale: exclusion cleanup is an all-or-none authority transition; a
// partial set cannot be accepted as replay or opportunistically repaired.
func TestBackupRuntimeRepositoryRejectsMixedExclusionRelease(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	second := testBackupLaterSource(run.CreatedAt, 1, testbackupruntime.BackupSourceAttemptPending)
	second.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
	second.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + second.SourceID + "/" +
		second.RecoveryPointID + "/artifact.bin"
	run.Sources = append(run.Sources, second)
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed := run
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = testbackupruntime.BackupFailureCapture
	failed.Sources[1].State = testbackupruntime.BackupSourceAttemptUnstarted
	failed.Sources[1].Phase = testbackupruntime.BackupSourcePhaseCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	firstKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, firstKey)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: firstKey, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: firstKey}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("delete one exclusion = %#v, %v", result, err)
	}
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), created, failed,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(mixed) error = %v", err)
	}
	secondKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		second.TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if entry := mustOptionalKey(t, store, secondKey); entry == nil {
		t.Fatal("mixed exclusion release removed the surviving authority")
	}
}

// Rationale: terminal Task evidence, run state, exclusion release, lock release,
// and epoch advance must share one bounded transaction revision.
func TestBackupRuntimeRepositoryComposesTerminalRunAndAuthorityRelease(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed := run
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = testbackupruntime.BackupFailureCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupRunTerminal(context.Background(), created, failed)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminal() error = %v", err)
	}
	defer plan.clear()
	marker := "/v1/test/backup-terminal-tasks/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: marker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose terminal backup run = %v", err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("terminal backup transaction = %#v, %v", result, err)
	}
	exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{exclusionKey, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), marker, testhierarchy.EnvironmentMutationEpochKey(run.EnvironmentID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != result.Revision {
			t.Fatalf("terminal companion %q = %#v, want revision %d", key, entry, result.Revision)
		}
	}
}

// Rationale: successful Task completion cannot claim cleanup that was not
// acknowledged by the exact Agent assignment and checkpoint sequence.
func TestBackupRuntimeRepositoryRequiresCleanupCheckpointBeforeSuccess(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	cleanupPending := run
	cleanupPending.State = testbackupruntime.BackupRunRunning
	cleanupPending.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	cleanupPending.Sources[0].State = testbackupruntime.BackupSourceAttemptCleanupPending
	cleanupPending.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	cleanupPending.Sources[0].SizeBytes = 123
	cleanupPending.Sources[0].SHA256 = testBackupDigest
	cleanupPending.UpdatedAt = run.UpdatedAt.Add(time.Second)
	cleanupVersion, err := repository.replaceBackupRunForTest(
		context.Background(), created, cleanupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := cleanupPending
	completed.State = testbackupruntime.BackupRunCompleted
	completed.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	completed.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	completed.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), cleanupVersion, completed,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareBackupRunTerminal(uncheckpointed cleanup) error = %v", err)
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind:    testbackupruntime.BackupCheckpointSourceCleanupCompleted,
		PointID: cleanupPending.Sources[0].RecoveryPointID,
	}
	succeeded := cleanupPending
	succeeded.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	succeeded.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	succeeded.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	succeededVersion, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, cleanupVersion, succeeded,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(cleanup) error = %v", err)
	}
	completed = succeeded
	completed.State = testbackupruntime.BackupRunCompleted
	completed.UpdatedAt = succeeded.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupRunTerminal(
		context.Background(), succeededVersion, completed,
	)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminal(checkpointed cleanup) error = %v", err)
	}
	plan.clear()
}

// Rationale: Task failure or abort after artifact preparation must publish an
// upload-intent orphan before releasing source exclusions and the owned lock.
func TestBackupRuntimeRepositoryTerminalizesUploadIntentWithOrphan(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		state   testbackupruntime.BackupRunState
		failure testbackupruntime.BackupFailureCode
		phase   testbackupruntime.BackupSourceAttemptPhase
	}{
		{
			name: "failed upload", state: testbackupruntime.BackupRunFailed, failure: testbackupruntime.BackupFailureUpload,
			phase: testbackupruntime.BackupSourcePhaseUpload,
		},
		{
			name: "aborted head verification", state: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
			phase: testbackupruntime.BackupSourcePhaseHeadVerification,
		},
		{
			name: "timed out point commit", state: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut,
			phase: testbackupruntime.BackupSourcePhasePointCommit,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			created, err := repository.createBackupRunForTest(
				context.Background(),
				run,
				backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
			)
			if err != nil {
				t.Fatal(err)
			}
			staged := run
			staged.State = testbackupruntime.BackupRunRunning
			staged.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
			staged.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
			staged.Sources[0].Phase = test.phase
			staged.Sources[0].SizeBytes = 123
			staged.Sources[0].SHA256 = testBackupDigest
			staged.UpdatedAt = run.UpdatedAt.Add(time.Second)
			stagedVersion, err := repository.replaceBackupRunForTest(
				context.Background(), created, staged,
			)
			if err != nil {
				t.Fatal(err)
			}
			withoutOrphan := staged
			withoutOrphan.State = test.state
			withoutOrphan.Sources = append(
				[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
				staged.Sources...,
			)
			withoutOrphan.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
			withoutOrphan.Sources[0].FailureCode = test.failure
			withoutOrphan.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			if _, err := repository.prepareBackupRunTerminal(
				context.Background(), stagedVersion, withoutOrphan,
			); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("prepareBackupRunTerminal(without orphan) error = %v", err)
			}
			terminal := withoutOrphan
			terminal.Sources = append(
				[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
				withoutOrphan.Sources...,
			)
			terminal.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
			for _, mutate := range []struct {
				name string
				run  func(*testbackupruntime.BackupRunSourceAttemptRecord)
			}{
				{name: "empty", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SizeBytes = 0
					source.SHA256 = ""
				}},
				{name: "malformed", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SHA256 = "not-a-sha256"
				}},
				{name: "substituted", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SizeBytes++
				}},
			} {
				changed := terminal
				changed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), terminal.Sources...)
				mutate.run(&changed.Sources[0])
				if _, err := repository.prepareBackupRunTerminal(
					context.Background(), stagedVersion, changed,
				); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("prepareBackupRunTerminal(%s artifact evidence) error = %v", mutate.name, err)
				}
			}
			plan, err := repository.prepareBackupRunTerminal(
				context.Background(), stagedVersion, terminal,
			)
			if err != nil {
				t.Fatalf("prepareBackupRunTerminal(orphan) error = %v", err)
			}
			marker := "/v1/test/backup-upload-terminal/" + run.TaskID
			conditions, mutations, err := plan.composeTransaction(
				[]testkeyvalue.Condition{{Key: marker}},
				[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
			testkeyvalue.ClearMutationValues(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize upload intent = %#v, %v", result, err)
			}
			pointID := staged.Sources[0].RecoveryPointID
			connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(run.ConnectorID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(run.EnvironmentID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{testbackupruntime.BackupOrphanKey(pointID), connectorIndex, environmentIndex, marker} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != result.Revision {
					t.Fatalf("terminal orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("terminal upload intent retained its Environment lock")
			}
		})
	}
}

// Rationale: an already-persisted orphan is retained as exact cleanup
// authority while failure, abort, or timeout terminalizes the owning Task and
// releases the run lock and exclusions in the same transaction.
func TestBackupRuntimeRepositoryTerminalRetainsExactExistingOrphan(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		state       testbackupruntime.BackupRunState
		failureCode testbackupruntime.BackupFailureCode
	}{
		{name: "failed", state: testbackupruntime.BackupRunFailed, failureCode: testbackupruntime.BackupFailurePointCommit},
		{name: "aborted", state: testbackupruntime.BackupRunAborted, failureCode: testbackupruntime.BackupFailureAborted},
		{name: "timed out", state: testbackupruntime.BackupRunTimedOut, failureCode: testbackupruntime.BackupFailureTimedOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			orphaned := staged
			orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
			orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
			orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
			orphan := testbackupruntime.BackupOrphanRecord{
				Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
				CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
			}
			checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
			orphanedVersion, err := repository.CreateBackupOrphan(
				context.Background(),
				backupAssignmentFromCheckpoint(checkpoint),
				stagedVersion,
				orphaned,
				0,
				orphan,
			)
			if err != nil {
				t.Fatal(err)
			}
			storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
			if err != nil || !found {
				t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
			}
			terminal := orphaned
			terminal.State = test.state
			terminal.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
			terminal.Sources[0].FailureCode = test.failureCode
			terminal.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
			plan, err := repository.prepareBackupRunTerminal(
				context.Background(), orphanedVersion, terminal,
			)
			if err != nil {
				t.Fatalf("prepareBackupRunTerminal() error = %v", err)
			}
			marker := "/v1/test/backup-retained-orphan-terminal/" + test.name + "/" + run.TaskID
			conditions, mutations, err := plan.composeTransaction(
				[]testkeyvalue.Condition{{Key: marker}},
				[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
			testkeyvalue.ClearMutationValues(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize retained orphan = %#v, %v", result, err)
			}
			connectorIndex, _ := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
			environmentIndex, _ := testbackupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
			for _, key := range []string{testbackupruntime.BackupOrphanKey(point.ID), connectorIndex, environmentIndex} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != storedOrphan.Revision || entry.Version != 1 {
					t.Fatalf("retained orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("retained orphan terminal kept its Environment lock")
			}
			deleting := storedOrphan.Record
			deleting.State = testbackupruntime.BackupOrphanDelete
			deleting.UpdatedAt = deleting.UpdatedAt.Add(time.Second)
			transitioned, err := repository.TransitionReconciledBackupOrphan(
				context.Background(), storedOrphan, deleting,
			)
			if err != nil {
				t.Fatalf("TransitionReconciledBackupOrphan(terminal Task) error = %v", err)
			}
			if err := repository.DeleteReconciledBackupOrphan(
				context.Background(), transitioned,
			); err != nil {
				t.Fatalf("DeleteReconciledBackupOrphan(terminal Task) error = %v", err)
			}
		})
	}
}

// Rationale: Controller orphan reconciliation is durable authority in its own
// right, so generic Task/run pruning cannot prevent a verified artifact adoption.
func TestBackupRuntimeRepositoryAdoptsOrphanAfterOriginTaskPruned(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion, orphaned, 0, orphan,
	)
	if err != nil {
		t.Fatal(err)
	}
	storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
	}
	pruned, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRunKey(run.TaskID), ModRevision: orphanedVersion.Revision},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupRunKey(run.TaskID)},
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskStorageKey(run.TaskID)},
	})
	if err != nil || !pruned.Succeeded {
		t.Fatalf("prune originating Task/run = %#v, %v", pruned, err)
	}
	point.VerifiedAt = storedOrphan.Record.UpdatedAt.Add(time.Second)
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:     storedOrphan.Record.Reconciliation.RetentionKeep,
		Revision: storedOrphan.Record.Reconciliation.PolicyRevision,
		State:    testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	adopted, err := repository.AdoptReconciledBackupOrphan(
		context.Background(), storedOrphan, point, sweep,
	)
	if err != nil {
		t.Fatalf("AdoptReconciledBackupOrphan(pruned Task) error = %v", err)
	}
	if adopted.Record != point || mustOptionalKey(t, store, testbackupruntime.BackupOrphanKey(point.ID)) != nil {
		t.Fatalf("adopted orphan = %#v", adopted)
	}
	storedSweep, found, err := repository.GetBackupRetentionSweep(
		context.Background(), point.SourceID, point.ID,
	)
	if err != nil || !found || storedSweep.Record != sweep || storedSweep.Revision != adopted.Revision {
		t.Fatalf("adopted retention sweep = %#v/%v/%v", storedSweep, found, err)
	}
}

// Rationale: rewriting one existing orphan companion with identical bytes is
// corruption, not a new valid authority revision, and cannot terminalize the
// owning run.
func TestBackupRuntimeRepositoryRejectsRewrittenRetainedOrphanCompanion(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		orphaned,
		0,
		orphan,
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, _ := testbackupruntime.BackupOrphanEnvironmentIndexKey(run.EnvironmentID, point.ID)
	entry := mustOptionalKey(t, store, environmentIndex)
	rewritten, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: environmentIndex, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: entry.Value}},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite orphan companion = %#v, %v", rewritten, err)
	}
	terminal := orphaned
	terminal.State = testbackupruntime.BackupRunFailed
	terminal.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	terminal.Sources[0].FailureCode = testbackupruntime.BackupFailurePointCommit
	terminal.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), orphanedVersion, terminal,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(rewritten orphan) error = %v", err)
	}
	if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) == nil {
		t.Fatal("rewritten orphan companion released the Environment lock")
	}
}
