package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	strings "strings"
	testing "testing"
	time "time"
)

func TestBackupTerminalReceiptBindsPriorRevisionAndFullTask(t *testing.T) {

	t.Parallel()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()

	value, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(plan.record)
	if err != nil {
		t.Fatalf("encodeBackupTerminalReceiptRecord() error = %v", err)
	}
	defer clear(value)
	decoded, err := testbackupruntime.DecodeBackupTerminalReceiptRecord(value)
	if err != nil || decoded.ReceiptDigest != plan.record.ReceiptDigest {
		t.Fatalf("decodeBackupTerminalReceiptRecord() = %#v, %v", decoded, err)
	}

	rewrittenPrior := plan.record
	rewrittenPrior.PriorTaskRevision++
	if _, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewrittenPrior); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten prior revision) succeeded")
	}
	rewrittenTask := plan.record
	rewrittenTask.Task.Actor = testtaskjournal.TaskActorOperator
	if _, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewrittenTask); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten Task) succeeded")
	}
	rewrittenDomain := plan.record
	rewrittenDomain.DomainDigest = strings.Repeat("f", 64)
	rewrittenDomain.ReceiptDigest, err = testbackupruntime.BackupTerminalReceiptDigest(rewrittenDomain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewrittenDomain); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten domain digest) succeeded")
	}
	rewrittenEpoch := plan.record
	rewrittenEpoch.EnvironmentEpochDigest = strings.Repeat("e", 64)
	rewrittenEpoch.ReceiptDigest, err = testbackupruntime.BackupTerminalReceiptDigest(rewrittenEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewrittenEpoch); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten Environment epoch) succeeded")
	}
	rewrittenPoints := plan.record
	rewrittenPoints.Points = append([]testbackupruntime.BackupPruneTerminalPointOutcome(nil), plan.record.Points...)
	rewrittenPoints.Points[0].Outcome = testbackupruntime.BackupPruneTerminalRemoved
	rewrittenPoints.ReceiptDigest, err = testbackupruntime.BackupTerminalReceiptDigest(rewrittenPoints)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewrittenPoints); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten point outcomes) succeeded")
	}
	if err := validateBackupTerminalReceiptTaskBinding(terminal, plan.record); err != nil {
		t.Fatalf("validateBackupTerminalReceiptTaskBinding() error = %v", err)
	}
	changed := terminal
	changed.Actor = testtaskjournal.TaskActorOperator
	if err := validateBackupTerminalReceiptTaskBinding(changed, plan.record); err == nil {
		t.Fatal("validateBackupTerminalReceiptTaskBinding(full Task mismatch) succeeded")
	}
}

func TestPrepareBackupPruneTerminalReceiptRecordsOrderedOutcomes(t *testing.T) {

	t.Parallel()
	terminal, dispatch, assigned, _ := backupTerminalReceiptPruneFixture(t)
	pointTwo := assigned.Record.Point
	pointTwo.ID = ids.NewAt(
		ids.KindRecoveryPoint,
		assigned.Record.Point.CreatedAt.Add(time.Millisecond),
		7301,
	)
	pointTwo.CreatedAt = assigned.Record.Point.CreatedAt.Add(time.Millisecond)
	pointTwo.ObjectKey = "production/" + pointTwo.EnvironmentID + "/" + pointTwo.SourceID + "/" +
		pointTwo.ID + "/artifact.bin"
	dispatch.RecoveryPointIDs = []string{assigned.Record.Point.ID, pointTwo.ID}
	verified := assigned
	verified.Record.State = testbackupruntime.BackupPruneVerifiedAbsent
	second := assigned
	second.Record.Point = pointTwo
	second.Revision++

	plan, err := prepareBackupPruneTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{verified, second},
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	defer plan.clear()
	if len(plan.record.Points) != 2 ||
		plan.record.Points[0].Outcome != testbackupruntime.BackupPruneTerminalRemoved ||
		plan.record.Points[1].Outcome != testbackupruntime.BackupPruneTerminalRetained ||
		plan.record.Points[0].Point.ID != assigned.Record.Point.ID ||
		plan.record.Points[1].Point.ID != pointTwo.ID {
		t.Fatalf("terminal receipt outcomes = %#v", plan.record.Points)
	}
}

func TestBackupTerminalReceiptReplayRejectsMissingTornAndReconstructedReceipts(t *testing.T) {

	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *memoryTaskStore, string)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, store *memoryTaskStore, taskID string) {
				t.Helper()
				if _, err := deleteTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupTerminalReceiptKey(taskID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "later reconstructed",
			mutate: func(t *testing.T, store *memoryTaskStore, taskID string) {
				t.Helper()
				read, err := store.Get(ctx, testbackupruntime.BackupTerminalReceiptKey(taskID))
				if err != nil || read.Entry == nil {
					t.Fatalf("Get(receipt) = %#v, %v", read, err)
				}
				value := append([]byte(nil), read.Entry.Value...)
				defer clear(value)
				if _, err := deleteTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupTerminalReceiptKey(taskID)); err != nil {
					t.Fatal(err)
				}
				if _, err := putTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupTerminalReceiptKey(taskID), value); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, task := seedBackupTerminalReceiptReplay(t)
			test.mutate(t, store, task.Record.ID)
			err := repository.validateBackupTerminalReceiptReplay(ctx, task)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("validateBackupTerminalReceiptReplay() error = %v", err)
			}
		})
	}

	t.Run("torn revision", func(t *testing.T) {
		terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
		defer plan.clear()
		store := newMemoryTaskStore()
		repository, err := newTaskRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		taskValue, err := EncodeTaskRecord(terminal)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(taskValue)
		taskRevision, err := putTerminalReceiptTestValue(
			ctx,
			store,
			testtaskjournal.TaskStorageKey(terminal.ID),
			taskValue,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := putTerminalReceiptTestValue(
			ctx,
			store, testbackupruntime.BackupTerminalReceiptKey(terminal.ID), plan.mutations[0].Value,
		); err != nil {
			t.Fatal(err)
		}
		task := testkeyvalue.Versioned[TaskRecord]{
			Record: terminal, Revision: taskRevision, ReadRevision: taskRevision,
		}
		err = repository.validateBackupTerminalReceiptReplay(ctx, task)
		if !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("validateBackupTerminalReceiptReplay(torn) error = %v", err)
		}
	})
}

func TestBackupTerminalReceiptReplayClassifiesDurableCorruptionAsInternal(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	terminal, _, _, _ := backupTerminalReceiptPruneFixture(t)
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: taskValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: []byte("corrupt"),
		},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed corrupt receipt = %#v, %v", transaction, err)
	}
	task := testkeyvalue.Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(corrupt) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplayClassifiesCallerMismatchAsStateConflict(t *testing.T) {

	t.Parallel()
	repository, _, task := seedBackupTerminalReceiptReplay(t)
	task.Record.Actor = testtaskjournal.TaskActorOperator
	err := repository.validateBackupTerminalReceiptReplay(context.Background(), task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(caller mismatch) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplaySurvivesCompactionAndLaterPointLifecycle(t *testing.T) {

	ctx := context.Background()
	for _, test := range []struct {
		name         string
		wantConflict bool
		mutate       func(*testing.T, *memoryTaskStore, testkeyvalue.Versioned[TaskRecord], testbackupruntime.BackupPruneTerminalPointOutcome)
	}{
		{
			name: "point later pruned",
			mutate: func(t *testing.T, store *memoryTaskStore, _ testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				keys, err := backupPruneAuthorityKeys(point.Point)
				if err != nil {
					t.Fatal(err)
				}
				mutations := make([]testkeyvalue.Mutation, len(keys))
				for index, key := range keys {
					mutations[index] = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
				}
				if transaction, err := store.Transact(ctx, nil, mutations); err != nil ||
					!transaction.Succeeded {
					t.Fatal(err)
				}
			},
		},
		{
			name: "successor completed",
			mutate: func(t *testing.T, store *memoryTaskStore, _ testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, testbackupruntime.BackupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				successor := testbackupruntime.BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   ids.NewAt(ids.KindOperation, point.CreatedAt, 7401),
					State:         testbackupruntime.BackupPruneAssigned,
					TaskID:        ids.NewAt(ids.KindTask, point.CreatedAt, 7402),
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     point.CreatedAt.Add(time.Hour),
				}
				value, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(successor)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupRecoveryPointPruneKey(point.Point.ID), value); err != nil {
					t.Fatal(err)
				}
				keys, err := backupPruneAuthorityKeys(point.Point)
				if err != nil {
					t.Fatal(err)
				}
				mutations := make([]testkeyvalue.Mutation, len(keys))
				for index, key := range keys {
					mutations[index] = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
				}
				if transaction, err := store.Transact(ctx, nil, mutations); err != nil ||
					!transaction.Succeeded {
					t.Fatal(err)
				}
			},
		},
		{
			name: "same-operation successor assigned",
			mutate: func(t *testing.T, store *memoryTaskStore, task testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, testbackupruntime.BackupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				successorAt := point.CreatedAt.Add(time.Hour)
				successorTaskID := ids.NewAt(ids.KindTask, successorAt, 7403)
				successor := testbackupruntime.BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   task.Record.OperationID,
					State:         testbackupruntime.BackupPruneAssigned,
					TaskID:        successorTaskID,
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     successorAt,
				}
				pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(successor)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(pruneValue)
				dispatchValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
					TaskID: successorTaskID, OperationID: task.Record.OperationID,
					EnvironmentID:    point.Point.EnvironmentID,
					RecoveryPointIDs: []string{point.Point.ID}, CreatedAt: successorAt,
				},
				)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(dispatchValue)
				lockValue, err := testbackupruntime.EncodeBackupOperationLockRecord(testbackupruntime.BackupOperationLockRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: task.Record.OperationID,
					TaskID: successorTaskID, Kind: testbackupruntime.BackupOperationPrune,
					CreatedAt: successorAt, UpdatedAt: successorAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(lockValue)
				epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(testbackupruntime.EnvironmentMutationEpochRecord{
					EnvironmentID: point.Point.EnvironmentID,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(epochValue)
				transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneKey(point.Point.ID), Value: pruneValue},
					{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(successorTaskID), Value: dispatchValue},
					{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentOperationLockKey(point.Point.EnvironmentID), Value: lockValue},
					{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(point.Point.EnvironmentID), Value: epochValue},
				})
				if err != nil || !transaction.Succeeded {
					t.Fatalf("seed same-operation prune successor = %#v, %v", transaction, err)
				}
			},
		},
		{
			name: "same-operation successor retained again",
			mutate: func(t *testing.T, store *memoryTaskStore, task testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				pruneRead, err := store.Get(ctx, testbackupruntime.BackupRecoveryPointPruneKey(point.Point.ID))
				if err != nil || pruneRead.Entry == nil {
					t.Fatalf("Get(prune) = %#v, %v", pruneRead, err)
				}
				retained, err := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(pruneRead.Entry.Value)
				if err != nil {
					t.Fatal(err)
				}
				retained.OperationID = task.Record.OperationID
				retained.UpdatedAt = retained.UpdatedAt.Add(time.Hour)
				value, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(retained)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(
					ctx, store, testbackupruntime.BackupRecoveryPointPruneKey(point.Point.ID), value,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:         "fabricated Environment deletion adoption",
			wantConflict: true,
			mutate: func(t *testing.T, store *memoryTaskStore, _ testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, testbackupruntime.BackupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				adopted := testbackupruntime.BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   ids.NewAt(ids.KindOperation, point.CreatedAt, 7411),
					State:         testbackupruntime.BackupPruneAssigned,
					TaskID:        ids.NewAt(ids.KindTask, point.CreatedAt, 7412),
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     point.CreatedAt.Add(2 * time.Hour),
				}
				value, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(adopted)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupRecoveryPointPruneKey(point.Point.ID), value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Environment deletion adopted",
			mutate: func(t *testing.T, store *memoryTaskStore, _ testkeyvalue.Versioned[TaskRecord], point testbackupruntime.BackupPruneTerminalPointOutcome) {
				t.Helper()
				environmentRead, err := store.Get(ctx, testhierarchy.EnvironmentKey(point.Point.EnvironmentID))
				if err != nil || environmentRead.Entry == nil {
					t.Fatalf("Get(Environment) = %#v, %v", environmentRead, err)
				}
				deletionAt := point.CreatedAt.Add(3 * time.Hour)
				operationID := ids.NewAt(ids.KindOperation, deletionAt, 7421)
				taskID := ids.NewAt(ids.KindTask, deletionAt, 7422)
				lockValue, err := testbackupruntime.EncodeBackupOperationLockRecord(testbackupruntime.BackupOperationLockRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: operationID,
					TaskID: taskID, Kind: testbackupruntime.BackupOperationDeletion,
					CreatedAt: deletionAt, UpdatedAt: deletionAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(lockValue)
				tombstone := testdeletions.DeletionTombstoneRecord{
					TargetKind: testdeletions.DeletionTargetEnvironment, TargetID: point.Point.EnvironmentID,
					TargetRevision: environmentRead.Entry.ModRevision, TaskID: taskID,
					Phase: testdeletions.DeletionPhaseHostEffects, CreatedAt: deletionAt, UpdatedAt: deletionAt,
				}
				tombstoneValue, err := testdeletions.EncodeDeletionTombstone(tombstone)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(tombstoneValue)
				intentValue, err := testdeletions.EncodeEnvironmentDeletionIntent(testdeletions.EnvironmentDeletionIntentRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: operationID, TaskID: taskID,
					TargetRevision: tombstone.TargetRevision,
					CleanupPhase:   testdeletions.EnvironmentDeletionCleanupEnumerating, CreatedAt: deletionAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(intentValue)
				epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(testbackupruntime.EnvironmentMutationEpochRecord{
					EnvironmentID: point.Point.EnvironmentID,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(epochValue)
				transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentOperationLockKey(point.Point.EnvironmentID), Value: lockValue},
					{Type: testkeyvalue.MutationPut, Key: testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEnvironment), point.Point.EnvironmentID), Value: tombstoneValue},
					{Type: testkeyvalue.MutationPut, Key: testdeletions.EnvironmentDeletionIntentKey(operationID), Value: intentValue},
					{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(point.Point.EnvironmentID), Value: epochValue},
				})
				if err != nil || !transaction.Succeeded {
					t.Fatalf("seed Environment deletion adoption = %#v, %v", transaction, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, task := seedBackupTerminalReceiptReplay(t)
			receiptRead, err := store.Get(ctx, testbackupruntime.BackupTerminalReceiptKey(task.Record.ID))
			if err != nil || receiptRead.Entry == nil {
				t.Fatalf("Get(receipt) = %#v, %v", receiptRead, err)
			}
			receipt, err := testbackupruntime.DecodeBackupTerminalReceiptRecord(receiptRead.Entry.Value)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, store, task, receipt.Points[0])
			compacted := &compactedTaskStore{memoryTaskStore: store}
			compacted.compact(task.Revision)
			repository.store = compacted
			err = repository.validateBackupTerminalReceiptReplay(ctx, task)
			if test.wantConflict {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("validateBackupTerminalReceiptReplay() error = %v", err)
				}
			} else if err != nil {
				t.Fatalf("validateBackupTerminalReceiptReplay() error = %v", err)
			}
			if compacted.historicalReads != 0 {
				t.Fatalf("terminal replay used %d historical reads after compaction", compacted.historicalReads)
			}
		})
	}
}

func TestRemovedBackupTerminalPointRejectsReconstructedAuthority(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	terminal, dispatch, verified, initialPlan := backupTerminalReceiptPruneFixture(t)
	initialPlan.clear()
	verified.Record.State = testbackupruntime.BackupPruneVerifiedAbsent
	plan, err := prepareBackupPruneTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{verified},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: taskValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed removed terminal receipt = %#v, %v", transaction, err)
	}
	task := testkeyvalue.Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(absent point) error = %v", err)
	}

	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: verified.Record.Point,
		VerifiedAt:                  verified.Record.CreatedAt,
	}
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupRecoveryPointKey(point.ID), pointValue,
	); err != nil {
		t.Fatal(err)
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(torn point) error = %v", err)
	}
	if _, err := deleteTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupRecoveryPointKey(point.ID),
	); err != nil {
		t.Fatal(err)
	}
	reconstructed := verified.Record
	reconstructed.State = testbackupruntime.BackupPrunePending
	reconstructed.TaskID = ""
	reconstructed.UpdatedAt = terminal.FinishedAt.Add(time.Hour)
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(reconstructed)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupRecoveryPointPruneKey(point.ID), pruneValue,
	); err != nil {
		t.Fatal(err)
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(reconstructed prune) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplaySurvivesLaterOrphanReconciliationAndEnvironmentCascade(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	now := taskJournalTime().Add(3 * time.Hour)
	run := testBackupRun(now, now.Add(2*time.Second), newTestBackupRecipient(t))
	current := validTaskRecord(now)
	current.ID = run.TaskID
	current.OperationID = run.OperationID
	current.Owner = testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspacePlatform,
		ProjectID:     ids.NewAt(ids.KindProject, now, 7451),
		EnvironmentID: run.EnvironmentID,
	}
	current.Actor = testtaskjournal.TaskActorOperator
	current.Executor = testtaskjournal.TaskExecutorAgent
	current.Type = testtaskjournal.TaskBackup
	current.Target = run.EnvironmentID
	current.RenderGeneration = 0
	current.Params = nil
	current.Materializations = nil
	current.Steps = nil
	current.TimeoutSeconds = backupTaskTimeoutSeconds
	startedAt := now.Add(time.Second)
	current.Status = testtaskjournal.TaskStatusRunning
	current.StartedAt = &startedAt
	current.UpdatedAt = startedAt
	terminal, err := TransitionTaskStatus(
		current, testtaskjournal.TaskStatusRunning, testtaskjournal.TaskStatusCompleted, run.UpdatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.Result = &testtaskjournal.TaskResultRecord{
		Kind:       testtaskjournal.TaskResultCompose,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	terminal.TerminalAssignment = &testtaskjournal.TaskTerminalAssignmentRecord{
		AssignmentID:    ids.NewAt(ids.KindAssignment, now, 7452),
		AgentID:         ids.NewAt(ids.KindAgent, now, 7453),
		AgentGeneration: 1,
	}
	plan, err := prepareBackupRunTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: current, Revision: 19},
		terminal,
		run,
	)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminalReceipt() error = %v", err)
	}
	defer plan.clear()
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project := testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 7454), Slug: "backup-owner", Name: "Backup Owner",
		Kind: testhierarchy.ProjectKindBacking,
	}
	environment, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		project,
		run.EnvironmentID,
		"backup-owner",
		"10.199.0.0/24",
		terminal.ID,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentValue, err := testhierarchy.EncodeEnvironment(environment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store, testhierarchy.EnvironmentKey(environment.ID), environmentValue,
	); err != nil {
		t.Fatal(err)
	}
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	runValue, err := testbackupruntime.EncodeBackupRunRecord(run)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runValue)
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environment.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(epochValue)
	membershipKey, err := testbackupruntime.BackupRunEnvironmentIndexKey(environment.ID, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: taskValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(terminal.ID), Value: runValue},
		{Type: testkeyvalue.MutationPut, Key: membershipKey, Value: []byte(terminal.ID)},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environment.ID),
			Value: epochValue,
		},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed terminal Backup receipt = %#v, %v", transaction, err)
	}
	task := testkeyvalue.Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	point := backupRuntimeTestPoint(run, run.Sources[0], run.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point:  point.BackupRecoveryPointSnapshot,
		TaskID: run.TaskID,
		Reconciliation: testbackupruntime.BackupOrphanReconciliationAuthority{
			OperationID: run.OperationID, PolicyRevision: run.PolicyRevision,
			RetentionKeep: run.RetentionKeep,
		},
		State: testbackupruntime.BackupOrphanInspect, CreatedAt: run.UpdatedAt, UpdatedAt: run.UpdatedAt,
	}
	orphanValue, err := testbackupruntime.EncodeBackupOrphanRecord(orphan)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(orphanValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupOrphanKey(point.ID), orphanValue,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupOrphanKey(point.ID),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(reconciled orphan) error = %v", err)
	}
	unchangedRun, err := store.Get(ctx, testbackupruntime.BackupRunKey(run.TaskID))
	if err != nil || unchangedRun.Entry == nil || unchangedRun.Entry.ModRevision != task.Revision {
		t.Fatalf("terminal run after orphan reconciliation = %#v, %v", unchangedRun, err)
	}
	if _, err := putTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupRunKey(run.TaskID), runValue); err != nil {
		t.Fatal(err)
	}
	compacted := &compactedTaskStore{memoryTaskStore: store}
	compacted.compact(task.Revision)
	repository.store = compacted
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err == nil {
		t.Fatal("validateBackupTerminalReceiptReplay(identical run re-put) succeeded")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("validateBackupTerminalReceiptReplay(identical run re-put) error = %v", err)
	}
	if compacted.historicalReads != 0 {
		t.Fatalf("terminal run replay used %d historical reads", compacted.historicalReads)
	}
	repository.store = store
	if _, err := deleteTerminalReceiptTestValue(ctx, store, testbackupruntime.BackupRunKey(run.TaskID)); err != nil {
		t.Fatal(err)
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err == nil {
		t.Fatal("validateBackupTerminalReceiptReplay(torn owner cascade) succeeded")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("validateBackupTerminalReceiptReplay(torn owner cascade) error = %v", err)
	}
	if _, err := deleteTerminalReceiptTestValue(ctx, store, membershipKey); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTerminalReceiptTestValue(
		ctx,
		store, testhierarchy.EnvironmentMutationEpochKey(environment.ID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTerminalReceiptTestValue(ctx, store, testhierarchy.EnvironmentKey(environment.ID)); err != nil {
		t.Fatal(err)
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(completed owner cascade) error = %v", err)
	}
}

func TestBackupTerminalReceiptPruneCompanionRequiresAtomicRevisionAndOlderPrior(t *testing.T) {

	t.Parallel()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	key := testbackupruntime.BackupTerminalReceiptKey(terminal.ID)
	entry := &testkeyvalue.KeyValue{Key: key, Value: plan.mutations[0].Value, ModRevision: 20}
	companion, err := prepareBackupTerminalReceiptPruneCompanion(terminal, 20, entry)
	if err != nil || companion.revision != 20 {
		t.Fatalf("prepareBackupTerminalReceiptPruneCompanion() = %#v, %v", companion, err)
	}
	torn := *entry
	torn.ModRevision = 21
	if _, err := prepareBackupTerminalReceiptPruneCompanion(terminal, 20, &torn); err == nil {
		t.Fatal("prepareBackupTerminalReceiptPruneCompanion(torn) succeeded")
	}
	rewritten := plan.record
	rewritten.PriorTaskRevision = 20
	rewritten.ReceiptDigest, err = testbackupruntime.BackupTerminalReceiptDigest(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	value, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	if _, err := prepareBackupTerminalReceiptPruneCompanion(
		terminal,
		20,
		&testkeyvalue.KeyValue{Key: key, Value: value, ModRevision: 20},
	); err == nil {
		t.Fatal("prepareBackupTerminalReceiptPruneCompanion(rewritten prior) succeeded")
	}
}

func TestBackupTerminalReceiptPruneFinalizationResumesAfterPrimaryDeletion(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	store := &taskPruneOperationStore{memoryTaskStore: newMemoryTaskStore(), trackPruning: true}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, taskJournalTime(), 7501)
	receiptRevision, err := putTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupTerminalReceiptKey(taskID), []byte("retained-proof"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := testtaskjournal.PruneIntent{
		TaskID: taskID, TaskRevision: receiptRevision,
		BackupCheckpointCursorsComplete:        true,
		BackupCheckpointDeduplicationsComplete: true,
		TaskPrimaryDeleted:                     true,
		BackupTerminalReceiptRevision:          receiptRevision,
	}
	intentValue, err := testtaskjournal.EncodePruneIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(intentValue)
	intentRevision, err := putTerminalReceiptTestValue(
		ctx,
		store, testtaskjournal.TaskPruneIntentKey(taskID), intentValue,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.finishTaskPruneIntent(ctx, testkeyvalue.Versioned[testtaskjournal.PruneIntent]{
		Record: intent, Revision: intentRevision, ReadRevision: intentRevision,
	}); err != nil {
		t.Fatalf("finishTaskPruneIntent() error = %v", err)
	}
	if store.maximumOperations > testkeyvalue.MaximumOperations {
		t.Fatalf("receipt prune finalization operations = %d", store.maximumOperations)
	}
	for _, key := range []string{testbackupruntime.BackupTerminalReceiptKey(taskID), testtaskjournal.TaskPruneIntentKey(taskID)} {
		read, err := store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("Get(%s) = %#v, %v", key, read, err)
		}
	}
}

func TestBackupTerminalReceiptDuplicateWriterLosesTaskCAS(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	value, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision - 1
	prior, err := putTerminalReceiptTestValue(ctx, store, testtaskjournal.TaskStorageKey(terminal.ID), value)
	if err != nil || prior != plan.record.PriorTaskRevision {
		t.Fatalf("seed prior Task revision = %d, %v", prior, err)
	}
	conditions := []testkeyvalue.Condition{{Key: testtaskjournal.TaskStorageKey(terminal.ID), ModRevision: prior}}
	mutations := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: value},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
	}
	first, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !first.Succeeded {
		t.Fatalf("first terminal writer = %#v, %v", first, err)
	}
	second, err := store.Transact(ctx, conditions, mutations)
	if err != nil || second.Succeeded {
		t.Fatalf("duplicate terminal writer = %#v, %v", second, err)
	}
	receiptRead, err := store.Get(ctx, testbackupruntime.BackupTerminalReceiptKey(terminal.ID))
	if err != nil || receiptRead.Entry == nil || receiptRead.Entry.Version != 1 ||
		receiptRead.Entry.ModRevision != first.Revision {
		t.Fatalf("terminal receipt after duplicate = %#v, %v", receiptRead, err)
	}
}

func backupTerminalReceiptPruneFixture(
	t *testing.T,
) (
	TaskRecord, testbackupruntime.BackupRecoveryPointPruneDispatchRecord, testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	backupTerminalReceiptPlan,
) {
	t.Helper()
	now := taskJournalTime().Add(40 * time.Minute)
	point := testBackupVolumePoint(now, newTestBackupRecipient(t))
	operationID := ids.NewAt(ids.KindOperation, now, 7201)
	taskID := ids.NewAt(ids.KindTask, now, 7202)
	owner := testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspacePlatform,
		ProjectID:     ids.NewAt(ids.KindProject, now, 7203),
		EnvironmentID: point.EnvironmentID,
	}
	current := validTaskRecord(now)
	current.ID = taskID
	current.OperationID = operationID
	current.Owner = owner
	current.Actor = testtaskjournal.TaskActorSystem
	current.Executor = testtaskjournal.TaskExecutorAgent
	current.Type = testtaskjournal.TaskBackupPrune
	current.Target = point.EnvironmentID
	current.RenderGeneration = 0
	current.Params = nil
	current.Materializations = nil
	current.Steps = nil
	current.TimeoutSeconds = backupTaskTimeoutSeconds
	startedAt := now.Add(time.Second)
	current.Status = testtaskjournal.TaskStatusRunning
	current.StartedAt = &startedAt
	current.UpdatedAt = startedAt
	terminalAt := startedAt.Add(time.Second)
	terminal, err := TransitionTaskStatus(
		current, testtaskjournal.TaskStatusRunning, testtaskjournal.TaskStatusFailed, terminalAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.Result = &testtaskjournal.TaskResultRecord{
		Kind:       testtaskjournal.TaskResultCompose,
		ExitCode:   1,
		Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
	}
	terminal.TerminalAssignment = &testtaskjournal.TaskTerminalAssignmentRecord{
		AssignmentID:    ids.NewAt(ids.KindAssignment, now, 7204),
		AgentID:         ids.NewAt(ids.KindAgent, now, 7205),
		AgentGeneration: 1,
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID:           taskID,
		OperationID:      operationID,
		EnvironmentID:    point.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        now,
	}
	assigned := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
		Record: testbackupruntime.BackupRecoveryPointPruneRecord{
			Point:         point,
			PointRevision: 19,
			OperationID:   operationID,
			State:         testbackupruntime.BackupPruneAssigned,
			TaskID:        taskID,
			CreatedAt:     now,
			UpdatedAt:     startedAt,
		},
		Revision: 18,
	}
	plan, err := prepareBackupPruneTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: current, Revision: 19},
		terminal,
		dispatch,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{assigned},
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	return terminal, dispatch, assigned, plan
}

func seedBackupTerminalReceiptReplay(
	t *testing.T,
) (*TaskRepository, *memoryTaskStore, testkeyvalue.Versioned[TaskRecord]) {
	t.Helper()
	ctx := context.Background()
	terminal, _, assigned, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	pending := assigned.Record
	pending.State = testbackupruntime.BackupPrunePending
	pending.TaskID = ""
	pending.UpdatedAt = *terminal.FinishedAt
	pendingValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(pending)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pendingValue)
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: pending.Point,
		VerifiedAt:                  pending.CreatedAt,
	}
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(
		point.EnvironmentID,
		point.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	project := testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, pending.CreatedAt, 7701), Slug: "receipt-owner",
		Name: "Receipt Owner", Kind: testhierarchy.ProjectKindBacking,
	}
	environment, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		project,
		point.EnvironmentID,
		"receipt-owner",
		"10.198.0.0/24",
		terminal.ID,
		pending.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentValue, err := testhierarchy.EncodeEnvironment(environment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environment.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(epochValue)
	store.revision = plan.record.PriorTaskRevision - 1
	pointTransaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environment.ID), Value: environmentValue},
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointTransaction.Succeeded ||
		pointTransaction.Revision != plan.record.PriorTaskRevision {
		t.Fatalf("seed retained point authority = %#v, %v", pointTransaction, err)
	}
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: taskValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupRecoveryPointPruneKey(pending.Point.ID),
			Value: pendingValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environment.ID),
			Value: epochValue,
		},
	})
	if err != nil || !transaction.Succeeded || transaction.Revision <= plan.record.PriorTaskRevision {
		t.Fatalf("seed terminal receipt = %#v, %v", transaction, err)
	}
	return repository, store, testkeyvalue.Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}
}

type compactedTaskStore struct {
	*memoryTaskStore
	compactedRevision int64
	historicalReads   int
}

func (store *compactedTaskStore) compact(revision int64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, versions := range store.history {
		anchor := -1
		for index := range versions {
			if versions[index].revision <= revision {
				anchor = index
			}
		}
		if anchor < 0 {
			continue
		}
		compacted := make([]memoryTaskVersion, 0, len(versions)-anchor)
		compacted = append(compacted, versions[anchor])
		compacted = append(compacted, versions[anchor+1:]...)
		store.history[key] = compacted
	}
	store.compactedRevision = revision
}

type terminalReceiptTestTransactor interface {
	Transact(context.Context, []testkeyvalue.Condition, []testkeyvalue.Mutation) (testkeyvalue.TransactionResult, error)
}

func putTerminalReceiptTestValue(
	ctx context.Context,
	store terminalReceiptTestTransactor,
	key string,
	value []byte,
) (int64, error) {
	transaction, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return 0, err
	}
	if !transaction.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "test value put changed")
	}
	return transaction.Revision, nil
}

func deleteTerminalReceiptTestValue(
	ctx context.Context,
	store terminalReceiptTestTransactor,
	key string,
) (int64, error) {
	transaction, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}},
	)
	if err != nil {
		return 0, err
	}
	if !transaction.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "test value delete changed")
	}
	return transaction.Revision, nil
}

func (store *compactedTaskStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.GetMany(ctx, request)
}

func (store *compactedTaskStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.Range(ctx, request)
}
