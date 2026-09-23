package etcd

import (
	context "context"
	errors "errors"
	strings "strings"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
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
