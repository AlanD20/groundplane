package etcd

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBackupTerminalReceiptBindsPriorRevisionAndFullTask(t *testing.T) {
	// Rationale: the canonical receipt digest must make prior-revision and full
	// terminal Task evidence immutable rather than trusting selected fields.
	t.Parallel()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()

	value, err := encodeBackupTerminalReceiptRecord(plan.record)
	if err != nil {
		t.Fatalf("encodeBackupTerminalReceiptRecord() error = %v", err)
	}
	defer clear(value)
	decoded, err := decodeBackupTerminalReceiptRecord(value)
	if err != nil || decoded.ReceiptDigest != plan.record.ReceiptDigest {
		t.Fatalf("decodeBackupTerminalReceiptRecord() = %#v, %v", decoded, err)
	}

	rewrittenPrior := plan.record
	rewrittenPrior.PriorTaskRevision++
	if _, err := encodeBackupTerminalReceiptRecord(rewrittenPrior); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten prior revision) succeeded")
	}
	rewrittenTask := plan.record
	rewrittenTask.Task.Actor = TaskActorOperator
	if _, err := encodeBackupTerminalReceiptRecord(rewrittenTask); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten Task) succeeded")
	}
	rewrittenDomain := plan.record
	rewrittenDomain.DomainDigest = strings.Repeat("f", 64)
	rewrittenDomain.ReceiptDigest, err = backupTerminalReceiptDigest(rewrittenDomain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeBackupTerminalReceiptRecord(rewrittenDomain); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten domain digest) succeeded")
	}
	rewrittenEpoch := plan.record
	rewrittenEpoch.EnvironmentEpochDigest = strings.Repeat("e", 64)
	rewrittenEpoch.ReceiptDigest, err = backupTerminalReceiptDigest(rewrittenEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeBackupTerminalReceiptRecord(rewrittenEpoch); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten Environment epoch) succeeded")
	}
	rewrittenPoints := plan.record
	rewrittenPoints.Points = append([]BackupPruneTerminalPointOutcome(nil), plan.record.Points...)
	rewrittenPoints.Points[0].Outcome = BackupPruneTerminalRemoved
	rewrittenPoints.ReceiptDigest, err = backupTerminalReceiptDigest(rewrittenPoints)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeBackupTerminalReceiptRecord(rewrittenPoints); err == nil {
		t.Fatal("encodeBackupTerminalReceiptRecord(rewritten point outcomes) succeeded")
	}
	if err := validateBackupTerminalReceiptTaskBinding(terminal, plan.record); err != nil {
		t.Fatalf("validateBackupTerminalReceiptTaskBinding() error = %v", err)
	}
	changed := terminal
	changed.Actor = TaskActorOperator
	if err := validateBackupTerminalReceiptTaskBinding(changed, plan.record); err == nil {
		t.Fatal("validateBackupTerminalReceiptTaskBinding(full Task mismatch) succeeded")
	}
}

func TestPrepareBackupPruneTerminalReceiptRecordsOrderedOutcomes(t *testing.T) {
	// Rationale: a mixed terminal failure records removed and retained point
	// outcomes in dispatch order, without depending on their later authority.
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
	verified.Record.State = BackupPruneVerifiedAbsent
	second := assigned
	second.Record.Point = pointTwo
	second.Revision++

	plan, err := prepareBackupPruneTerminalReceipt(
		Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		[]Versioned[BackupRecoveryPointPruneRecord]{verified, second},
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	defer plan.clear()
	if len(plan.record.Points) != 2 ||
		plan.record.Points[0].Outcome != BackupPruneTerminalRemoved ||
		plan.record.Points[1].Outcome != BackupPruneTerminalRetained ||
		plan.record.Points[0].Point.ID != assigned.Record.Point.ID ||
		plan.record.Points[1].Point.ID != pointTwo.ID {
		t.Fatalf("terminal receipt outcomes = %#v", plan.record.Points)
	}
}

func TestBackupTerminalReceiptReplayRejectsMissingTornAndReconstructedReceipts(t *testing.T) {
	// Rationale: only a receipt created at the terminal Task revision proves
	// atomicity; absence or a semantically identical later reconstruction does not.
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *memoryTaskStore, string)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, store *memoryTaskStore, taskID string) {
				t.Helper()
				if _, err := deleteTerminalReceiptTestValue(ctx, store, backupTerminalReceiptKey(taskID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "later reconstructed",
			mutate: func(t *testing.T, store *memoryTaskStore, taskID string) {
				t.Helper()
				read, err := store.Get(ctx, backupTerminalReceiptKey(taskID))
				if err != nil || read.Entry == nil {
					t.Fatalf("Get(receipt) = %#v, %v", read, err)
				}
				value := append([]byte(nil), read.Entry.Value...)
				defer clear(value)
				if _, err := deleteTerminalReceiptTestValue(ctx, store, backupTerminalReceiptKey(taskID)); err != nil {
					t.Fatal(err)
				}
				if _, err := putTerminalReceiptTestValue(ctx, store, backupTerminalReceiptKey(taskID), value); err != nil {
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
		taskValue, err := encodeTaskRecord(terminal)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(taskValue)
		taskRevision, err := putTerminalReceiptTestValue(ctx, store, taskKey(terminal.ID), taskValue)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := putTerminalReceiptTestValue(
			ctx,
			store,
			backupTerminalReceiptKey(terminal.ID),
			plan.mutations[0].Value,
		); err != nil {
			t.Fatal(err)
		}
		task := Versioned[TaskRecord]{
			Record: terminal, Revision: taskRevision, ReadRevision: taskRevision,
		}
		err = repository.validateBackupTerminalReceiptReplay(ctx, task)
		if !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("validateBackupTerminalReceiptReplay(torn) error = %v", err)
		}
	})
}

func TestBackupTerminalReceiptReplayClassifiesDurableCorruptionAsInternal(t *testing.T) {
	// Rationale: malformed durable receipt bytes are storage corruption, while a
	// valid but nonmatching caller acknowledgement remains a StateConflict.
	t.Parallel()
	ctx := context.Background()
	terminal, _, _, _ := backupTerminalReceiptPruneFixture(t)
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskValue, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	transaction, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: taskKey(terminal.ID), Value: taskValue},
		{Type: MutationPut, Key: backupTerminalReceiptKey(terminal.ID), Value: []byte("corrupt")},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed corrupt receipt = %#v, %v", transaction, err)
	}
	task := Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(corrupt) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplayClassifiesCallerMismatchAsStateConflict(t *testing.T) {
	// Rationale: a different acknowledgement is a caller conflict, not durable
	// corruption, even though the stored terminal Task and receipt remain valid.
	t.Parallel()
	repository, _, task := seedBackupTerminalReceiptReplay(t)
	task.Record.Actor = TaskActorOperator
	err := repository.validateBackupTerminalReceiptReplay(context.Background(), task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(caller mismatch) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplaySurvivesCompactionAndLaterPointLifecycle(t *testing.T) {
	// Rationale: replay reads the current Task and receipt rather than terminal
	// MVCC history, and later successor, adoption, reconciliation, or pruning
	// may replace or remove point authority without invalidating the receipt.
	ctx := context.Background()
	for _, test := range []struct {
		name         string
		wantConflict bool
		mutate       func(*testing.T, *memoryTaskStore, Versioned[TaskRecord], BackupPruneTerminalPointOutcome)
	}{
		{
			name: "point later pruned",
			mutate: func(t *testing.T, store *memoryTaskStore, _ Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				keys, err := backupPruneAuthorityKeys(point.Point)
				if err != nil {
					t.Fatal(err)
				}
				mutations := make([]Mutation, len(keys))
				for index, key := range keys {
					mutations[index] = Mutation{Type: MutationDelete, Key: key}
				}
				if transaction, err := store.Transact(ctx, nil, mutations); err != nil ||
					!transaction.Succeeded {
					t.Fatal(err)
				}
			},
		},
		{
			name: "successor completed",
			mutate: func(t *testing.T, store *memoryTaskStore, _ Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, backupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				successor := BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   ids.NewAt(ids.KindOperation, point.CreatedAt, 7401),
					State:         BackupPruneAssigned,
					TaskID:        ids.NewAt(ids.KindTask, point.CreatedAt, 7402),
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     point.CreatedAt.Add(time.Hour),
				}
				value, err := encodeBackupRecoveryPointPruneRecord(successor)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(ctx, store, backupRecoveryPointPruneKey(point.Point.ID), value); err != nil {
					t.Fatal(err)
				}
				keys, err := backupPruneAuthorityKeys(point.Point)
				if err != nil {
					t.Fatal(err)
				}
				mutations := make([]Mutation, len(keys))
				for index, key := range keys {
					mutations[index] = Mutation{Type: MutationDelete, Key: key}
				}
				if transaction, err := store.Transact(ctx, nil, mutations); err != nil ||
					!transaction.Succeeded {
					t.Fatal(err)
				}
			},
		},
		{
			name: "same-operation successor assigned",
			mutate: func(t *testing.T, store *memoryTaskStore, task Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, backupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				successorAt := point.CreatedAt.Add(time.Hour)
				successorTaskID := ids.NewAt(ids.KindTask, successorAt, 7403)
				successor := BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   task.Record.OperationID,
					State:         BackupPruneAssigned,
					TaskID:        successorTaskID,
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     successorAt,
				}
				pruneValue, err := encodeBackupRecoveryPointPruneRecord(successor)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(pruneValue)
				dispatchValue, err := encodeBackupRecoveryPointPruneDispatchRecord(
					BackupRecoveryPointPruneDispatchRecord{
						TaskID: successorTaskID, OperationID: task.Record.OperationID,
						EnvironmentID:    point.Point.EnvironmentID,
						RecoveryPointIDs: []string{point.Point.ID}, CreatedAt: successorAt,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(dispatchValue)
				lockValue, err := encodeBackupOperationLockRecord(BackupOperationLockRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: task.Record.OperationID,
					TaskID: successorTaskID, Kind: BackupOperationPrune,
					CreatedAt: successorAt, UpdatedAt: successorAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(lockValue)
				epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
					EnvironmentID: point.Point.EnvironmentID,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(epochValue)
				transaction, err := store.Transact(ctx, nil, []Mutation{
					{Type: MutationPut, Key: backupRecoveryPointPruneKey(point.Point.ID), Value: pruneValue},
					{Type: MutationPut, Key: backupRecoveryPointPruneDispatchKey(successorTaskID), Value: dispatchValue},
					{Type: MutationPut, Key: environmentOperationLockKey(point.Point.EnvironmentID), Value: lockValue},
					{Type: MutationPut, Key: environmentMutationEpochKey(point.Point.EnvironmentID), Value: epochValue},
				})
				if err != nil || !transaction.Succeeded {
					t.Fatalf("seed same-operation prune successor = %#v, %v", transaction, err)
				}
			},
		},
		{
			name: "same-operation successor retained again",
			mutate: func(t *testing.T, store *memoryTaskStore, task Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				pruneRead, err := store.Get(ctx, backupRecoveryPointPruneKey(point.Point.ID))
				if err != nil || pruneRead.Entry == nil {
					t.Fatalf("Get(prune) = %#v, %v", pruneRead, err)
				}
				retained, err := decodeBackupRecoveryPointPruneRecord(pruneRead.Entry.Value)
				if err != nil {
					t.Fatal(err)
				}
				retained.OperationID = task.Record.OperationID
				retained.UpdatedAt = retained.UpdatedAt.Add(time.Hour)
				value, err := encodeBackupRecoveryPointPruneRecord(retained)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(
					ctx, store, backupRecoveryPointPruneKey(point.Point.ID), value,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:         "fabricated Environment deletion adoption",
			wantConflict: true,
			mutate: func(t *testing.T, store *memoryTaskStore, _ Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				pointRead, err := store.Get(ctx, backupRecoveryPointKey(point.Point.ID))
				if err != nil || pointRead.Entry == nil {
					t.Fatalf("Get(recovery point) = %#v, %v", pointRead, err)
				}
				adopted := BackupRecoveryPointPruneRecord{
					Point:         point.Point,
					PointRevision: pointRead.Entry.ModRevision,
					OperationID:   ids.NewAt(ids.KindOperation, point.CreatedAt, 7411),
					State:         BackupPruneAssigned,
					TaskID:        ids.NewAt(ids.KindTask, point.CreatedAt, 7412),
					CreatedAt:     point.CreatedAt,
					UpdatedAt:     point.CreatedAt.Add(2 * time.Hour),
				}
				value, err := encodeBackupRecoveryPointPruneRecord(adopted)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				if _, err := putTerminalReceiptTestValue(ctx, store, backupRecoveryPointPruneKey(point.Point.ID), value); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Environment deletion adopted",
			mutate: func(t *testing.T, store *memoryTaskStore, _ Versioned[TaskRecord], point BackupPruneTerminalPointOutcome) {
				t.Helper()
				environmentRead, err := store.Get(ctx, environmentKey(point.Point.EnvironmentID))
				if err != nil || environmentRead.Entry == nil {
					t.Fatalf("Get(Environment) = %#v, %v", environmentRead, err)
				}
				deletionAt := point.CreatedAt.Add(3 * time.Hour)
				operationID := ids.NewAt(ids.KindOperation, deletionAt, 7421)
				taskID := ids.NewAt(ids.KindTask, deletionAt, 7422)
				lockValue, err := encodeBackupOperationLockRecord(BackupOperationLockRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: operationID,
					TaskID: taskID, Kind: BackupOperationDeletion,
					CreatedAt: deletionAt, UpdatedAt: deletionAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(lockValue)
				tombstone := DeletionTombstoneRecord{
					TargetKind: DeletionTargetEnvironment, TargetID: point.Point.EnvironmentID,
					TargetRevision: environmentRead.Entry.ModRevision, TaskID: taskID,
					Phase: DeletionPhaseHostEffects, CreatedAt: deletionAt, UpdatedAt: deletionAt,
				}
				tombstoneValue, err := encodeDeletionTombstone(tombstone)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(tombstoneValue)
				intentValue, err := encodeEnvironmentDeletionIntent(EnvironmentDeletionIntentRecord{
					EnvironmentID: point.Point.EnvironmentID, OperationID: operationID, TaskID: taskID,
					TargetRevision: tombstone.TargetRevision,
					CleanupPhase:   EnvironmentDeletionCleanupEnumerating, CreatedAt: deletionAt,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(intentValue)
				epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
					EnvironmentID: point.Point.EnvironmentID,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer clear(epochValue)
				transaction, err := store.Transact(ctx, nil, []Mutation{
					{Type: MutationPut, Key: environmentOperationLockKey(point.Point.EnvironmentID), Value: lockValue},
					{Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetEnvironment), point.Point.EnvironmentID), Value: tombstoneValue},
					{Type: MutationPut, Key: environmentDeletionIntentKey(operationID), Value: intentValue},
					{Type: MutationPut, Key: environmentMutationEpochKey(point.Point.EnvironmentID), Value: epochValue},
				})
				if err != nil || !transaction.Succeeded {
					t.Fatalf("seed Environment deletion adoption = %#v, %v", transaction, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, task := seedBackupTerminalReceiptReplay(t)
			receiptRead, err := store.Get(ctx, backupTerminalReceiptKey(task.Record.ID))
			if err != nil || receiptRead.Entry == nil {
				t.Fatalf("Get(receipt) = %#v, %v", receiptRead, err)
			}
			receipt, err := decodeBackupTerminalReceiptRecord(receiptRead.Entry.Value)
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
	// Rationale: a verified-absent point has a permanently retired stable id;
	// later point or prune companions are corruption, not successor authority.
	t.Parallel()
	ctx := context.Background()
	terminal, dispatch, verified, initialPlan := backupTerminalReceiptPruneFixture(t)
	initialPlan.clear()
	verified.Record.State = BackupPruneVerifiedAbsent
	plan, err := prepareBackupPruneTerminalReceipt(
		Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		[]Versioned[BackupRecoveryPointPruneRecord]{verified},
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
	taskValue, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	transaction, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: taskKey(terminal.ID), Value: taskValue},
		{Type: MutationPut, Key: backupTerminalReceiptKey(terminal.ID), Value: plan.mutations[0].Value},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed removed terminal receipt = %#v, %v", transaction, err)
	}
	task := Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(absent point) error = %v", err)
	}

	point := BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: verified.Record.Point,
		VerifiedAt:                  verified.Record.CreatedAt,
	}
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store,
		backupRecoveryPointKey(point.ID),
		pointValue,
	); err != nil {
		t.Fatal(err)
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(torn point) error = %v", err)
	}
	if _, err := deleteTerminalReceiptTestValue(
		ctx,
		store,
		backupRecoveryPointKey(point.ID),
	); err != nil {
		t.Fatal(err)
	}
	reconstructed := verified.Record
	reconstructed.State = BackupPrunePending
	reconstructed.TaskID = ""
	reconstructed.UpdatedAt = terminal.FinishedAt.Add(time.Hour)
	pruneValue, err := encodeBackupRecoveryPointPruneRecord(reconstructed)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store,
		backupRecoveryPointPruneKey(point.ID),
		pruneValue,
	); err != nil {
		t.Fatal(err)
	}
	err = repository.validateBackupTerminalReceiptReplay(ctx, task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("validateBackupTerminalReceiptReplay(reconstructed prune) error = %v", err)
	}
}

func TestBackupTerminalReceiptReplaySurvivesLaterOrphanReconciliationAndEnvironmentCascade(t *testing.T) {
	// Rationale: orphan reconciliation changes only orphan authority; the
	// terminal BackupRun remains frozen at the Task/receipt commit revision.
	t.Parallel()
	ctx := context.Background()
	now := taskJournalTime().Add(3 * time.Hour)
	run := testBackupRun(now, now.Add(2*time.Second), newTestBackupRecipient(t))
	current := validTaskRecord(now)
	current.ID = run.TaskID
	current.OperationID = run.OperationID
	current.Owner = TaskOwner{
		WorkspaceType: TaskWorkspacePlatform,
		ProjectID:     ids.NewAt(ids.KindProject, now, 7451),
		EnvironmentID: run.EnvironmentID,
	}
	current.Actor = TaskActorOperator
	current.Executor = TaskExecutorAgent
	current.Type = TaskBackup
	current.Target = run.EnvironmentID
	current.RenderGeneration = 0
	current.Params = nil
	current.Materializations = nil
	current.Steps = nil
	current.TimeoutSeconds = backupTaskTimeoutSeconds
	startedAt := now.Add(time.Second)
	current.Status = TaskStatusRunning
	current.StartedAt = &startedAt
	current.UpdatedAt = startedAt
	terminal, err := transitionTaskStatus(
		current,
		TaskStatusRunning,
		TaskStatusCompleted,
		run.UpdatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.Result = &TaskResultRecord{
		Kind:       TaskResultCompose,
		Diagnostic: TaskResultDiagnosticNone,
	}
	terminal.TerminalAssignment = &TaskTerminalAssignmentRecord{
		AssignmentID:    ids.NewAt(ids.KindAssignment, now, 7452),
		AgentID:         ids.NewAt(ids.KindAgent, now, 7453),
		AgentGeneration: 1,
	}
	plan, err := prepareBackupRunTerminalReceipt(
		Versioned[TaskRecord]{Record: current, Revision: 19},
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
	project := ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 7454), Slug: "backup-owner", Name: "Backup Owner",
		Kind: ProjectKindBacking,
	}
	environment, err := NewProvisioningEnvironment(
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
	environmentValue, err := encodeEnvironment(environment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store,
		environmentKey(environment.ID),
		environmentValue,
	); err != nil {
		t.Fatal(err)
	}
	taskValue, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	runValue, err := encodeBackupRunRecord(run)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runValue)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environment.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(epochValue)
	membershipKey, err := backupRunEnvironmentIndexKey(environment.ID, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: taskKey(terminal.ID), Value: taskValue},
		{Type: MutationPut, Key: backupTerminalReceiptKey(terminal.ID), Value: plan.mutations[0].Value},
		{Type: MutationPut, Key: backupRunKey(terminal.ID), Value: runValue},
		{Type: MutationPut, Key: membershipKey, Value: []byte(terminal.ID)},
		{Type: MutationPut, Key: environmentMutationEpochKey(environment.ID), Value: epochValue},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed terminal Backup receipt = %#v, %v", transaction, err)
	}
	task := Versioned[TaskRecord]{
		Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}
	point := backupRuntimeTestPoint(run, run.Sources[0], run.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point:  point.BackupRecoveryPointSnapshot,
		TaskID: run.TaskID,
		Reconciliation: BackupOrphanReconciliationAuthority{
			OperationID: run.OperationID, PolicyRevision: run.PolicyRevision,
			RetentionKeep: run.RetentionKeep,
		},
		State: BackupOrphanInspect, CreatedAt: run.UpdatedAt, UpdatedAt: run.UpdatedAt,
	}
	orphanValue, err := encodeBackupOrphanRecord(orphan)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(orphanValue)
	if _, err := putTerminalReceiptTestValue(
		ctx,
		store,
		backupOrphanKey(point.ID),
		orphanValue,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTerminalReceiptTestValue(
		ctx,
		store,
		backupOrphanKey(point.ID),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(reconciled orphan) error = %v", err)
	}
	unchangedRun, err := store.Get(ctx, backupRunKey(run.TaskID))
	if err != nil || unchangedRun.Entry == nil || unchangedRun.Entry.ModRevision != task.Revision {
		t.Fatalf("terminal run after orphan reconciliation = %#v, %v", unchangedRun, err)
	}
	if _, err := putTerminalReceiptTestValue(ctx, store, backupRunKey(run.TaskID), runValue); err != nil {
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
	if _, err := deleteTerminalReceiptTestValue(ctx, store, backupRunKey(run.TaskID)); err != nil {
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
		store,
		environmentMutationEpochKey(environment.ID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTerminalReceiptTestValue(ctx, store, environmentKey(environment.ID)); err != nil {
		t.Fatal(err)
	}
	if err := repository.validateBackupTerminalReceiptReplay(ctx, task); err != nil {
		t.Fatalf("validateBackupTerminalReceiptReplay(completed owner cascade) error = %v", err)
	}
}

func TestBackupTerminalReceiptPruneCompanionRequiresAtomicRevisionAndOlderPrior(t *testing.T) {
	// Rationale: Task pruning may permanently delete the proof only after it
	// validates the Task/receipt same-revision fence and an older prior revision.
	t.Parallel()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	key := backupTerminalReceiptKey(terminal.ID)
	entry := &KeyValue{Key: key, Value: plan.mutations[0].Value, ModRevision: 20}
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
	rewritten.ReceiptDigest, err = backupTerminalReceiptDigest(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeBackupTerminalReceiptRecord(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	if _, err := prepareBackupTerminalReceiptPruneCompanion(
		terminal,
		20,
		&KeyValue{Key: key, Value: value, ModRevision: 20},
	); err == nil {
		t.Fatal("prepareBackupTerminalReceiptPruneCompanion(rewritten prior) succeeded")
	}
}

func TestBackupTerminalReceiptPruneFinalizationResumesAfterPrimaryDeletion(t *testing.T) {
	// Rationale: a crash after public Task deletion leaves the private intent as
	// durable authority to delete the immutable receipt and intent atomically.
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
		store,
		backupTerminalReceiptKey(taskID),
		[]byte("retained-proof"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := taskPruneIntent{
		TaskID: taskID, TaskRevision: receiptRevision,
		BackupCheckpointCursorsComplete:        true,
		BackupCheckpointDeduplicationsComplete: true,
		TaskPrimaryDeleted:                     true,
		BackupTerminalReceiptRevision:          receiptRevision,
	}
	intentValue, err := encodeTaskPruneIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(intentValue)
	intentRevision, err := putTerminalReceiptTestValue(
		ctx,
		store,
		taskPruneIntentKey(taskID),
		intentValue,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.finishTaskPruneIntent(ctx, Versioned[taskPruneIntent]{
		Record: intent, Revision: intentRevision, ReadRevision: intentRevision,
	}); err != nil {
		t.Fatalf("finishTaskPruneIntent() error = %v", err)
	}
	if store.maximumOperations > maximumTransactionOperations {
		t.Fatalf("receipt prune finalization operations = %d", store.maximumOperations)
	}
	for _, key := range []string{backupTerminalReceiptKey(taskID), taskPruneIntentKey(taskID)} {
		read, err := store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("Get(%s) = %#v, %v", key, read, err)
		}
	}
}

func TestBackupTerminalReceiptWorstPruneRecordFitsDurableBound(t *testing.T) {
	// Rationale: the closed eleven-point prune maximum must fit the runtime
	// record bound before the terminal transaction can consume its one mutation.
	t.Parallel()
	terminal, dispatch, assigned, _ := backupTerminalReceiptPruneFixture(t)
	dispatch.RecoveryPointIDs = make([]string, maximumBackupPruneDispatchPoints)
	prunes := make([]Versioned[BackupRecoveryPointPruneRecord], len(dispatch.RecoveryPointIDs))
	for index := range prunes {
		point := assigned.Record.Point
		point.ID = ids.NewAt(
			ids.KindRecoveryPoint,
			assigned.Record.Point.CreatedAt.Add(time.Duration(index)*time.Millisecond),
			int64(7600+index),
		)
		point.CreatedAt = assigned.Record.Point.CreatedAt.Add(time.Duration(index) * time.Millisecond)
		point.ObjectKey = "production/" + point.EnvironmentID + "/" + point.SourceID + "/" +
			point.ID + "/artifact.bin"
		dispatch.RecoveryPointIDs[index] = point.ID
		prune := assigned.Record
		prune.Point = point
		prunes[index] = Versioned[BackupRecoveryPointPruneRecord]{
			Record: prune, Revision: int64(index + 1),
		}
	}
	plan, err := prepareBackupPruneTerminalReceipt(
		Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		prunes,
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	defer plan.clear()
	if len(plan.conditions) != 0 {
		t.Fatalf("worst terminal receipt conditions = %d, want 0", len(plan.conditions))
	}
	if size := len(plan.mutations[0].Value); size > maximumBackupRuntimeRecordBytes {
		t.Fatalf("worst terminal receipt size = %d, limit %d", size, maximumBackupRuntimeRecordBytes)
	}
}

func TestBackupTerminalReceiptWorstPruneCompositionUsesExactlyNinetySixOperations(t *testing.T) {
	// Rationale: the closed eleven-point terminal path has no spare etcd
	// operation, so receipt creation must reuse the Task and epoch CAS fences.
	t.Parallel()
	_, _, _, receiptPlan := backupTerminalReceiptPruneFixture(t)
	defer receiptPlan.clear()
	taskPlan := backupTaskTerminalPlan{}
	for index := range 9 {
		taskPlan.conditions = append(taskPlan.conditions, Condition{
			Key: "/test/terminal-task-condition/" + strconv.Itoa(index), ModRevision: 1,
		})
	}
	for index := range 8 {
		taskPlan.mutations = append(taskPlan.mutations, Mutation{
			Type: MutationDelete, Key: "/test/terminal-task-mutation/" + strconv.Itoa(index),
		})
	}
	prunePlan := backupPruneTransactionPlan{}
	for index := range 64 {
		key := "/test/terminal-prune-condition/" + strconv.Itoa(index)
		if index == 0 {
			key = environmentMutationEpochKey(receiptPlan.record.Task.Owner.EnvironmentID)
		}
		prunePlan.conditions = append(prunePlan.conditions, Condition{Key: key, ModRevision: 18})
	}
	for index := range 14 {
		prunePlan.mutations = append(prunePlan.mutations, Mutation{
			Type: MutationDelete, Key: "/test/terminal-prune-mutation/" + strconv.Itoa(index),
		})
	}
	conditions, mutations, err := composeBackupPruneTerminalTransaction(
		taskPlan,
		prunePlan,
		receiptPlan,
	)
	defer clearBackupRuntimeMutations(mutations)
	if err != nil {
		t.Fatalf("composeBackupPruneTerminalTransaction() error = %v", err)
	}
	if operations := len(conditions) + len(mutations); operations != maximumTransactionOperations {
		t.Fatalf("worst terminal prune operations = %d, want %d", operations, maximumTransactionOperations)
	}
}

func TestBackupTerminalReceiptDuplicateWriterLosesTaskCAS(t *testing.T) {
	// Rationale: the exact running Task ModRevision compare serializes receipt
	// creation even though the closed transaction cannot spend a receipt CAS.
	t.Parallel()
	ctx := context.Background()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	value, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision - 1
	prior, err := putTerminalReceiptTestValue(ctx, store, taskKey(terminal.ID), value)
	if err != nil || prior != plan.record.PriorTaskRevision {
		t.Fatalf("seed prior Task revision = %d, %v", prior, err)
	}
	conditions := []Condition{{Key: taskKey(terminal.ID), ModRevision: prior}}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(terminal.ID), Value: value},
		{Type: MutationPut, Key: backupTerminalReceiptKey(terminal.ID), Value: plan.mutations[0].Value},
	}
	first, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !first.Succeeded {
		t.Fatalf("first terminal writer = %#v, %v", first, err)
	}
	second, err := store.Transact(ctx, conditions, mutations)
	if err != nil || second.Succeeded {
		t.Fatalf("duplicate terminal writer = %#v, %v", second, err)
	}
	receiptRead, err := store.Get(ctx, backupTerminalReceiptKey(terminal.ID))
	if err != nil || receiptRead.Entry == nil || receiptRead.Entry.Version != 1 ||
		receiptRead.Entry.ModRevision != first.Revision {
		t.Fatalf("terminal receipt after duplicate = %#v, %v", receiptRead, err)
	}
}

func backupTerminalReceiptPruneFixture(
	t *testing.T,
) (
	TaskRecord,
	BackupRecoveryPointPruneDispatchRecord,
	Versioned[BackupRecoveryPointPruneRecord],
	backupTerminalReceiptPlan,
) {
	t.Helper()
	now := taskJournalTime().Add(40 * time.Minute)
	point := testBackupVolumePoint(now, newTestBackupRecipient(t))
	operationID := ids.NewAt(ids.KindOperation, now, 7201)
	taskID := ids.NewAt(ids.KindTask, now, 7202)
	owner := TaskOwner{
		WorkspaceType: TaskWorkspacePlatform,
		ProjectID:     ids.NewAt(ids.KindProject, now, 7203),
		EnvironmentID: point.EnvironmentID,
	}
	current := validTaskRecord(now)
	current.ID = taskID
	current.OperationID = operationID
	current.Owner = owner
	current.Actor = TaskActorSystem
	current.Executor = TaskExecutorAgent
	current.Type = TaskBackupPrune
	current.Target = point.EnvironmentID
	current.RenderGeneration = 0
	current.Params = nil
	current.Materializations = nil
	current.Steps = nil
	current.TimeoutSeconds = backupTaskTimeoutSeconds
	startedAt := now.Add(time.Second)
	current.Status = TaskStatusRunning
	current.StartedAt = &startedAt
	current.UpdatedAt = startedAt
	terminalAt := startedAt.Add(time.Second)
	terminal, err := transitionTaskStatus(
		current,
		TaskStatusRunning,
		TaskStatusFailed,
		terminalAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.Result = &TaskResultRecord{
		Kind:       TaskResultCompose,
		ExitCode:   1,
		Diagnostic: TaskResultDiagnosticComposeFailed,
	}
	terminal.TerminalAssignment = &TaskTerminalAssignmentRecord{
		AssignmentID:    ids.NewAt(ids.KindAssignment, now, 7204),
		AgentID:         ids.NewAt(ids.KindAgent, now, 7205),
		AgentGeneration: 1,
	}
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID:           taskID,
		OperationID:      operationID,
		EnvironmentID:    point.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        now,
	}
	assigned := Versioned[BackupRecoveryPointPruneRecord]{
		Record: BackupRecoveryPointPruneRecord{
			Point:         point,
			PointRevision: 19,
			OperationID:   operationID,
			State:         BackupPruneAssigned,
			TaskID:        taskID,
			CreatedAt:     now,
			UpdatedAt:     startedAt,
		},
		Revision: 18,
	}
	plan, err := prepareBackupPruneTerminalReceipt(
		Versioned[TaskRecord]{Record: current, Revision: 19},
		terminal,
		dispatch,
		[]Versioned[BackupRecoveryPointPruneRecord]{assigned},
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	return terminal, dispatch, assigned, plan
}

func seedBackupTerminalReceiptReplay(
	t *testing.T,
) (*TaskRepository, *memoryTaskStore, Versioned[TaskRecord]) {
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
	taskValue, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	pending := assigned.Record
	pending.State = BackupPrunePending
	pending.TaskID = ""
	pending.UpdatedAt = *terminal.FinishedAt
	pendingValue, err := encodeBackupRecoveryPointPruneRecord(pending)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pendingValue)
	point := BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: pending.Point,
		VerifiedAt:                  pending.CreatedAt,
	}
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(
		point.EnvironmentID,
		point.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	project := ProjectRecord{
		ID: ids.NewAt(ids.KindProject, pending.CreatedAt, 7701), Slug: "receipt-owner",
		Name: "Receipt Owner", Kind: ProjectKindBacking,
	}
	environment, err := NewProvisioningEnvironment(
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
	environmentValue, err := encodeEnvironment(environment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environment.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(epochValue)
	store.revision = plan.record.PriorTaskRevision - 1
	pointTransaction, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentKey(environment.ID), Value: environmentValue},
		{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointTransaction.Succeeded ||
		pointTransaction.Revision != plan.record.PriorTaskRevision {
		t.Fatalf("seed retained point authority = %#v, %v", pointTransaction, err)
	}
	transaction, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: taskKey(terminal.ID), Value: taskValue},
		{Type: MutationPut, Key: backupTerminalReceiptKey(terminal.ID), Value: plan.mutations[0].Value},
		{Type: MutationPut, Key: backupRecoveryPointPruneKey(pending.Point.ID), Value: pendingValue},
		{Type: MutationPut, Key: environmentMutationEpochKey(environment.ID), Value: epochValue},
	})
	if err != nil || !transaction.Succeeded || transaction.Revision <= plan.record.PriorTaskRevision {
		t.Fatalf("seed terminal receipt = %#v, %v", transaction, err)
	}
	return repository, store, Versioned[TaskRecord]{
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
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
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
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
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
		[]Mutation{{Type: MutationDelete, Key: key}},
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
	request GetManyRequest,
) (*GetManyResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.GetMany(ctx, request)
}

func (store *compactedTaskStore) Range(
	ctx context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.Range(ctx, request)
}
