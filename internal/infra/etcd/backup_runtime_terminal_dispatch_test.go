package etcd

import (
	context "context"
	hex "encoding/hex"
	errors "errors"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: future manual Task publication must be able to commit the Task,
// lock, run membership, and exclusions in one transaction with no standalone
// pre-publication write.
func TestBackupRuntimeRepositoryPreparesComposableRunPublication(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
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
		t.Fatalf("prepareBackupRunPublication() error = %v", err)
	}
	defer plan.clear()
	taskMarker := "/v1/test/backup-publication-tasks/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: taskMarker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: taskMarker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose backup publication = %v", err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("composed backup publication = %#v, %v", result, err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), membership, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID), taskMarker} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != result.Revision {
			t.Fatalf("composed publication key %q = %#v", key, entry)
		}
	}
}

// Rationale: terminal Backup state, the exact assigned Task terminal records,
// exclusion and lock release, and the final 768-KiB envelope are composed and
// committed at one revision.
func TestBackupRuntimeRepositoryComposesRealAssignedTaskTerminal(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	genericTask := task
	genericTask.Params = map[string]string{"source": run.Sources[0].SourceID}
	if _, err := runPlan.taskIdempotencyPlan(
		genericTask, sealed, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(generic Backup Params) error = %v", err)
	}
	fabricatedRun := run
	fabricatedRun.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	fabricatedRun.Sources[0].SourceRevision++
	fabricated := backupRuntimeSealedRunPlan(t, fabricatedRun, task.PlanID)
	fabricatedTask := task
	fabricatedTask.PlanHash = hex.EncodeToString(fabricated.PlanHash)
	if _, err := runPlan.taskIdempotencyPlan(
		fabricatedTask, fabricated, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(fabricated Backup source) error = %v", err)
	}
	idempotencyPlan, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idempotency.Apply(context.Background(), marker, idempotencyPlan)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 990)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	terminal, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		run.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(Backup failure) error = %v", err)
	}
	committedRevision := terminal.Revision
	for _, key := range []string{testtaskjournal.TaskStorageKey(run.TaskID), testbackupruntime.BackupRunKey(run.TaskID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != committedRevision {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	failedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || failedRun.Record.State != testbackupruntime.BackupRunFailed ||
		failedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptFailed ||
		failedRun.Record.Sources[0].FailureCode != testbackupruntime.BackupFailureCapture {
		t.Fatalf("terminal Backup run = %#v, %v", failedRun, err)
	}
	if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
		t.Fatal("terminal Backup Task retained its Environment lock")
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay) = %#v, %v", replay, err)
	}
	newerAt := run.CreatedAt.Add(4 * time.Second)
	putBackupRuntimeLock(t, store, testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   ids.NewAt(ids.KindOperation, newerAt, 1991),
		TaskID:        ids.NewAt(ids.KindTask, newerAt, 1992),
		Kind:          testbackupruntime.BackupOperationRestore,
		CreatedAt:     newerAt,
		UpdatedAt:     newerAt,
	})
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay after successor lock) = %#v, %v", replay, err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tornMembership, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: membership}},
	)
	if err != nil || !tornMembership.Succeeded {
		t.Fatalf("remove Backup run membership = %#v, %v", tornMembership, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with torn membership) error = %v", err)
	}
	incompleteCascade, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentKey(run.EnvironmentID)},
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)},
			{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupRunKey(run.TaskID)},
		},
	)
	if err != nil || !incompleteCascade.Succeeded {
		t.Fatalf("simulate incomplete Environment owner cascade = %#v, %v", incompleteCascade, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with retained epoch) error = %v", err)
	}
	cascade, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentMutationEpochKey(run.EnvironmentID)},
		},
	)
	if err != nil || !cascade.Succeeded {
		t.Fatalf("complete Environment owner cascade = %#v, %v", cascade, err)
	}
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(4*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay after Environment cascade) = %#v, %v", replay, err)
	}
}

// Rationale: a public completed acknowledgement must atomically terminalize the
// Backup Task and its captured run rather than accepting a Task-only outcome.
func TestBackupRuntimeRepositoryRoutesCompletedAcknowledgement(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		context.Background(), run.TaskID, testtaskjournal.TaskStatusCompleted, run.CreatedAt.Add(time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("AcknowledgeControllerTask(Backup) error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 1993)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	current, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	running := current.Record
	running.State = testbackupruntime.BackupRunRunning
	running.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), current.Record.Sources...)
	running.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	running.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	running.Sources[0].SizeBytes = 123
	running.Sources[0].SHA256 = testBackupDigest
	running.UpdatedAt = run.CreatedAt.Add(2 * time.Second)
	value, err := testbackupruntime.EncodeBackupRunRecord(running)
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRunKey(run.TaskID), ModRevision: current.Revision}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(run.TaskID), Value: value},
		},
	)
	clear(value)
	if err != nil || !replaced.Succeeded {
		t.Fatalf("seed completed source state = %#v, %v", replaced, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		run.TaskID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		completedComposeTaskResult(),
		run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(Backup completed) = %#v, %v", terminal, err)
	}
	completed, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || completed.Record.State != testbackupruntime.BackupRunCompleted ||
		completed.Revision != terminal.Revision {
		t.Fatalf("completed Backup run = %#v, %v", completed, err)
	}
}

// Rationale: individually valid domain records must not authorize terminalizing
// a Task whose operation, owner, target, creation, or retry identity differs.
func TestBackupTaskTerminalBindingRejectsRewrittenDomainIdentity(t *testing.T) {
	_, store, run := newBackupRuntimeBareFixture(t)
	task, _, _, _ := backupRuntimePublicationTask(t, store, run)
	if err := ValidateBackupRunTaskBinding(task, run); err != nil {
		t.Fatalf("validateBackupRunTaskBinding(valid) error = %v", err)
	}
	rewrittenRun := run
	rewrittenRun.OperationID = ids.NewAt(ids.KindOperation, run.CreatedAt, 1994)
	if err := ValidateBackupRunTaskBinding(task, rewrittenRun); !errors.Is(
		err, errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("validateBackupRunTaskBinding(rewritten) error = %v", err)
	}
	pruneTask := task
	pruneTask.Type = testtaskjournal.TaskBackupPrune
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: task.ID, OperationID: task.OperationID,
		EnvironmentID: run.EnvironmentID, CreatedAt: task.CreatedAt,
		RecoveryPointIDs: []string{run.Sources[0].RecoveryPointID},
	}
	if err := ValidateBackupPruneTaskBinding(pruneTask, dispatch); err != nil {
		t.Fatalf("validateBackupPruneTaskBinding(valid) error = %v", err)
	}
	dispatch.EnvironmentID = ids.NewAt(ids.KindEnvironment, run.CreatedAt, 1995)
	if err := ValidateBackupPruneTaskBinding(pruneTask, dispatch); !errors.Is(
		err, errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("validateBackupPruneTaskBinding(rewritten) error = %v", err)
	}
}

// Rationale: every abort and timeout entry point must use the same atomic Backup
// Task/run/orphan/cleanup/exclusion/lock terminal transaction.
func TestBackupRuntimeRepositoryRoutesAbortAndTimeoutThroughDomainTerminal(t *testing.T) {
	for _, test := range []struct {
		name            string
		pending         bool
		status          testtaskjournal.TaskStatus
		runState        testbackupruntime.BackupRunState
		failure         testbackupruntime.BackupFailureCode
		useTimeout      bool
		useAgentTimeout bool
	}{
		{
			name: "pending abort", pending: true, status: testtaskjournal.TaskStatusAborted,
			runState: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
		},
		{
			name: "running abort", status: testtaskjournal.TaskStatusAborted,
			runState: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
		},
		{
			name: "running timeout", status: testtaskjournal.TaskStatusTimedOut,
			runState: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut, useTimeout: true,
		},
		{
			name: "stale Agent timeout", status: testtaskjournal.TaskStatusTimedOut,
			runState: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut, useAgentTimeout: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
			runPlan, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run),
				backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer runPlan.clear()
			task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
			publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			idempotency, err := NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			applied, err := idempotency.Apply(context.Background(), marker, publication)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := applied.Classify()
			if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}

			var terminal testkeyvalue.Versioned[TaskRecord]
			var assignmentID string
			agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 1990)
			if test.pending {
				terminal, err = tasks.AbortPendingTask(
					context.Background(), run.TaskID, run.CreatedAt.Add(time.Second),
				)
			} else {
				claim, found, claimErr := tasks.ClaimNextTask(
					context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
				)
				if claimErr != nil || !found || claim.Task.Record.ID != run.TaskID {
					t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, claimErr)
				}
				assignmentID = claim.Assignment.Record.AssignmentID
				if test.useTimeout || test.useAgentTimeout {
					var count int
					var timeoutErr error
					if test.useAgentTimeout {
						count, timeoutErr = tasks.TimeoutAgentAssignments(
							context.Background(), agentID, 1, 1,
							claim.Assignment.Record.Deadline,
						)
					} else {
						count, timeoutErr = tasks.ExpireTimedOutTasks(
							context.Background(), claim.Assignment.Record.Deadline,
						)
					}
					if timeoutErr != nil || count != 1 {
						t.Fatalf("timeout collector = %d, %v", count, timeoutErr)
					}
					terminal, err = tasks.GetTask(context.Background(), run.TaskID)
				} else {
					terminal, err = tasks.AcknowledgeTask(
						context.Background(), agentID, 1, run.TaskID, assignmentID,
						test.status, testtaskjournal.TaskResultRecord{
							Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
							ReconciliationRequired: true, ExecutionEpoch: 1,
						}, run.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal Backup Task = %#v, %v", terminal, err)
			}
			storedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
			if err != nil || storedRun.Record.State != test.runState ||
				storedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptFailed ||
				storedRun.Record.Sources[0].FailureCode != test.failure ||
				storedRun.Revision != terminal.Revision {
				t.Fatalf("terminal Backup run = %#v, %v", storedRun, err)
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("terminal Backup retained its Environment lock")
			}
			if test.pending {
				replay, replayErr := tasks.AbortPendingTask(
					context.Background(), run.TaskID, run.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AbortPendingTask(replay) = %#v, %v", replay, replayErr)
				}
			} else {
				result := testtaskjournal.TaskResultRecord{
					Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: 1,
				}
				replay, replayErr := tasks.AcknowledgeTask(
					context.Background(), agentID, 1, run.TaskID, assignmentID,
					test.status, result, run.CreatedAt.Add(4*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, replayErr)
				}
			}
		})
	}
}

// Rationale: one Backup terminalization must not prevent a timeout collector
// from processing later expired Tasks in the same bounded scan.
func TestTaskRepositoryTimeoutCollectorContinuesAfterBackupTerminal(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	backupTask, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(backupTask, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := applied.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := validTaskRecord(run.CreatedAt.Add(time.Second))
	ordinary.TimeoutSeconds = backupTaskTimeoutSeconds + 10
	createLifecycleTask(t, tasks, ordinary)
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 2990)
	backupClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(2*time.Second),
	)
	if err != nil || !found || backupClaim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask(Backup) = %#v/%v/%v", backupClaim, found, err)
	}
	ordinaryClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
		t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
	}
	if !ordinaryClaim.Assignment.Record.Deadline.After(backupClaim.Assignment.Record.Deadline) {
		t.Fatal("test fixture did not order Backup timeout before ordinary timeout")
	}
	count, err := tasks.ExpireTimedOutTasks(
		context.Background(), ordinaryClaim.Assignment.Record.Deadline,
	)
	if err != nil || count != 2 {
		t.Fatalf("ExpireTimedOutTasks(batch) = %d, %v", count, err)
	}
	for _, taskID := range []string{run.TaskID, ordinary.ID} {
		terminal, getErr := tasks.GetTask(context.Background(), taskID)
		if getErr != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
			t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
		}
	}
}

// Rationale: an expired Backup Agent assignment must not stop the collector from
// terminalizing later assignments in the same bounded scan.
func TestTaskRepositoryAgentTimeoutContinuesAfterBackupTerminal(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	backupTask, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(backupTask, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := validTaskRecord(run.CreatedAt.Add(time.Second))
	createLifecycleTask(t, tasks, ordinary)
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 2991)
	backupClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(2*time.Second),
	)
	if err != nil || !found || backupClaim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask(Backup) = %#v/%v/%v", backupClaim, found, err)
	}
	ordinaryClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
		t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
	}
	terminalAt := backupClaim.Assignment.Record.Deadline.Add(time.Second)
	count, err := tasks.TimeoutAgentAssignments(
		context.Background(), agentID, 1, 2, terminalAt,
	)
	if err != nil || count != 2 {
		t.Fatalf("TimeoutAgentAssignments(batch) = %d, %v", count, err)
	}
	for _, taskID := range []string{run.TaskID, ordinary.ID} {
		terminal, getErr := tasks.GetTask(context.Background(), taskID)
		if getErr != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
			t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
		}
	}
}

// Rationale: prune pending abort, assigned abort, and timeout must atomically
// release dispatch authority while preserving every surviving tombstone.
func TestBackupRuntimeRepositoryRoutesPruneAbortAndTimeoutThroughDomainTerminal(t *testing.T) {
	for _, test := range []struct {
		name       string
		pending    bool
		status     testtaskjournal.TaskStatus
		useTimeout bool
	}{
		{name: "pending abort", pending: true, status: testtaskjournal.TaskStatusAborted},
		{name: "running abort", status: testtaskjournal.TaskStatusAborted},
		{name: "running timeout", status: testtaskjournal.TaskStatusTimedOut, useTimeout: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, tasks, dispatch, pointID := publishBackupPruneLifecycleTask(t)
			agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 3990)
			var terminal testkeyvalue.Versioned[TaskRecord]
			var assignmentID string
			var err error
			if test.pending {
				terminal, err = tasks.AbortPendingTask(
					context.Background(), dispatch.TaskID, dispatch.CreatedAt.Add(time.Second),
				)
			} else {
				claim, found, claimErr := tasks.ClaimNextTask(
					context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
				)
				if claimErr != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
					t.Fatalf("ClaimNextTask(prune) = %#v/%v/%v", claim, found, claimErr)
				}
				assignmentID = claim.Assignment.Record.AssignmentID
				if test.useTimeout {
					count, timeoutErr := tasks.ExpireTimedOutTasks(
						context.Background(), claim.Assignment.Record.Deadline,
					)
					if timeoutErr != nil || count != 1 {
						t.Fatalf("ExpireTimedOutTasks(prune) = %d, %v", count, timeoutErr)
					}
					terminal, err = tasks.GetTask(context.Background(), dispatch.TaskID)
				} else {
					terminal, err = tasks.AcknowledgeTask(
						context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
						test.status, testtaskjournal.TaskResultRecord{
							Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
							ReconciliationRequired: true, ExecutionEpoch: 1,
						}, dispatch.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal prune Task = %#v, %v", terminal, err)
			}
			if mustOptionalKey(
				t,
				store,
				testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			) != nil ||
				mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID)) != nil {
				t.Fatal("terminal prune retained dispatch or Environment lock")
			}
			entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
			if entry == nil {
				t.Fatal("terminal prune lost its surviving tombstone")
			}
			prune, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
			if decodeErr != nil || prune.State != testbackupruntime.BackupPrunePending || prune.TaskID != "" ||
				prune.OperationID != dispatch.OperationID || entry.ModRevision != terminal.Revision {
				t.Fatalf("terminal prune survivor = %#v/%#v/%v", entry, prune, decodeErr)
			}
			if test.pending {
				replay, replayErr := tasks.AbortPendingTask(
					context.Background(), dispatch.TaskID, dispatch.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AbortPendingTask(prune replay) = %#v, %v", replay, replayErr)
				}
			} else {
				result := testtaskjournal.TaskResultRecord{
					Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: 1,
				}
				replay, replayErr := tasks.AcknowledgeTask(
					context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
					test.status, result, dispatch.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AcknowledgeTask(prune replay) = %#v, %v", replay, replayErr)
				}
				if test.name == "running abort" {
					torn, deleteErr := store.Transact(
						context.Background(),
						nil,
						[]testkeyvalue.Mutation{{
							Type: testkeyvalue.MutationDelete,
							Key:  testhierarchy.EnvironmentMutationEpochKey(dispatch.EnvironmentID),
						}},
					)
					if deleteErr != nil || !torn.Succeeded {
						t.Fatalf("remove prune Environment epoch = %#v, %v", torn, deleteErr)
					}
					if _, replayErr := tasks.AcknowledgeTask(
						context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
						test.status, result, dispatch.CreatedAt.Add(4*time.Second),
					); !errors.Is(replayErr, errs.New(errs.KindStateConflict, "")) {
						t.Fatalf("AcknowledgeTask(prune replay without epoch) error = %v", replayErr)
					}
				}
			}
			_ = repository
		})
	}
}
