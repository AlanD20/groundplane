package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestTaskRepositoryExpiresOnlyOverdueAssignments(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	task.TimeoutSeconds = 30
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 118)
	assignedAt := task.CreatedAt.Add(time.Second)
	assignment, found, err := repository.ClaimNextTask(ctx, agentID, 4, assignedAt)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", assignment, found, err)
	}

	count, err := repository.ExpireTimedOutTasks(ctx, assignment.Assignment.Record.Deadline.Add(-time.Nanosecond))
	if err != nil || count != 0 {
		t.Fatalf("ExpireTimedOutTasks(before) = %d, %v", count, err)
	}
	count, err = repository.ExpireTimedOutTasks(ctx, assignment.Assignment.Record.Deadline)
	if err != nil || count != 1 {
		t.Fatalf("ExpireTimedOutTasks(at deadline) = %d, %v", count, err)
	}
	terminal, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if terminal.Record.Status != testtaskjournal.TaskStatusTimedOut || terminal.Record.Result == nil ||
		!terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("timed out Task = %#v", terminal.Record)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentKey(agentID, task.ID), false)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentIndexKey(task.ID), false)
	assertTaskLifecycleValue(
		t,
		store,
		testtaskjournal.TaskTimeoutIndexKey(task.ID, assignment.Assignment.Record.Deadline),
		false,
	)
}

func TestTaskRepositoryRecoveryExpiryMovesBoundedRowsToProofRequired(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	assignments := make([]testtaskassignments.TaskAssignmentRecord, 25)
	for index := range assignments {
		task := validTaskRecord(now.Add(time.Duration(index) * time.Nanosecond))
		task.ID = ids.NewAt(ids.KindTask, now, int64(2000+index*10))
		task.OperationID = ids.NewAt(ids.KindOperation, now, int64(2001+index*10))
		task.PlanID = ids.NewAt(ids.KindPlan, now, int64(2002+index*10))
		task.Target = ids.NewAt(ids.KindService, now, int64(2003+index*10))
		task.Steps[0].ID = ids.NewAt(ids.KindStep, now, int64(2004+index*10))
		task.IdempotencyKey = fmt.Sprintf("recovery-%08d", index)
		createLifecycleTask(t, repository, task)
		agentID := ids.NewAt(ids.KindAgent, now, int64(2005+index*10))
		claim, found, claimErr := repository.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
		if claimErr != nil || !found {
			t.Fatalf("ClaimNextTask(%d) = %#v, %t, %v", index, claim, found, claimErr)
		}
		assignments[index] = seedRecoveryTimeoutAssignment(t, store, claim)
	}

	deadline := assignments[len(assignments)-1].RecoveryDeadline
	store.conflictNextTransactions(1)
	count, err := repository.ExpireTimedOutTasks(ctx, deadline)
	if err != nil || count != 23 {
		t.Fatalf("ExpireTimedOutTasks(first) = %d, %v", count, err)
	}
	assertTaskLifecycleValue(
		t,
		store, testtaskjournal.TaskTimeoutIndexKey(assignments[0].TaskID, assignments[0].RecoveryDeadline), true,
	)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskRecoveryProofRequiredKey(assignments[0].TaskID), false)
	count, err = repository.ExpireTimedOutTasks(ctx, deadline)
	if err != nil || count != 2 {
		t.Fatalf("ExpireTimedOutTasks(second) = %d, %v", count, err)
	}
	for _, assignment := range assignments {
		assertTaskLifecycleValue(
			t,
			store,
			testtaskjournal.TaskTimeoutIndexKey(assignment.TaskID, assignment.RecoveryDeadline),
			false,
		)
		assertTaskLifecycleValue(t, store, testtaskjournal.TaskRecoveryProofRequiredKey(assignment.TaskID), true)
		current, lookupErr := repository.GetTaskAssignment(ctx, assignment.TaskID)
		if lookupErr != nil || !current.RecoveryProofRequired ||
			!current.Assignment.Record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) ||
			!current.Assignment.Record.RecoveryExecutionDeadline.Equal(
				deadline.Add(releaseRecoveryProofExecutionBudget),
			) ||
			current.Assignment.Record.RestorationAuthority == nil ||
			current.Assignment.Record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 {
			t.Fatalf("proof-required assignment = %#v, %v", current, lookupErr)
		}
		authorityDigest, digestErr := testtaskassignments.ReleaseRestorationAuthoritySHA256(
			*current.Assignment.Record.RestorationAuthority,
		)
		if digestErr != nil || authorityDigest != assignment.RestorationAuthoritySHA256 {
			t.Fatalf("proof-required restoration authority digest = %q, %v", authorityDigest, digestErr)
		}
		listed, listErr := repository.ListAgentAssignments(ctx, assignment.AgentID, assignment.AgentGeneration, 1)
		if listErr != nil || len(listed) != 1 || !listed[0].RecoveryProofRequired ||
			listed[0].Task.Revision != listed[0].Assignment.Revision {
			t.Fatalf("list proof-required assignment = %#v, %v", listed, listErr)
		}
		read, readErr := store.Get(ctx, testtaskassignments.ReleaseRecoveryKey(assignment.TaskID))
		if readErr != nil || read.Entry == nil {
			t.Fatalf("read recovery authority = %#v, %v", read, readErr)
		}
		recovery, decodeErr := testtaskassignments.DecodeReleaseRecoveryRecord(read.Entry.Value)
		if decodeErr != nil || !recovery.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
			t.Fatalf("immutable recovery deadline = %#v, %v", recovery, decodeErr)
		}
	}
}

func seedRecoveryTimeoutAssignment(
	t *testing.T,
	store *memoryTaskStore,
	claim TaskAssignment,
) testtaskassignments.TaskAssignmentRecord {
	t.Helper()
	record := claim.Assignment.Record
	authority := testtaskassignments.ReleaseRestorationAuthority{
		Schema: 1,
		TaskID: record.TaskID, OperationID: claim.Task.Record.OperationID, PlanHash: claim.Task.Record.PlanHash,
		EnvironmentID:       ids.NewAt(ids.KindEnvironment, claim.Task.Record.CreatedAt, 9000),
		CandidateArtifactID: ids.NewAt(ids.KindConfig, claim.Task.Record.CreatedAt, 9001),
		Candidates: []testtaskassignments.ReleaseRestorationCandidate{{
			Target:    testtaskassignments.ReleaseRestorationCandidateAbsence,
			ServiceID: ids.NewAt(ids.KindService, claim.Task.Record.CreatedAt, 9002),
			ReleaseID: ids.NewAt(ids.KindDeployment, claim.Task.Record.CreatedAt, 9003),
		}},
		NativePredecessors: []testtaskassignments.ReleaseNativePredecessorAuthority{{
			ServiceID: ids.NewAt(ids.KindService, claim.Task.Record.CreatedAt, 9002),
		}},
	}
	authorityDigest, err := testtaskassignments.ReleaseRestorationAuthoritySHA256(authority)
	if err != nil {
		t.Fatal(err)
	}
	primary := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
		ReconciliationRequired: true, ExecutionEpoch: record.ExecutionEpoch,
	}
	reportDigest, err := testtaskassignments.CanonicalPrimaryReportSHA256(testtaskjournal.TaskStatusFailed, primary)
	if err != nil {
		t.Fatal(err)
	}
	recovery := testtaskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: record.TaskID, AssignmentID: record.AssignmentID,
		OperationID: claim.Task.Record.OperationID, PlanHash: claim.Task.Record.PlanHash,
		RestorationAuthoritySHA256: authorityDigest, PrimaryReportSHA256: reportDigest,
		PrimaryStatus: testtaskjournal.TaskStatusFailed, PrimaryResult: primary,
		RecoveryDeadline: record.RecoveryDeadline,
		RecoveryStepIDs: []string{
			ids.NewAt(ids.KindStep, claim.Task.Record.CreatedAt, 9004),
			ids.NewAt(ids.KindStep, claim.Task.Record.CreatedAt, 9005),
		},
		Phase: testtaskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: claim.Task.ReadRevision,
	}
	recoveryDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(recovery)
	if err != nil {
		t.Fatal(err)
	}
	record.ExecutionMode = testtaskassignments.TaskExecutionModeRecoveryOnly
	record.ExecutionEpoch++
	record.RestorationAuthority = &authority
	record.RestorationAuthoritySHA256 = authorityDigest
	record.ReleaseRecoveryRecordSHA256 = recoveryDigest
	assignmentValue, err := testtaskassignments.EncodeTaskAssignment(record)
	if err != nil {
		t.Fatal(err)
	}
	recoveryValue, err := testtaskassignments.EncodeReleaseRecoveryRecord(recovery)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testtaskjournal.TaskAssignmentKey(record.AgentID, record.TaskID), ModRevision: claim.Assignment.Revision},
		{Key: testtaskjournal.TaskAssignmentIndexKey(record.TaskID), ModRevision: claim.Assignment.Revision},
		{
			Key:         testtaskjournal.TaskTimeoutIndexKey(record.TaskID, record.Deadline),
			ModRevision: claim.Assignment.Revision,
		},
		{Key: testtaskjournal.TaskTimeoutIndexKey(record.TaskID, record.RecoveryDeadline)},
		{Key: testtaskassignments.ReleaseRecoveryKey(record.TaskID)},
	}, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskAssignmentKey(record.AgentID, record.TaskID),
			Value: assignmentValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskAssignmentIndexKey(record.TaskID),
			Value: assignmentValue,
		},
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskTimeoutIndexKey(record.TaskID, record.Deadline)},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskTimeoutIndexKey(record.TaskID, record.RecoveryDeadline),
			Value: assignmentValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskassignments.ReleaseRecoveryKey(record.TaskID),
			Value: recoveryValue,
		},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed recovery assignment = %#v, %v", transaction, err)
	}
	return record
}
