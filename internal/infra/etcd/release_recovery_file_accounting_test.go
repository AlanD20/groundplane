package etcd

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: SVC-15/JOURNEY-02 recovery after reconnect may compensate only a
// file whose forward write reached durable Running evidence. An untouched file
// and host state cannot create that authority.
func TestReleaseFileRunningEvidenceSurvivesRepositoryRestart(t *testing.T) {
	ctx := context.Background()
	at := taskJournalTime()
	task := validTaskRecord(at)
	store := newMemoryTaskStore()
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	appended, err := repository.AppendTaskEvent(ctx, taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning),
		at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, appended.Revision)
	if err != nil {
		t.Fatal(err)
	}
	secondForward := ids.NewAt(ids.KindStep, at, 6)
	firstProbe, secondProbe := ids.NewAt(ids.KindStep, at, 7), ids.NewAt(ids.KindStep, at, 8)
	firstCompensate, secondCompensate := ids.NewAt(ids.KindStep, at, 9), ids.NewAt(ids.KindStep, at, 10)
	procedure := &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: &agentpb.ConfigurationRestoration{
		Files: []*agentpb.ConfigurationFileRestoration{
			{ForwardStepId: taskJournalStepID(), ProbeStepId: firstProbe, CompensateStepId: firstCompensate},
			{ForwardStepId: secondForward, ProbeStepId: secondProbe, CompensateStepId: secondCompensate},
		},
	}}
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: taskEventTestAssignmentID, AgentID: taskEventTestAgentID,
		AgentGeneration: 1, ExecutionEpoch: 1,
	}
	effect, evidence, err := repository.releaseCandidateMutationEvidenceAtRevision(
		ctx,
		snapshot.Task,
		assignment,
		procedure,
		appended.Revision,
	)
	if err != nil || !effect || !slices.Equal(evidence, []testtaskassignments.ReleaseRecoveryMutationEvidence{{
		StepID: taskJournalStepID(), Running: true,
	}}) {
		t.Fatalf("durable file evidence = %#v, %t, %v", evidence, effect, err)
	}

	primary := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, ExitCode: 1, Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	primaryDigest, err := testtaskassignments.CanonicalPrimaryReportSHA256(testtaskjournal.TaskStatusFailed, primary)
	if err != nil {
		t.Fatal(err)
	}
	steps := executionplan.RecoveryStepIDs(procedure)
	deadline := at.Add(time.Hour)
	record := testtaskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, RestorationAuthoritySHA256: strings.Repeat("b", 64),
		PrimaryReportSHA256: primaryDigest, PrimaryStatus: testtaskjournal.TaskStatusFailed, PrimaryResult: primary,
		RecoveryDeadline: deadline, MutationEvidence: evidence, RecoveryStepIDs: steps,
		Phase: testtaskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: appended.Revision,
	}
	recoveryValue, err := testtaskassignments.EncodeReleaseRecoveryRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	seedTaskRepositoryValue(t, store, testtaskassignments.ReleaseRecoveryKey(task.ID), recoveryValue)
	recoveryDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	assignment.ExecutionMode = testtaskassignments.TaskExecutionModeRecoveryOnly
	assignment.RestorationAuthority = &testtaskassignments.ReleaseRestorationAuthority{}
	assignment.RestorationAuthoritySHA256 = record.RestorationAuthoritySHA256
	assignment.RecoveryDeadline = deadline
	assignment.ReleaseRecoveryRecordSHA256 = recoveryDigest
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	directive, err := restarted.releaseRecoveryDirectiveAtRevision(
		ctx,
		snapshot.Task,
		assignment,
		procedure,
		store.currentRevision(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(directive.StepIDs, []string{firstProbe, secondProbe, firstCompensate, secondCompensate}) ||
		!slices.Equal(directive.ApplicableCompensationStepIDs, []string{firstCompensate}) {
		t.Fatalf("restarted file recovery directive = %#v", directive)
	}
	progress := record
	for index, stepID := range steps {
		progress, _, err = testtaskassignments.AdvanceReleaseRecoveryRecord(progress, testtaskjournal.TaskEventInput{
			Identity: testtaskjournal.TaskEventIdentity{StepID: stepID}, State: testtaskjournal.TaskEventStateCompleted,
		}, store.currentRevision()+int64(index)+1)
		if err != nil {
			t.Fatal(err)
		}
		if index+1 == len(steps)/2 && progress.Phase != testtaskassignments.ReleaseRecoveryPhaseCompensate {
			t.Fatalf("file recovery phase at compensation boundary = %s", progress.Phase)
		}
	}
	if progress.Phase != testtaskassignments.ReleaseRecoveryPhaseProven || int(progress.Cursor) != len(steps) {
		t.Fatalf("terminal file recovery progress = %#v", progress)
	}
}

// Rationale: SVC-15 rejects foreign, reordered, or non-Running evidence rather
// than letting an observation outside the sealed journal authorize a write.
func TestReleaseFileCompensationRejectsUnsealedEvidence(t *testing.T) {
	at := taskJournalTime()
	forwardA, forwardB := ids.NewAt(ids.KindStep, at, 20), ids.NewAt(ids.KindStep, at, 21)
	procedure := &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: &agentpb.ConfigurationRestoration{
		Files: []*agentpb.ConfigurationFileRestoration{
			{ForwardStepId: forwardA, CompensateStepId: ids.NewAt(ids.KindStep, at, 22)},
			{ForwardStepId: forwardB, CompensateStepId: ids.NewAt(ids.KindStep, at, 23)},
		},
	}}
	for _, test := range []struct {
		name     string
		evidence []testtaskassignments.ReleaseRecoveryMutationEvidence
	}{
		{"foreign", []testtaskassignments.ReleaseRecoveryMutationEvidence{{StepID: ids.NewAt(ids.KindStep, at, 24), Running: true}}},
		{"reordered", []testtaskassignments.ReleaseRecoveryMutationEvidence{
			{StepID: forwardB, Running: true}, {StepID: forwardA, Running: true},
		}},
		{"not running", []testtaskassignments.ReleaseRecoveryMutationEvidence{{StepID: forwardA, Completed: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := releaseApplicableCompensationStepIDs(procedure, nil, test.evidence); err == nil {
				t.Fatal("unsealed file evidence accepted")
			}
		})
	}
}
