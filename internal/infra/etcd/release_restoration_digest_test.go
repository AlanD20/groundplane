package etcd

import (
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	reflect "reflect"
	strings "strings"
	testing "testing"
	time "time"
)

func TestReleaseRecoveryAuthorityDigestAndCanonicalProgress(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 15, 0, 0, 0, time.UTC)
	probeA, probeB := ids.NewAt(ids.KindStep, at, 1), ids.NewAt(ids.KindStep, at, 2)
	compensateA, compensateB := ids.NewAt(ids.KindStep, at, 3), ids.NewAt(ids.KindStep, at, 4)
	forwardA, forwardB := ids.NewAt(ids.KindStep, at, 8), ids.NewAt(ids.KindStep, at, 9)
	procedure := &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{
		{
			ForwardStepIds:   []string{forwardA},
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{ProbeStepId: probeA, CompensateStepId: compensateA},
		},
		{
			ForwardStepIds:   []string{forwardB},
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{ProbeStepId: probeB, CompensateStepId: compensateB},
		},
	}}
	candidates := make([]testtaskassignments.ReleaseRestorationCandidate, len(procedure.Members))
	for index, member := range procedure.Members {
		member.ServiceId = ids.NewAt(ids.KindService, at, int64(20+index))
		member.CandidateReleaseId = ids.NewAt(ids.KindDeployment, at, int64(30+index))
		candidates[index] = testtaskassignments.ReleaseRestorationCandidate{
			ServiceID: member.ServiceId,
			ReleaseID: member.CandidateReleaseId,
			Target:    testtaskassignments.ReleaseRestorationCandidateAbsence,
		}
	}
	steps, err := releaseRestorationStepIDs(procedure, candidates)
	if err != nil || !reflect.DeepEqual(steps, []string{probeA, probeB, compensateB, compensateA}) {
		t.Fatalf("canonical recovery steps = %v, %v", steps, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:                   testtaskjournal.TaskResultCompose,
		ExitCode:               1,
		Diagnostic:             testtaskjournal.TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	report, err := testtaskassignments.CanonicalPrimaryReportSHA256(testtaskjournal.TaskStatusFailed, result)
	if err != nil {
		t.Fatal(err)
	}
	record := testtaskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: ids.NewAt(ids.KindTask, at, 5), AssignmentID: ids.NewAt(ids.KindAssignment, at, 6),
		OperationID: ids.NewAt(ids.KindOperation, at, 7), PlanHash: strings.Repeat("1", 64),
		RestorationAuthoritySHA256: strings.Repeat("2", 64), PrimaryReportSHA256: report,
		PrimaryStatus: testtaskjournal.TaskStatusFailed, PrimaryResult: result, RecoveryDeadline: at.Add(time.Hour),
		MutationEvidence: []testtaskassignments.ReleaseRecoveryMutationEvidence{
			{StepID: forwardB, Running: true, Completed: true},
		}, RecoveryStepIDs: steps,
		Phase: testtaskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: 9,
	}
	digest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	applicable, err := releaseApplicableCompensationStepIDs(procedure, candidates, record.MutationEvidence)
	if err != nil || !reflect.DeepEqual(applicable, []string{compensateB}) {
		t.Fatalf("recorded mutation compensation = %v, %v", applicable, err)
	}
	changedEvidence := record
	changedEvidence.MutationEvidence = []testtaskassignments.ReleaseRecoveryMutationEvidence{
		{StepID: forwardB, Running: true},
	}
	changedEvidenceDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(changedEvidence)
	if err != nil || changedEvidenceDigest == digest {
		t.Fatalf("changed mutation evidence digest = %q, %v", changedEvidenceDigest, err)
	}
	changedDeadline := record
	changedDeadline.RecoveryDeadline = changedDeadline.RecoveryDeadline.Add(time.Nanosecond)
	changedDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(changedDeadline)
	if err != nil || changedDigest == digest {
		t.Fatalf("changed recovery deadline digest = %q, %v", changedDigest, err)
	}
	for index, stepID := range steps {
		next, changed, advanceErr := testtaskassignments.AdvanceReleaseRecoveryRecord(
			record,
			testtaskjournal.TaskEventInput{
				Identity: testtaskjournal.TaskEventIdentity{
					StepID: stepID,
				}, State: testtaskjournal.TaskEventStateCompleted,
			},
			int64(10+index),
		)
		if advanceErr != nil || !changed {
			t.Fatalf("advance %d = %#v, %v", index, next, advanceErr)
		}
		nextDigest, digestErr := testtaskassignments.ReleaseRecoveryRecordSHA256(next)
		if digestErr != nil || nextDigest != digest {
			t.Fatalf("progress digest = %q, %v, want %q", nextDigest, digestErr, digest)
		}
		record = next
	}
	if record.Phase != testtaskassignments.ReleaseRecoveryPhaseProven || int(record.Cursor) != len(steps) {
		t.Fatalf("terminal recovery progress = %#v", record)
	}
}

func TestTaskAssignmentRequiresEpochModeDeadlinesAndRecoveryDigest(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	record := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 1), TaskID: ids.NewAt(ids.KindTask, at, 2),
		Executor: testtaskjournal.TaskExecutorAgent, AgentID: ids.NewAt(ids.KindAgent, at, 3), AgentGeneration: 1,
		ClaimedTaskRevision: 4, AssignedAt: at, Deadline: at.Add(time.Minute), RecoveryDeadline: at.Add(2 * time.Minute),
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err != nil {
		t.Fatalf("forward assignment rejected: %v", err)
	}
	record.ExecutionMode = testtaskassignments.TaskExecutionModeRecoveryOnly
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without recovery record digest")
	}
	record.ReleaseRecoveryRecordSHA256 = strings.Repeat("3", 64)
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without restoration authority")
	}
}
