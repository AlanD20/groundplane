package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ProveMixedMemberRecovery starts with the real producer's claimed Task. The
// supplied terminal evidence exercises persistence, not Docker observation.
func (fixture *ExecutedArtifactFixture) ProveMixedMemberRecovery(t *testing.T, agentID string,
	claim TaskAssignment, plan *agentpb.ExecutionPlan, servingServiceID, priorReleaseID string,
	runRecovery ...func(TaskAssignment, testtaskjournal.TaskResultRecord) testtaskjournal.TaskResultRecord,
) {
	t.Helper()
	ctx := context.Background()
	task, assignment := claim.Task.Record, claim.Assignment.Record
	authority := assignment.RestorationAuthority
	procedure := plan.GetCandidateReleaseProcedure()
	if authority == nil || len(authority.Candidates) != 2 || len(procedure.GetMembers()) != 2 ||
		len(authority.NativePredecessors) != 2 || authority.AppliedPredecessor == nil {
		t.Fatal("real mixed producer did not publish two members, native predecessors, and an applied witness")
	}
	for _, member := range authority.Candidates {
		want := testtaskassignments.ReleaseRestorationCandidateAbsence
		if member.ServiceID == servingServiceID {
			want = testtaskassignments.ReleaseRestorationServingPredecessor
		}
		if member.Target != want {
			t.Fatalf("member %s target %s, want %s", member.ServiceID, member.Target, want)
		}
	}
	if _, err := testtaskassignments.OpenRestorationWitness(task.Target, authority.AppliedPredecessor.ComposeArtifact); err != nil {
		t.Fatal(err)
	}
	witness := mixedNativeRestorationWitness(t, authority, servingServiceID)
	appliedKey := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(task.Target)
	servingKey := testreleases.ReleaseProjectionKey(servingServiceID)
	retained, err := fixture.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{appliedKey, servingKey}})
	if err != nil || retained.Values[0] == nil || retained.Values[1] == nil {
		t.Fatalf("missing applied/serving witness: %v", err)
	}
	if retained.Values[0].ModRevision != authority.AppliedPredecessor.KeyRevision {
		t.Fatal("claimed witness revision differs from applied authority")
	}
	forwardOrdinal := uint64(1)
	for _, member := range procedure.GetMembers() {
		for _, state := range []testtaskjournal.TaskEventState{testtaskjournal.TaskEventStateRunning, testtaskjournal.TaskEventStateCompleted} {
			_, err := fixture.Tasks.AppendTaskEvent(
				ctx,
				testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
					AssignmentID: assignment.AssignmentID, AgentID: agentID, AgentGeneration: 1, TaskID: task.ID,
					StepID: member.GetForwardStepIds()[0], Attempt: 1, Ordinal: forwardOrdinal,
				}, State: state, Payload: json.RawMessage(`{"message":"candidate forward"}`)},
				task.CreatedAt.Add(time.Duration(1000+100*forwardOrdinal)*time.Millisecond),
			)
			if err != nil {
				t.Fatalf("forward event: %v", err)
			}
			forwardOrdinal++
		}
	}
	failedHealthStep := ""
	for _, step := range plan.GetSteps() {
		if step.GetWaitHealthy() != nil {
			failedHealthStep = step.GetStepId()
		}
	}
	if failedHealthStep == "" {
		t.Fatal("real mixed candidate plan has no health gate")
	}
	primary := testtaskjournal.TaskResultRecord{
		Kind:                   testtaskjournal.TaskResultCompose,
		Diagnostic:             testtaskjournal.TaskResultDiagnosticComposeFailed,
		ExitCode:               17,
		FailedStepID:           failedHealthStep,
		ReconciliationRequired: true,
		ExecutionEpoch:         1,
	}
	transitioned, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID,
		testtaskjournal.TaskStatusFailed,
		primary,
		task.CreatedAt.Add(3*time.Second),
	)
	if err != nil || transitioned.Record.Status != testtaskjournal.TaskStatusRunning ||
		transitioned.Record.Result != nil {
		t.Fatalf("mixed recovery transition: status=%s error=%v", transitioned.Record.Status, err)
	}
	// Reopen the real repository and use the reconnect listing path. Neither a
	// newer target choice nor a new Task/assignment may appear on reconstruction.
	reopened, err := NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	reconnected, err := reopened.ListAgentAssignments(ctx, agentID, 1, 1)
	if err != nil || len(reconnected) != 1 {
		t.Fatalf("reconnect: %d %v", len(reconnected), err)
	}
	recovery := reconnected[0]
	if recovery.Assignment.Record.AssignmentID != assignment.AssignmentID || recovery.Task.Record.ID != task.ID ||
		recovery.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly || recovery.Assignment.Record.ExecutionEpoch != 2 ||
		recovery.ReleaseRecovery == nil || recovery.Assignment.Record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 {
		t.Fatal("reconnect changed mixed recovery identity or selection")
	}
	beforeAuthority, err := testtaskassignments.ReleaseRestorationAuthoritySHA256(*authority)
	if err != nil {
		t.Fatal(err)
	}
	afterAuthority, err := testtaskassignments.ReleaseRestorationAuthoritySHA256(
		*recovery.Assignment.Record.RestorationAuthority,
	)
	if err != nil || beforeAuthority != afterAuthority {
		t.Fatal("reconnect replaced the original member map/witness")
	}
	beforeReplay := fixture.ReadRevision()
	if replay, err := reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusFailed, primary, task.CreatedAt.Add(4*time.Second)); err != nil ||
		replay.Record.Status != testtaskjournal.TaskStatusRunning ||
		fixture.ReadRevision() != beforeReplay {
		t.Fatalf("exact primary replay was not read-only: %v", err)
	}
	changedPrimary := primary
	changedPrimary.ExitCode++
	if _, err := reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusFailed, changedPrimary, task.CreatedAt.Add(4*time.Second)); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("changed old-epoch report accepted: %v", err)
	}
	wantSteps, err := releaseRestorationStepIDs(procedure, authority.Candidates)
	if err != nil || !slices.Equal(wantSteps, recovery.ReleaseRecovery.StepIDs) {
		t.Fatalf("mixed recovery procedure diverges: %v", err)
	}
	if len(recovery.ReleaseRecovery.ApplicableCompensationStepIDs) != 2 {
		t.Fatal("mixed recovery lost a touched member's compensation obligation")
	}
	final := mixedRecoveryResult(t, recovery, procedure, witness, servingServiceID, priorReleaseID)
	if len(runRecovery) != 0 {
		final = runRecovery[0](recovery, final)
	} else {
		ordinal := uint64(1)
		for _, stepID := range recovery.ReleaseRecovery.StepIDs {
			for _, state := range []testtaskjournal.TaskEventState{testtaskjournal.TaskEventStateRunning, testtaskjournal.TaskEventStateCompleted} {
				_, err := reopened.AppendTaskEvent(ctx, testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
					AssignmentID: assignment.AssignmentID, AgentID: agentID, AgentGeneration: 1, TaskID: task.ID,
					StepID: stepID, Attempt: 2, Ordinal: ordinal,
				}, State: state, Payload: json.RawMessage(`{"message":"mixed recovery"}`)}, task.CreatedAt.Add(time.Duration(4+ordinal)*time.Second))
				if err != nil {
					t.Fatalf("mixed recovery event %s/%s: %v", stepID, state, err)
				}
				ordinal++
			}
		}
	}
	for _, mutate := range []func(*testtaskjournal.TaskResultRecord){
		func(r *testtaskjournal.TaskResultRecord) { r.CandidateAbsenceEvidence = nil },
		func(r *testtaskjournal.TaskResultRecord) {
			r.CandidateAbsenceEvidence.Candidates = append(r.CandidateAbsenceEvidence.Candidates, testtaskjournal.TaskCandidateAbsenceCandidate{ServiceID: servingServiceID, ReleaseID: priorReleaseID})
		},
		func(r *testtaskjournal.TaskResultRecord) { r.ProxyEvidence, r.RecreateEvidence = nil, nil },
		func(r *testtaskjournal.TaskResultRecord) {
			changedDigest := []byte(r.CandidateAbsenceEvidence.AuthoritySHA256)
			if changedDigest[0] == '0' {
				changedDigest[0] = '1'
			} else {
				changedDigest[0] = '0'
			}
			r.CandidateAbsenceEvidence.AuthoritySHA256 = string(changedDigest)
		},
	} {
		changed := testtaskjournal.CloneTaskResult(&final)
		mutate(changed)
		beforeAttempt := fixture.ReadRevision()
		if _, err := reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, *changed, task.CreatedAt.Add(30*time.Second)); !isKind(
			err,
			errs.KindStateConflict,
		) &&
			!isKind(err, errs.KindValidationFailed) {
			t.Fatalf("incomplete or mismatched mixed proof accepted: %v", err)
		}
		if fixture.ReadRevision() != beforeAttempt {
			t.Fatal("rejected mixed proof changed durable state")
		}
	}
	var terminal testkeyvalue.Versioned[TaskRecord]
	if len(plan.ScriptBodyArtifacts) != 0 {
		terminal = fixture.ProveRecoveryHookTerminalReconnect(
			t,
			agentID,
			recovery,
			final,
			task.CreatedAt.Add(31*time.Second),
		)
	} else {
		terminal, err = reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, final, task.CreatedAt.Add(31*time.Second))
	}
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed || terminal.Record.Result == nil ||
		terminal.Record.Result.ExitCode != primary.ExitCode || terminal.Record.Result.FailedStepID != primary.FailedStepID ||
		terminal.Record.Result.Diagnostic != primary.Diagnostic || terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("mixed final primary failure not retained: status=%s err=%v", terminal.Record.Status, err)
	}
	after, err := fixture.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{appliedKey, servingKey}})
	if err != nil {
		t.Fatal(err)
	}
	for index, original := range retained.Values {
		if after.Values[index] == nil || after.Values[index].ModRevision != original.ModRevision ||
			!bytes.Equal(after.Values[index].Value, original.Value) {
			t.Fatal("failed mixed candidate changed applied/serving authority")
		}
	}
	cleanup, err := fixture.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskAssignmentKey(agentID, task.ID),
				testtaskjournal.TaskAssignmentIndexKey(task.ID),
				testtaskjournal.TaskActiveOperationKey(task.OperationID),
				testtaskjournal.TaskTimeoutIndexKey(task.ID, recovery.Assignment.Record.RecoveryDeadline),
				testtaskjournal.TaskMaterializationWriterKey(task.Target),
				testtaskassignments.ReleaseRecoveryKey(task.ID),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range cleanup.Values {
		if value != nil {
			t.Fatal("mixed recovery retained an execution fence")
		}
	}
	replay, err := reopened.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		final,
		task.CreatedAt.Add(32*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision || replay.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("mixed terminal replay: %v", err)
	}
	changed := testtaskjournal.CloneTaskResult(&final)
	changed.CandidateAbsenceEvidence.Candidates[0].ReleaseID = priorReleaseID
	if _, err := reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, *changed, task.CreatedAt.Add(33*time.Second)); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("changed mixed terminal replay accepted: %v", err)
	}
}

func mixedNativeRestorationWitness(
	t *testing.T,
	authority *testtaskassignments.ReleaseRestorationAuthority,
	servingServiceID string,
) *agentpb.ComposeArtifact {
	t.Helper()
	found := false
	var encoded []byte
	for _, predecessor := range authority.NativePredecessors {
		if predecessor.ServiceID == servingServiceID {
			if found {
				t.Fatal("mixed authority repeated the serving native predecessor")
			}
			found = true
			encoded = predecessor.CurrentArtifact
		}
	}
	if !found || len(encoded) == 0 {
		t.Fatal("mixed authority omitted the serving native predecessor")
	}
	if authority.AppliedPredecessor != nil && bytes.Equal(encoded, authority.AppliedPredecessor.ComposeArtifact) {
		t.Fatal("mixed fixture did not distinguish native and applied predecessor artifacts")
	}
	witness, err := testtaskassignments.OpenRestorationWitness(authority.EnvironmentID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return witness
}

func mixedRecoveryResult(t *testing.T, recovery TaskAssignment, procedure *agentpb.CandidateReleaseProcedure,
	witness *agentpb.ComposeArtifact, servingServiceID, priorReleaseID string,
) testtaskjournal.TaskResultRecord {
	t.Helper()
	assignment := recovery.Assignment.Record
	authority := assignment.RestorationAuthority
	result := testtaskjournal.TaskResultRecord{
		Kind:                        testtaskjournal.TaskResultCompose,
		Diagnostic:                  testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch:              2,
		ReleaseRecoveryRecordSHA256: assignment.ReleaseRecoveryRecordSHA256,
		CandidateAbsenceEvidence: &testtaskjournal.TaskCandidateAbsenceEvidence{
			AssignmentID: assignment.AssignmentID, PlanHash: authority.PlanHash,
			AuthoritySHA256: assignment.RestorationAuthoritySHA256, CandidateArtifactID: authority.CandidateArtifactID,
			AbsenceProven: true,
		},
	}
	for index, member := range authority.Candidates {
		if member.Target == testtaskassignments.ReleaseRestorationCandidateAbsence {
			result.CandidateAbsenceEvidence.ComposeProjectName = procedure.GetMembers()[index].GetCandidateAbsence().
				GetComposeProjectName()
			result.CandidateAbsenceEvidence.Candidates = append(
				result.CandidateAbsenceEvidence.Candidates,
				testtaskjournal.TaskCandidateAbsenceCandidate{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID},
			)
		}
	}
	var proxy *agentpb.ComposeService
	for _, service := range witness.Services {
		if service.ServiceId == servingServiceID &&
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			proxy = service
		}
	}
	if proxy != nil {
		generation, err := executionplan.ProxyConfigGeneration(proxy.ProxyConfigJson, priorReleaseID)
		if err != nil {
			t.Fatal(err)
		}
		result.ProxyEvidence = []testtaskjournal.TaskProxyEvidence{
			{ServiceID: servingServiceID, ReleaseID: priorReleaseID,
				Target: "singleton", Compensated: true, ProxyGeneration: generation, ConfigSHA256: hex.EncodeToString(proxy.ProxyConfigSha256)},
		}
	} else {
		result.RecreateEvidence = []testtaskjournal.TaskRecreateEvidence{{ServiceID: servingServiceID, ReleaseID: priorReleaseID,
			Target: "singleton", Compensated: true, ArtifactID: witness.ArtifactId}}
	}
	return result
}
