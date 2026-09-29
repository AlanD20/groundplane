package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// SVC05: interruption can follow partial terminal publication. Proven absence
// must remove stale serving authority, preserve history, and remain proof-fenced.
func TestRestoredReleaseSettlesPublishedCompletionWithoutRewritingHistory(t *testing.T) {
	f := newReleaseTerminalFixture(t, 2)
	ctx := context.Background()
	for count := 0; count < 4; count++ {
		processed, err := f.finalize(t, testtaskjournal.TaskStatusCompleted)
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	repository, err := newTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	member := f.head.Members[0]
	keys := []string{testreleases.ReleaseTerminalKey(member.ReleaseID),
		testreleases.ReleaseProjectionKey(member.ServiceID), serviceruntimerecord.Key(member.ServiceID),
		testreleases.ReleaseOperationKey(f.task.OperationID)}
	before, err := f.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil || before.Values[0] == nil || before.Values[2] == nil {
		t.Fatalf("completed fixture: %v", err)
	}
	f.assignment.ExecutionMode = testtaskassignments.TaskExecutionModeRecoveryOnly
	f.assignment.RestorationAuthority = &testtaskassignments.ReleaseRestorationAuthority{}
	for _, candidate := range f.head.Members {
		f.assignment.RestorationAuthority.Candidates = append(
			f.assignment.RestorationAuthority.Candidates,
			testtaskassignments.ReleaseRestorationCandidate{
				ServiceID: candidate.ServiceID,
				ReleaseID: candidate.ReleaseID,
				Target:    testtaskassignments.ReleaseRestorationCandidateAbsence,
			},
		)
		f.assignment.RestorationAuthority.NativePredecessors = append(
			f.assignment.RestorationAuthority.NativePredecessors,
			testtaskassignments.ReleaseNativePredecessorAuthority{ServiceID: candidate.ServiceID},
		)
	}
	proofKey := testtaskassignments.ReleaseRecoveryKey(f.task.ID)
	proof, err := f.store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: proofKey, Value: []byte("proof")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	recovery := releaseRecoveryAcknowledgement{final: true, status: testtaskjournal.TaskStatusFailed,
		value:      &testkeyvalue.KeyValue{Key: proofKey, ModRevision: proof.Revision},
		conditions: []testkeyvalue.Condition{{Key: proofKey, ModRevision: proof.Revision}},
		result: testtaskjournal.TaskResultRecord{
			Kind:       testtaskjournal.TaskResultCompose,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
		},
	}
	// No fabricated FailedStepID: reconnect, not a failed forward step, caused recovery.
	for count := 0; count < 4; count++ {
		read, err := f.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			stale := recovery
			stale.conditions = []testkeyvalue.Condition{{Key: proofKey, ModRevision: proof.Revision - 1}}
			if processed, err := repository.finalizeRestoredRelease(ctx, f.task, f.assignment, stale, f.now.Add(2*time.Minute), read.ReadRevision); err == nil ||
				processed {
				t.Fatal("stale recovery proof changed runtime authority")
			}
		}
		processed, err := repository.finalizeRestoredRelease(
			ctx,
			f.task,
			f.assignment,
			recovery,
			f.now.Add(2*time.Minute),
			read.ReadRevision,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
		if count == 3 {
			t.Fatal("restoration settlement did not converge")
		}
	}
	after, err := f.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil || !bytes.Equal(before.Values[0].Value, after.Values[0].Value) || after.Values[2] != nil {
		t.Fatalf("history changed or stale runtime survived: %v", err)
	}
	projection, err := testreleases.DecodeReleaseRecord[domain.ServiceProjection](
		after.Values[1].Value,
		"service-release-projection",
	)
	if err != nil || projection.ServingReleaseID != "" || projection.CurrentSuccessfulReleaseID != "" {
		t.Fatalf("absent candidate is still serving: %v", err)
	}
	head, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseOperationHead](
		after.Values[3].Value,
		"release-operation",
	)
	if err != nil || head.State != domain.StateFailed {
		t.Fatalf("original interrupted operation did not fail: %v", err)
	}
}

// SVC05: after partial publication overwrites applied authority, restoration
// must recover the captured predecessor bytes, never rerender current desired state.
func TestRestoredReleaseReinstatesCapturedRuntime(t *testing.T) {
	f := newReleaseTerminalFixture(t, 2)
	if _, err := f.finalize(t, testtaskjournal.TaskStatusCompleted); err != nil {
		t.Fatal(err)
	}
	member := f.head.Members[0]
	read, err := f.store.GetMany(context.Background(), testkeyvalue.GetManyRequest{Keys: []string{
		testreleases.ReleaseIntentStagingKey(
			f.head.PublicationID,
			member.ReleaseID,
		), serviceruntimerecord.Key(member.ServiceID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := testreleases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	if err != nil {
		t.Fatal(err)
	}
	intent.PriorServingReleaseID = ids.New(ids.KindDeployment)
	prior := terminalRuntimeFixture(t, f.task.Owner.EnvironmentID, member.ServiceID,
		intent.PriorServingReleaseID, f.task.PlanID, ids.New(ids.KindConfig))
	f.assignment.AgentID = f.agentID
	f.assignment.RestorationAuthority = &testtaskassignments.ReleaseRestorationAuthority{
		Candidates: []testtaskassignments.ReleaseRestorationCandidate{{ServiceID: member.ServiceID,
			ReleaseID: member.ReleaseID, Target: testtaskassignments.ReleaseRestorationServingPredecessor}},
		NativePredecessors: []testtaskassignments.ReleaseNativePredecessorAuthority{{ServiceID: member.ServiceID,
			CurrentArtifact: prior.CurrentArtifact}},
	}
	recovery := releaseRecoveryAcknowledgement{
		record: testtaskassignments.ReleaseRecoveryRecord{RecoveryStepIDs: []string{f.task.Steps[4].ID}},
		result: testtaskjournal.TaskResultRecord{ProxyEvidence: []testtaskjournal.TaskProxyEvidence{{
			ServiceID: member.ServiceID, ReleaseID: prior.ReleaseID, Target: prior.Target, Compensated: true,
		}}},
	}
	mutation, err := restoredReleaseRuntimeMutation(
		f.task,
		f.assignment,
		recovery,
		intent,
		read.Values[1],
		f.now.Add(time.Minute),
	)
	if err != nil || mutation.Type != testkeyvalue.MutationPut {
		t.Fatalf("captured runtime restoration: %v", err)
	}
	restored, err := testreleases.DecodeReleaseRecord[serviceruntimerecord.Record](
		mutation.Value,
		"service-acknowledged-runtime",
	)
	if err != nil || serviceruntimerecord.Validate(restored) != nil ||
		restored.Runtime.ReleaseID != prior.ReleaseID || !bytes.Equal(restored.Runtime.CurrentArtifact, prior.CurrentArtifact) {
		t.Fatalf("restoration lost exact predecessor authority: %v", err)
	}
	replay, err := restoredReleaseRuntimeMutation(f.task, f.assignment, recovery, intent,
		&testkeyvalue.KeyValue{Key: mutation.Key, Value: mutation.Value}, f.now.Add(2*time.Minute))
	if err != nil || replay.Key != "" {
		t.Fatalf("restored runtime replay rewrote authority: %v", err)
	}
}
