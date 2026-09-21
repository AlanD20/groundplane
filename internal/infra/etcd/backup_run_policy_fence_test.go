package etcd

import (
	"context"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"testing"
)

// Rationale: a manual run sealed from policy P1 must not publish after an
// atomic policy/coordination transition to P2, even when its ordinary mutation
// epoch snapshot has not otherwise changed.
func TestManualBackupRunPublicationFencesConcurrentPolicyTransition(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	plan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()

	policy := mustOptionalKey(t, store, testbackuppolicy.BackupPolicyKey(run.EnvironmentID))
	coordination := mustOptionalKey(t, store, testenvironmentcoordination.Key(run.EnvironmentID))
	if policy == nil || coordination == nil {
		t.Fatal("manual Backup fixture has no policy coordination")
	}
	transition, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: policy.Key, ModRevision: policy.ModRevision},
		{Key: coordination.Key, ModRevision: coordination.ModRevision},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: policy.Key, Value: append([]byte(nil), policy.Value...)},
		{Type: testkeyvalue.MutationPut, Key: coordination.Key, Value: append([]byte(nil), coordination.Value...)},
	})
	if err != nil || !transition.Succeeded {
		t.Fatalf("replace policy while manual Backup is prepared = %#v, %v", transition, err)
	}

	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict == nil {
		t.Fatalf("stale manual Backup publication conflict/error = %#v/%v", conflict, classifyErr)
	}
}
