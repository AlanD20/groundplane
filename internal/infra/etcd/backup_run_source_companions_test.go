package etcd

import (
	context "context"
	errors "errors"
	testing "testing"

	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a generic idempotency marker cannot make a Config run replay
// successful after its immutable cursor or either direction of its reference
// pair is missing or rewritten at another revision.
func TestBackupRuntimeRepositoryCreateRunReplayRejectsChangedMembershipRevision(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	if _, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	); err != nil {
		t.Fatalf("createBackupRunForTest() error = %v", err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, membership)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: membership, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: membership, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("rewrite run membership = %#v, %v", result, err)
	}
	if _, err := repository.GetBackupRun(
		context.Background(), run.TaskID,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("GetBackupRun(changed membership) error = %v", err)
	}
}

// Rationale: cursors are meaningful only with the fixed revision whose
// snapshot they continue.
func TestBackupRuntimeRepositoryRejectsCursorWithoutRevision(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	_, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{
			Limit:          1,
			StartExclusive: testbackupruntime.BackupRunEnvironmentPrefix + run.EnvironmentID + "/" + run.TaskID,
		},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ListBackupRunsByEnvironment(cursor without revision) error = %v", err)
	}
}

// Rationale: the full twelve-source contract must remain composable after
// primary-owned derived membership removes redundant absence compares.
func TestBackupRuntimeRepositoryPreparesTwelveSourcePublicationWithinBounds(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeBareFixture(t)
	for ordinal := uint32(1); ordinal < testbackuppolicy.MaximumBackupPolicySources; ordinal++ {
		source := testBackupLaterSource(run.CreatedAt, ordinal, testbackupruntime.BackupSourceAttemptPending)
		source.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		run.Sources = append(run.Sources, source)
	}
	extendBackupRuntimePublicationSources(t, repository.store, &run)
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
		t.Fatalf("prepareBackupRunPublication(12 sources) error = %v", err)
	}
	defer plan.clear()
	if len(plan.conditions)+len(plan.mutations) > testkeyvalue.MaximumOperations {
		t.Fatalf(
			"12-source publication operations = %d, want <= %d",
			len(plan.conditions)+len(plan.mutations), testkeyvalue.MaximumOperations,
		)
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil || len(exclusions) != testbackuppolicy.MaximumBackupPolicySources {
		t.Fatalf("12-source exclusions = %d, %v", len(exclusions), err)
	}
}

// Rationale: exclusion cleanup is an all-or-none authority transition; a
// partial set cannot be accepted as replay or opportunistically repaired.
