package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBackupPolicyRepositoryEnsuresStableSourceIdentityAndPagesIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store, environment, project := componentRepositoryTestHierarchy(t)
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	repository.now = func() time.Time { return time.Date(2026, 8, 23, 13, 0, 0, 0, time.UTC) }
	initialEpoch := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)

	first, err := testbackupsources.EnsureBackupSource(
		ctx,
		store,
		repository.now,
		environment,
		project,
		core.BackupSourceConfig,
		environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(first) error = %v", err)
	}
	createdEpoch := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	if createdEpoch.Revision <= initialEpoch.Revision {
		t.Fatalf("source creation epoch revision = %d, want after %d", createdEpoch.Revision, initialEpoch.Revision)
	}
	replayed, err := testbackupsources.EnsureBackupSource(
		ctx,
		store,
		repository.now,
		environment,
		project,
		core.BackupSourceConfig,
		environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(replay) error = %v", err)
	}
	if replayed.Record.ID != first.Record.ID || replayed.Revision != first.Revision {
		t.Fatalf("replayed = %#v, want stable source %#v", replayed, first)
	}
	replayedEpoch := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	if replayedEpoch.Revision != createdEpoch.Revision {
		t.Fatalf("source replay epoch revision = %d, want unchanged %d", replayedEpoch.Revision, createdEpoch.Revision)
	}

	stored, err := testbackupsources.GetBackupSource(ctx, store, first.Record.ID)
	if err != nil || stored.Record != first.Record {
		t.Fatalf("GetBackupSource() = %#v, %v", stored, err)
	}
	page, err := testbackupsources.ListBackupSources(
		ctx,
		store,
		environment.Record.ID, testkeyvalue.PageRequest{Limit: 20},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.ID != first.Record.ID {
		t.Fatalf("ListBackupSources() = %#v, %v", page, err)
	}
}

func TestBackupPolicyRepositoryRejectsSourceCreationWhileOperationLockIsHeld(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project := backupPolicyRepositoryTestHierarchy(t)
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentOperationLockKey(environment.Record.ID), Value: []byte("held"),
	}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed operation lock = %#v, %v", transaction, err)
	}
	_, err = testbackupsources.EnsureBackupSource(
		ctx,
		store,
		repository.now,
		environment,
		project,
		core.BackupSourceConfig,
		environment.Record.ID,
	)
	if !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("EnsureBackupSource(locked) error = %v", err)
	}
}

func TestBackupPolicyRepositoryTreatsAbsentPolicyAsUnconfigured(t *testing.T) {
	t.Parallel()
	repository, _, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	policy, found, err := repository.GetBackupPolicy(context.Background(), environment.Record.ID)
	if err != nil || found || policy.ReadRevision <= 0 {
		t.Fatalf("GetBackupPolicy(absent) = %#v, %t, %v", policy, found, err)
	}
}

func backupPolicyRepositoryTestHierarchy(
	t *testing.T,
) (*BackupPolicyRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	_, store, environment, project := componentRepositoryTestHierarchy(t)
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	return repository, store, environment, project
}

func mustBackupPolicyMutationEpoch(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) testkeyvalue.Versioned[testbackupruntime.EnvironmentMutationEpochRecord] {
	t.Helper()
	result, err := store.Get(context.Background(), testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("get Environment mutation epoch = %#v, %v", result, err)
	}
	record, err := testbackupruntime.DecodeEnvironmentMutationEpochRecord(result.Entry.Value)
	if err != nil || record.EnvironmentID != environmentID {
		t.Fatalf("decode Environment mutation epoch = %#v, %v", record, err)
	}
	return testkeyvalue.Versioned[testbackupruntime.EnvironmentMutationEpochRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}
}
