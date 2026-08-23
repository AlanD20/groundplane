package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
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

	first, err := repository.EnsureBackupSource(
		ctx,
		environment,
		project,
		core.BackupSourceConfig,
		environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(first) error = %v", err)
	}
	replayed, err := repository.EnsureBackupSource(
		ctx,
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

	stored, err := repository.GetBackupSource(ctx, first.Record.ID)
	if err != nil || stored.Record != first.Record {
		t.Fatalf("GetBackupSource() = %#v, %v", stored, err)
	}
	page, err := repository.ListBackupSources(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 20},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.ID != first.Record.ID {
		t.Fatalf("ListBackupSources() = %#v, %v", page, err)
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
) (*BackupPolicyRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	_, store, environment, project := componentRepositoryTestHierarchy(t)
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	return repository, store, environment, project
}
