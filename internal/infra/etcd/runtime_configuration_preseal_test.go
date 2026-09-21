package etcd

import (
	"context"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
)

// SVC-15: publication must preserve the exact source set already sealed into
// recovery, and reject a changed file set or source identity instead of silently
// assigning a different snapshot after plan creation.
func TestRuntimeConfigurationPublicationPreservesPresealedSource(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	task := configurationTaskFixture()
	seed, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: []byte("seed")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRuntimeConfigurationTask(ctx, store, task, seed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := prepareRuntimeConfigurationTask(ctx, store, prepared, seed.Revision)
	if err != nil || repeated.Configuration.Current != prepared.Configuration.Current ||
		repeated.Configuration.Prior != nil || repeated.Configuration.PriorRevision != 0 {
		t.Fatalf("publication replaced sealed source: %v", err)
	}
	changed := cloneTaskRecord(prepared)
	changed.Materializations[0].UID++
	if _, err := prepareRuntimeConfigurationTask(ctx, store, changed, seed.Revision); err == nil {
		t.Fatal("changed file metadata overwrote the prepared snapshot")
	}
	changed = cloneTaskRecord(prepared)
	changed.Configuration.Current.SHA256 = "invalid"
	if _, err := prepareRuntimeConfigurationTask(ctx, store, changed, seed.Revision); err == nil {
		t.Fatal("invalid presealed snapshot authority accepted")
	}
}
