package runtimeconfiguration

import (
	"context"
	"testing"
)

// SVC-15: retained source identity outlives its original read revision. Reading
// another newly staged snapshot must not substitute that newer file set.
func TestRetainedConfigurationLoadUsesExactImmutableReference(t *testing.T) {
	t.Parallel()
	store := newMemoryStore()
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	first := fixtureSnapshot(1, fixtureRecords())
	prior, err := repository.Stage(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	next := fixtureSnapshot(2, fixtureRecords())
	next.Files[0].UID++
	if _, err := repository.Stage(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.LoadRetained(context.Background(), prior)
	if err != nil || loaded.ID != first.ID || loaded.Generation != 1 ||
		loaded.Files[0].UID != first.Files[0].UID || len(loaded.Files) != len(first.Files) {
		t.Fatalf("retained load selected a newer source: %#v/%v", loaded, err)
	}
	prior.SHA256 = next.Files[0].SHA256
	if _, err := repository.LoadRetained(context.Background(), prior); err == nil {
		t.Fatal("changed snapshot digest was accepted")
	}
}
