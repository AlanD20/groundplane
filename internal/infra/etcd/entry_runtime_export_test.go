package etcd

import "testing"

func (fixture *ExecutedArtifactFixture) EntryRuntimeMarker(t *testing.T, task TaskRecord) IdempotencyMarker {
	t.Helper()
	marker := environmentBlueprintTestMarker(task, task.Target)
	marker.Locator.Method, marker.Locator.Route = "POST", "/entries"
	return marker
}

func (fixture *ExecutedArtifactFixture) AdvanceEntryRuntimeEpoch(t *testing.T) {
	t.Helper()
	key := environmentMutationEpochKey(fixture.Environment.Record.ID)
	current, err := fixture.store.Get(t.Context(), key)
	if err != nil || current.Entry == nil {
		t.Fatal("capture epoch", err)
	}
	if _, err := fixture.store.Put(t.Context(), key, current.Entry.Value); err != nil {
		t.Fatal(err)
	}
}
