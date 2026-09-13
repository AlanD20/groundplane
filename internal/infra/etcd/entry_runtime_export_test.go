package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// SeedEntryRuntimeIntent models completed lifecycle intent independently of the
// immutable serving Release and Blueprint retained by the Entry capture fixture.
func (fixture *ExecutedArtifactFixture) SeedEntryRuntimeIntent(
	t *testing.T, serviceID string, intent core.ServiceRuntimeIntent,
) {
	t.Helper()
	value, err := encodeServiceRuntimeRecord(ServiceRuntimeRecord{
		EnvironmentID: fixture.Environment.Record.ID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: intent},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(t.Context(), serviceRuntimeKey(serviceID), value); err != nil {
		t.Fatal(err)
	}
	fixture.AdvanceEntryRuntimeEpoch(t)
}

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
