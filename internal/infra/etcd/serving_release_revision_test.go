package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: leave_active can serve a different image than the last successful
// Release. Script publication must fence the actual serving pointer and intent.
func TestResolveServingPreservesServingAuthorityAndCASRevisions(t *testing.T) {
	ctx := context.Background()
	_, memory, environment, project := serviceRepositoryTestHierarchy(t)
	fixtures := stageReleaseLogDesiredProjection(t, memory, environment, project, []string{"api"})
	serviceID := fixtures[0].ID
	installServingRelease(t, memory, environment.Record.ID, project.Record, serviceID, 2401)
	store := &releaseLogMemoryStore{memoryHierarchyStore: memory}
	ledger := testReleaseLogLedger(t, store)
	entry, err := store.Get(ctx, releaseProjectionKey(serviceID))
	if err != nil {
		t.Fatal(err)
	}
	projection, err := decodeReleaseRecord[domain.ServiceProjection](entry.Entry.Value, "service-release-projection")
	if err != nil {
		t.Fatal(err)
	}
	projection.CurrentSuccessfulReleaseID = ids.NewAt(ids.KindDeployment, serviceRecordTestTime(), 2402)
	value, err := encodeReleaseRecord("service-release-projection", projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionRevision, err := store.Put(ctx, releaseProjectionKey(serviceID), value)
	if err != nil {
		t.Fatal(err)
	}
	intentEntry, err := store.Get(ctx, releaseIntentStagingKey("", projection.ServingReleaseID))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ledger.ResolveServing(ctx, environment.Record.ID, serviceID, projectionRevision)
	if err != nil {
		t.Fatal(err)
	}
	if got.Intent.ID != projection.ServingReleaseID || got.Intent.ID == projection.CurrentSuccessfulReleaseID ||
		got.ProjectionRevision != projectionRevision || got.IntentRevision != intentEntry.Entry.ModRevision || got.Revision != projectionRevision {
		t.Fatalf("wrong serving authority or publication revisions: %+v", got)
	}
}
