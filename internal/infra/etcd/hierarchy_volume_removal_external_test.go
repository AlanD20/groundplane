package etcd_test

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: parent deletion cannot adopt a separately owned Volume removal.
// A real removal publication must also defeat deletion's final epoch CAS.
func TestHierarchyDeletionExcludesPublishedVolumeRemoval(t *testing.T) {
	for _, kind := range []testhierarchydeletion.HierarchyDeletionTargetKind{testhierarchydeletion.HierarchyDeletionTargetEnvironment, testhierarchydeletion.HierarchyDeletionTargetProject, testhierarchydeletion.HierarchyDeletionTargetTenant} {
		for _, mode := range []string{"unlocked", "held", "late", "retained", "corrupt"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture := etcd.NewVolumePolicyDesiredFixture(t)
				fixture.PrepareRemovalRecords(t)
				stageVolumePolicyDesired(t, fixture)
				publish := func() {
					result, err := fixture.Publish(ctx)
					if err != nil {
						t.Fatal(err)
					}
					outcome, _, conflict, err := result.Classify()
					if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
						t.Fatalf("removal publication: %v/%v/%v", outcome, conflict, err)
					}
				}
				backend := &hierarchyVolumePublicationRaceStore{Store: fixture.Store}
				if mode == "held" {
					publish()
				}
				if mode == "late" {
					backend.publish = publish
				}
				if mode == "retained" || mode == "corrupt" {
					// A lock cannot be inferred absent from a missing active Task.
					value, err := removalrecord.EncodeOwner(removalrecord.Owner{
						VolumeID: fixture.Task.Target, EnvironmentID: fixture.Task.Owner.EnvironmentID,
						OperationID: fixture.Task.OperationID,
					})
					if err != nil {
						t.Fatal(err)
					}
					if mode == "corrupt" {
						value = []byte("corrupt")
					}
					if _, err := fixture.Store.Put(ctx,
						removalrecord.EnvironmentLockKey(fixture.Task.Owner.EnvironmentID), value); err != nil {
						t.Fatal(err)
					}
				}
				journal, err := etcd.NewHierarchyDeletionRepository(backend)
				if err != nil {
					t.Fatal(err)
				}
				begin := fixture.ParentDeletionBegin(kind)
				before := fixture.Revision()
				_, err = journal.Begin(ctx, begin)
				if mode == "unlocked" {
					if err != nil {
						t.Fatalf("unlocked deletion: %v", err)
					}
					return
				}
				wantKind, writes := errs.KindResourceInUse, int64(0)
				if mode == "late" {
					wantKind, writes = errs.KindStateConflict, 1
				}
				if got, _ := errs.KindOf(err); got != wantKind || fixture.Revision() != before+writes {
					t.Fatalf("deletion ignored removal: %v; revision delta %d", err, fixture.Revision()-before)
				}
				marker, err := testidempotency.IdempotencyMarkerKey(begin.Marker.Locator)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{testhierarchydeletion.HierarchyDeletionTombstoneKey(string(kind), begin.TargetID), testtaskjournal.TaskStorageKey(begin.TaskID), marker} {
					read, err := fixture.Store.Get(ctx, key)
					if err != nil || read.Entry != nil {
						t.Fatalf("losing deletion wrote %s: %v", key, err)
					}
				}
			})
		}
	}
}

type hierarchyVolumePublicationRaceStore struct {
	testkeyvalue.Store

	publish func()
}

func (store *hierarchyVolumePublicationRaceStore) Transact(ctx context.Context,
	conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation) (testkeyvalue.TransactionResult, error) {
	if store.publish != nil {
		publish := store.publish
		store.publish = nil
		publish()
	}
	return store.Store.Transact(ctx, conditions, mutations)
}
