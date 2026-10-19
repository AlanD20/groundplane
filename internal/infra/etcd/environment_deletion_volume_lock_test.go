package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the shared hierarchy engine's Environment finalizer must retain the
// same lock fence as the Task acknowledgement path, even at the mutation commit.
func TestHierarchyEnvironmentFinalizerExcludesVolumeRemovalLock(t *testing.T) {
	for _, mode := range []string{"unlocked", "held", "late"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fixture := newEnvironmentFinalizerFixture(t)
			backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: fixture.store}
			owner := removalrecord.Owner{EnvironmentID: fixture.environment.Record.ID,
				VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)}
			if mode == "held" {
				putSpecializedRemovalLock(t, fixture.store, owner)
			} else if mode == "late" {
				backend.owner = &owner
			}
			before := fixture.store.revision
			effects, err := testhierarchydeletionfinalization.NewPreparer(backend).Prepare(
				ctx, fixture.operation, testhierarchydeletion.HierarchyDeletionAction{
					ActionKind: testhierarchydeletion.HierarchyDeletionEnvironmentFinalize,
					TargetID:   owner.EnvironmentID, TargetRevision: fixture.environment.Revision,
				},
			)
			defer testkeyvalue.ClearByteSlices(effects.Values())
			if mode == "held" {
				if !isKind(err, errs.KindStateConflict) || fixture.store.revision != before {
					t.Fatalf("hierarchy finalizer ignored held Volume removal lock: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := backend.Transact(ctx, effects.Conditions(), effects.Mutations())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unlocked" {
				if !result.Succeeded {
					t.Fatal("unlocked hierarchy finalizer rejected")
				}
				assertEnvironmentMetadataPresence(
					t,
					fixture.store,
					testhierarchy.EnvironmentKey(owner.EnvironmentID),
					false,
				)
				return
			}
			if result.Succeeded || fixture.store.revision != before+1 {
				t.Fatal("hierarchy finalizer committed over late Volume removal ownership")
			}
			assertEnvironmentMetadataPresence(
				t,
				fixture.store,
				testhierarchy.EnvironmentKey(owner.EnvironmentID),
				true,
			)
		})
	}
}
