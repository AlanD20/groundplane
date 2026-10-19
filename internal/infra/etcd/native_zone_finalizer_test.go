package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OWN-05/CMP-04: Zone metadata cleanup must compare the native Environment
// writer claim, including acquisition after preparation. Captured desired Zone
// metadata and prior physical cleanup are supplied; this is not host deletion.
func TestNativeZoneFinalizerFencesEnvironmentConfigurationWriter(t *testing.T) {
	for _, state := range []string{"idle", "already active", "claimed after preparation"} {
		t.Run(state, func(t *testing.T) {
			store := newMemoryHierarchyStore()
			hierarchy, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			_, environment := createEnvironmentBlueprintOwners(t, hierarchy)
			zoneID, otherID := ids.New(ids.KindNetwork), ids.New(ids.KindNetwork)
			projection := withTestEnvironmentComposeArtifact(environmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environment.Record.ID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
				DesiredZones: []environmentprojection.EnvironmentZoneProjection{{
					EnvironmentID: environment.Record.ID,
					Desired: core.Zone{ID: zoneID, Name: "selected", Subnet: "10.40.1.0/24",
						OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID},
				}},
			})
			head := routeRepositoryTestStageProjection(t, hierarchy, projection)
			defer clear(head)
			poolKey := networkreservations.ZonePoolRegistryKey(environment.Record.ID)
			pool, err := recordcodec.Encode("zone_pool_registry", networkreservations.ZonePoolRegistry{
				Reservations: map[string]string{zoneID: "10.40.1.0/24", otherID: "10.40.2.0/24"},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer clear(pool)
			seed, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{
				{
					Type:  keyvalue.MutationPut,
					Key:   blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
					Value: head,
				},
				{Type: keyvalue.MutationPut, Key: poolKey, Value: pool},
			})
			if err != nil || !seed.Succeeded {
				t.Fatalf("seed captured Zone metadata: %v", err)
			}
			operation := hierarchydeletion.HierarchyDeletionOperation{
				Tombstone: hierarchydeletion.HierarchyDeletionTombstone{SnapshotRevision: seed.Revision},
			}
			action := hierarchydeletion.HierarchyDeletionAction{
				ActionKind: hierarchydeletion.HierarchyDeletionZoneRemove, TargetID: zoneID, TargetRevision: seed.Revision,
			}
			claimKey := workqueue.ClaimKey(workexecution.Claim{ScopeID: environment.Record.ID})
			claim := func() {
				t.Helper()
				// Supply scheduler-owned claim presence; this finalizer does not
				// authenticate or decode the executor's claim record.
				if result, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
					Type: keyvalue.MutationPut, Key: claimKey, Value: []byte(ids.New(ids.KindWorkItem)),
				}}); err != nil || !result.Succeeded {
					t.Fatalf("acquire configuration-writer claim: %v", err)
				}
			}
			if state == "already active" {
				claim()
			}
			before := store.revision
			effects, err := hierarchydeletionfinalization.NewPreparer(store).Prepare(t.Context(), operation, action)
			if state == "already active" {
				if !isKind(err, errs.KindResourceInUse) || store.revision != before {
					t.Fatalf("active writer did not block Zone cleanup without writes: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearByteSlices(effects.Values())
			if state == "claimed after preparation" {
				claim()
			}
			result, err := store.Transact(t.Context(), effects.Conditions(), effects.Mutations())
			if err != nil || result.Succeeded != (state == "idle") {
				t.Fatalf("Zone cleanup crossed a native writer claim: %v", err)
			}
			stored, err := store.Get(t.Context(), poolKey)
			if err != nil || stored.Entry == nil {
				t.Fatalf("Zone cleanup removed unrelated subnet reservations: %v", err)
			}
			defer clear(stored.Entry.Value)
			after, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](
				stored.Entry.Value,
				"zone_pool_registry",
			)
			if err != nil || after.Reservations[otherID] != "10.40.2.0/24" ||
				(after.Reservations[zoneID] != "") != (state != "idle") {
				t.Fatalf("Zone cleanup partially retired or changed unrelated reservations: %v", err)
			}
		})
	}
}
