package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// OWN-05/OWN-06: Apply's inert defaults must not wedge parent-last deletion;
// configured authority and concurrent metadata changes must never be discarded.
func TestHierarchyEnvironmentFinalizationRetiresOnlyIdleMetadata(t *testing.T) {
	for _, scenario := range []string{"idle", "configured", "active-schedule", "foreign-owner", "retained-key", "policy-race", "coordination-race"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			environmentID := fixture.environment.Record.ID
			policyKey, coordinationKey := backuppolicy.BackupPolicyKey(
				environmentID,
			), environmentcoordination.Key(
				environmentID,
			)
			policy := backuppolicy.BackupPolicyRecord{EnvironmentID: environmentID, UpdatedAt: fixture.now}
			coordination := environmentcoordination.EnvironmentCoordinationRecord{
				EnvironmentID: environmentID, ScheduleClockFloor: fixture.now,
			}
			if scenario == "configured" {
				policy.Frequency, policy.Keep, policy.Encryption = "*-*-* 00:00:00", 3, "none"
				policy.ConnectorID = ids.NewAt(ids.KindConnector, fixture.now, 9901)
			}
			if scenario == "foreign-owner" {
				coordination.EnvironmentID = ids.NewAt(ids.KindEnvironment, fixture.now, 9902)
			}
			if scenario == "active-schedule" {
				coordination.CurrentBackupScheduleState = &environmentcoordination.CurrentBackupScheduleState{
					PolicyDigest: strings.Repeat("a", 64), Frequency: "*-*-* 00:00:00",
					EnabledAt: fixture.now, LastEvaluatedAt: fixture.now, UpdatedAt: fixture.now,
				}
			}
			policyValue, err := backuppolicy.EncodeBackupPolicyRecord(policy)
			if err != nil {
				t.Fatal(err)
			}
			coordinationValue, err := environmentcoordination.Encode(coordination)
			if err != nil {
				t.Fatal(err)
			}
			fixture.putRaw(t, policyKey, policyValue)
			fixture.putRaw(t, coordinationKey, coordinationValue)
			if scenario == "retained-key" {
				fixture.putRaw(t, backuppolicy.BackupKeyKey(environmentID), []byte("retained authority"))
			}
			primary, err := fixture.store.Get(ctx, hierarchy.EnvironmentKey(environmentID))
			if err != nil {
				t.Fatal(err)
			}
			operation := hierarchydeletion.HierarchyDeletionOperation{
				TombstoneRevision: fixture.store.revision,
				Tombstone: hierarchydeletion.HierarchyDeletionTombstone{
					TargetKind: hierarchydeletion.HierarchyDeletionTargetEnvironment,
					TargetID:   environmentID, OperationID: fixture.task.OperationID,
				},
			}
			effects, err := hierarchydeletionfinalization.NewPreparer(fixture.store).Prepare(ctx, operation,
				hierarchydeletion.HierarchyDeletionAction{
					ActionKind: hierarchydeletion.HierarchyDeletionEnvironmentFinalize,
					TargetID:   environmentID, TargetRevision: primary.Entry.ModRevision,
				})
			blocked := scenario == "configured" || scenario == "active-schedule" || scenario == "foreign-owner" ||
				scenario == "retained-key"
			if blocked {
				if err == nil {
					t.Fatal("retained authority was accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer keyvalue.ClearByteSlices(effects.Values())
				if scenario == "policy-race" {
					fixture.putRaw(t, policyKey, policyValue)
				}
				if scenario == "coordination-race" {
					fixture.putRaw(t, coordinationKey, coordinationValue)
				}
				result, err := fixture.store.Transact(ctx, effects.Conditions(), effects.Mutations())
				if err != nil {
					t.Fatal(err)
				}
				if result.Succeeded != (scenario == "idle") {
					t.Fatalf("transaction succeeded = %t", result.Succeeded)
				}
			}
			for _, key := range []string{hierarchy.EnvironmentKey(environmentID), policyKey, coordinationKey} {
				value, err := fixture.store.Get(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if (value.Entry == nil) != (scenario == "idle") {
					t.Fatalf("unexpected survival of %s", key)
				}
			}
		})
	}
}
