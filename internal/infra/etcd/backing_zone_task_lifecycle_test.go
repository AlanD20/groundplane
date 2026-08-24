package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: every failed backing-Zone parent must restore visibility, and a
// normal Task retry must atomically reacquire the same creation fence.
func TestBackingZoneCascadeFailureAndRetryTransferTheFence(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 23, 15, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	zoneID := ids.NewAt(ids.KindNetwork, now, 3)
	zone := ZoneRecord{
		EnvironmentID: environmentID,
		Desired: core.Zone{
			ID: zoneID, Name: "backing", Subnet: "10.60.10.0/24",
			OwnerKind: core.ZoneOwnerBackingProject, OwnerID: projectID,
		},
	}
	zoneValue, err := encodeZoneRecord(zone)
	if err != nil {
		t.Fatalf("encodeZoneRecord() error = %v", err)
	}
	seed, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: zoneKey(zoneID), Value: zoneValue}})
	clear(zoneValue)
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Zone = %#v/%v", seed, err)
	}
	parent := validTaskRecord(now.Add(time.Second))
	parent.Executor = TaskExecutorController
	parent.Type = TaskRemove
	parent.Target = zoneID
	parent.Params = map[string]string{
		TaskResourceKindParam:    TaskResourceBackingZone,
		TaskZoneEnvironmentParam: environmentID,
		TaskZoneImpactTokenParam: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	parent.TimeoutSeconds = 24 * 60 * 60
	createLifecycleTask(t, tasks, parent)
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetZone, TargetID: zoneID, TargetRevision: seed.Revision,
		TaskID: parent.ID, Phase: DeletionPhaseHostEffects,
		CreatedAt: parent.CreatedAt, UpdatedAt: parent.CreatedAt,
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	if result, putErr := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetZone), zoneID), Value: tombstoneValue,
	}}); putErr != nil || !result.Succeeded {
		clear(tombstoneValue)
		t.Fatalf("seed tombstone = %#v/%v", result, putErr)
	}
	clear(tombstoneValue)
	if _, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(2*time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, parent.ID, TaskStatusFailed, now.Add(3*time.Second)); err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
	}
	stored, err := store.Get(ctx, deletionTombstoneKey(string(DeletionTargetZone), zoneID))
	if err != nil || stored.Entry != nil {
		t.Fatalf("failed cascade tombstone = %#v/%v", stored, err)
	}
	retryID := ids.NewAt(ids.KindTask, now.Add(4*time.Second), 40)
	retryMarker := pendingRetryMarker(parent, retryID, now.Add(4*time.Second), "backing-zone-retry-key-0001")
	if _, err := tasks.RetryTask(ctx, parent.ID, retryID, TaskActorOperator, retryMarker); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	stored, err = store.Get(ctx, deletionTombstoneKey(string(DeletionTargetZone), zoneID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("retry cascade tombstone = %#v/%v", stored, err)
	}
	retried, err := decodeDeletionTombstone(stored.Entry.Value)
	if err != nil || retried.TaskID != retryID || retried.TargetRevision != seed.Revision {
		t.Fatalf("retry cascade tombstone = %#v/%v", retried, err)
	}
}
