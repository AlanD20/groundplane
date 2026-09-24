package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: an empty current plan is not successful until explicitly sealed;
// sealing only releases the parent after all desired effects are acknowledged.
func TestCompleteBlueprintParentRequiresSealedSatisfiedPlan(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 70)
	parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 71)
	parent.Target = parent.Owner.EnvironmentID
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Steps = nil
	marker := pendingTaskMarker(parent)
	parent.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	parent, err = TransitionTaskStatus(
		parent,
		taskjournal.TaskStatusPending,
		taskjournal.TaskStatusRunning,
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	parentValue, err := EncodeTaskRecord(parent)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	parentRef, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan := blueprintunits.DesiredPlan{EnvironmentID: parent.Owner.EnvironmentID, ParentTaskID: parent.ID}
	planValue, err := blueprintunits.EncodeDesiredPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{
		EnvironmentID: parent.Owner.EnvironmentID, Sequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskActiveOperationKey(parent.OperationID), Value: parentRef},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID),
			Value: parentRef,
		},
		{Type: keyvalue.MutationPut, Key: blueprintunits.DesiredPlanKey(parent.Owner.EnvironmentID), Value: planValue},
		{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epochValue},
		{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed parent = %#v, %v", seed, err)
	}
	if _, err := repository.CompleteBlueprintParent(ctx, parent.Owner.EnvironmentID, parent.ID, now.Add(2*time.Second)); err == nil {
		t.Fatal("incomplete plan was treated as success")
	}
	plan.Complete = true
	planValue, err = blueprintunits.EncodeDesiredPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err = blueprintunits.EncodeEpoch(blueprintunits.EpochRecord{
		EnvironmentID: parent.Owner.EnvironmentID, Sequence: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: blueprintunits.DesiredPlanKey(parent.Owner.EnvironmentID), Value: planValue},
		{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epochValue},
	}); err != nil || !tx.Succeeded {
		t.Fatalf("seal plan = %#v, %v", tx, err)
	}
	completed, err := repository.CompleteBlueprintParent(
		ctx,
		parent.Owner.EnvironmentID,
		parent.ID,
		now.Add(3*time.Second),
	)
	if err != nil || completed.Record.Status != taskjournal.TaskStatusCompleted {
		t.Fatalf("complete sealed parent = %#v, %v", completed, err)
	}
	if _, err := repository.CompleteBlueprintParent(ctx, parent.Owner.EnvironmentID, parent.ID, now.Add(4*time.Second)); err == nil {
		t.Fatal("completed parent was completed twice")
	}
}
