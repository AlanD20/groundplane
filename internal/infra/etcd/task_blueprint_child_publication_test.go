package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a private Agent child must be published with its durable unit
// claim and lifecycle marker, so it can neither duplicate a ready effect nor
// become an unfinishable running Task.
func TestPublishBlueprintChildClaimsOneReadyUnitWithLifecycle(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 80)
	parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 81)
	parent.Target = parent.Owner.EnvironmentID
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Steps = nil
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
	parentRef, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 82)}
	unit := blueprintunits.Unit{
		Target:      service,
		Fingerprint: strings.Repeat("b", 64),
		Writes:      []blueprintunits.ResourceKey{service},
	}
	plan, err := blueprintunits.EncodeDesiredPlan(blueprintunits.DesiredPlan{
		EnvironmentID: parent.Owner.EnvironmentID, ParentTaskID: parent.ID, Complete: true,
		Units: []blueprintunits.Unit{unit},
	})
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := blueprintunits.EncodeEpoch(
		blueprintunits.EpochRecord{EnvironmentID: parent.Owner.EnvironmentID, Sequence: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	absent, err := blueprintunits.EncodeApplied(blueprintunits.AppliedRecord{
		EnvironmentID: parent.Owner.EnvironmentID, Target: service, State: blueprintunits.Absent,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
		{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: parentRef},
		{Type: keyvalue.MutationPut, Key: blueprintunits.DesiredPlanKey(parent.Owner.EnvironmentID), Value: plan},
		{Type: keyvalue.MutationPut, Key: blueprintunits.AppliedKey(parent.Owner.EnvironmentID, service), Value: absent},
		{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epoch},
	}); err != nil || !tx.Succeeded {
		t.Fatalf("seed parent and plan = %#v, %v", tx, err)
	}
	child := validTaskRecord(now)
	child.ID = ids.NewAt(ids.KindTask, now, 83)
	child.OperationID = ids.NewAt(ids.KindOperation, now, 84)
	child.PlanID = ids.NewAt(ids.KindPlan, now, 85)
	child.IdempotencyKey = "blueprint-child-83"
	child.Actor = taskjournal.TaskActorSystem
	child.Owner = parent.Owner
	child.Target = parent.Owner.EnvironmentID
	child.Params = map[string]string{
		taskjournal.TaskBlueprintParentParam:       parent.ID,
		blueprints.EnvironmentDesiredRevisionParam: parent.ID,
	}
	marker := pendingTaskMarker(child)
	marker.Locator.ScopeID = parent.Owner.EnvironmentID
	created, err := repository.PublishBlueprintChild(ctx, parent.ID, child, unit, marker)
	if err != nil || created.Record.ID != child.ID {
		t.Fatalf("publish child = %#v, %v", created, err)
	}
	if _, err := repository.PublishBlueprintChild(ctx, parent.ID, child, unit, marker); err == nil {
		t.Fatal("same ready unit was published twice")
	}
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil || len(snapshot.Executions) != 1 || snapshot.Executions[0].Record.TaskID != child.ID {
		t.Fatalf("durable child execution = %#v, %v", snapshot.Executions, err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskjournal.TaskQueueKey(child.Executor, child.ID), markerKey,
	}})
	if err != nil || read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		t.Fatalf("child queue and terminal marker = %#v, %v", read, err)
	}
	newHead := ids.NewAt(ids.KindTask, now, 86)
	newHeadRef, err := idempotency.EncodeTaskReference(newHead)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: newHeadRef,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("supersede desired head = %#v, %v", tx, err)
	}
	aborted, err := repository.AbortPendingTask(ctx, child.ID, now.Add(2*time.Second))
	if err != nil || aborted.Record.Status != taskjournal.TaskStatusAborted {
		t.Fatalf("abort unassigned child = %#v, %v", aborted, err)
	}
	snapshot, err = ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil || len(snapshot.Executions) != 0 {
		t.Fatalf("withdrawn child claim = %#v, %v", snapshot.Executions, err)
	}
}
