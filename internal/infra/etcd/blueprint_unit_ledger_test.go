package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	blueprintunits "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: a late child publication must fail when either a newer Blueprint
// head or another unit mutation overtakes its fixed-revision selection.
func TestBlueprintUnitMutationFencesHeadAndEpoch(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 30)
	parentID := ids.NewAt(ids.KindTask, now, 31)
	childID := ids.NewAt(ids.KindTask, now, 32)
	planID := ids.NewAt(ids.KindPlan, now, 33)
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 34)}
	headValue, err := idempotencyrecord.EncodeTaskReference(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headValue,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("seed Blueprint head = %#v, %v", tx, err)
	}
	initial, err := ledger.Load(ctx, environmentID)
	if err != nil || initial.HeadTaskID != parentID || initial.EpochRevision != 0 {
		t.Fatalf("initial unit snapshot = %#v, %v", initial, err)
	}
	absent := blueprintunits.AppliedRecord{
		EnvironmentID: environmentID, Target: service, State: blueprintunits.Absent,
	}
	execution := blueprintunits.ExecutionRecord{
		EnvironmentID: environmentID, ParentTaskID: parentID, TaskID: childID, PlanID: planID,
		Unit: blueprintunits.Unit{
			Target: service, Fingerprint: strings.Repeat("a", 64), Writes: []blueprintunits.ResourceKey{service},
		},
		State: blueprintunits.Pending,
	}
	plan, err := blueprintunits.PrepareMutation(initial,
		[]blueprintunits.AppliedChange{{Target: service, Next: &absent}},
		[]blueprintunits.ExecutionChange{{PlanID: planID, Next: &execution}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Clear()
	committed, err := store.Transact(ctx, plan.Conditions(), plan.Mutations())
	if err != nil || !committed.Succeeded {
		t.Fatalf("publish unit claim = %#v, %v", committed, err)
	}
	loaded, err := ledger.Load(ctx, environmentID)
	if err != nil || loaded.Epoch.Sequence != 1 || len(loaded.Applied) != 1 ||
		len(loaded.Executions) != 1 || loaded.Executions[0].Record.TaskID != childID {
		t.Fatalf("durable unit claim = %#v, %v", loaded, err)
	}
	rewritten := execution
	rewritten.Unit.Fingerprint = strings.Repeat("b", 64)
	if _, err := blueprintunits.PrepareMutation(loaded, nil,
		[]blueprintunits.ExecutionChange{{PlanID: planID, Next: &rewritten}}); err == nil {
		t.Fatal("existing child plan was rewritten")
	}
	if _, err := blueprintunits.PrepareMutation(loaded,
		[]blueprintunits.AppliedChange{{Target: service, Next: &absent}}, nil); err == nil {
		t.Fatal("existing resource was declared initially absent again")
	}
	forged := blueprintunits.AppliedRecord{
		EnvironmentID: environmentID, Target: service, State: blueprintunits.Applied,
		Fingerprint: execution.Unit.Fingerprint, ParentTaskID: parentID, SourceTaskID: childID, SourcePlanID: planID,
		SourceAssignment: ids.NewAt(ids.KindAssignment, now, 36), ExecutionEpoch: 1,
	}
	if _, err := blueprintunits.PrepareMutation(loaded,
		[]blueprintunits.AppliedChange{{Target: service, Next: &forged}}, nil); err == nil {
		t.Fatal("pending child supplied an applied receipt without an execution epoch")
	}
	stale, err := store.Transact(ctx, plan.Conditions(), plan.Mutations())
	if err != nil || stale.Succeeded {
		t.Fatalf("stale unit claim repeated = %#v, %v", stale, err)
	}
	next := execution
	next.State, next.Epoch = blueprintunits.Running, 1
	fromLatest, err := blueprintunits.PrepareMutation(loaded, nil,
		[]blueprintunits.ExecutionChange{{PlanID: planID, Next: &next}})
	if err != nil {
		t.Fatal(err)
	}
	defer fromLatest.Clear()
	newHead, err := idempotencyrecord.EncodeTaskReference(ids.NewAt(ids.KindTask, now, 35))
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), Value: newHead,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("publish newer Blueprint head = %#v, %v", tx, err)
	}
	overtaken, err := store.Transact(ctx, fromLatest.Conditions(), fromLatest.Mutations())
	if err != nil || overtaken.Succeeded {
		t.Fatalf("overtaken unit claim committed = %#v, %v", overtaken, err)
	}
	newSnapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blueprintunits.PrepareMutation(newSnapshot, nil,
		[]blueprintunits.ExecutionChange{{PlanID: planID, Next: &next}}); err == nil {
		t.Fatal("pending child from an older Blueprint head was admitted to run")
	}
	newParentID, err := idempotencyrecord.DecodeTaskReference(newHead)
	if err != nil {
		t.Fatal(err)
	}
	newExecution := execution
	newExecution.ParentTaskID = newParentID
	newExecution.TaskID = ids.NewAt(ids.KindTask, now, 37)
	newExecution.PlanID = ids.NewAt(ids.KindPlan, now, 38)
	newExecution.Unit.Fingerprint = strings.Repeat("c", 64)
	replace, err := blueprintunits.PrepareMutation(newSnapshot, nil, []blueprintunits.ExecutionChange{
		{PlanID: planID}, {PlanID: newExecution.PlanID, Next: &newExecution},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replace.Clear()
	if tx, err := store.Transact(ctx, replace.Conditions(), replace.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("replace obsolete pending child = %#v, %v", tx, err)
	}
	newPending, err := ledger.Load(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	newRunning := newExecution
	newRunning.State, newRunning.Epoch = blueprintunits.Running, 1
	start, err := blueprintunits.PrepareMutation(newPending, nil,
		[]blueprintunits.ExecutionChange{{PlanID: newRunning.PlanID, Next: &newRunning}})
	if err != nil {
		t.Fatal(err)
	}
	defer start.Clear()
	if tx, err := store.Transact(ctx, start.Conditions(), start.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("start latest child = %#v, %v", tx, err)
	}
	runningSnapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blueprintunits.PrepareMutation(runningSnapshot,
		[]blueprintunits.AppliedChange{{Target: service, Next: &forged}}, nil); err == nil {
		t.Fatal("obsolete child supplied the latest applied receipt")
	}
	acknowledged := forged
	acknowledged.Fingerprint = newExecution.Unit.Fingerprint
	acknowledged.ParentTaskID = newExecution.ParentTaskID
	acknowledged.SourceTaskID = newExecution.TaskID
	acknowledged.SourcePlanID = newExecution.PlanID
	ack, err := blueprintunits.PrepareMutation(runningSnapshot,
		[]blueprintunits.AppliedChange{{Target: service, Next: &acknowledged}},
		[]blueprintunits.ExecutionChange{{PlanID: newExecution.PlanID}})
	if err != nil {
		t.Fatal(err)
	}
	defer ack.Clear()
	if tx, err := store.Transact(ctx, ack.Conditions(), ack.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("acknowledge latest child = %#v, %v", tx, err)
	}
	settled, err := ledger.Load(ctx, environmentID)
	if err != nil || len(settled.Executions) != 0 || len(settled.Applied) != 1 ||
		settled.Applied[0].Record.Fingerprint != newExecution.Unit.Fingerprint {
		t.Fatalf("settled child receipt = %#v, %v", settled, err)
	}
}

// Rationale: one Apply may produce Attach facts before its remaining units can
// be fixed. An incomplete plan is not an empty successful plan, and sealing
// must fence both the current head and the earlier planning epoch.
func TestBlueprintDesiredUnitPlanSealsAfterFactDependentExpansion(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 70)
	parentID := ids.NewAt(ids.KindTask, now, 71)
	headValue, err := idempotencyrecord.EncodeTaskReference(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headValue,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("seed desired head = %#v, %v", tx, err)
	}
	initial, err := ledger.Load(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	attach := blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: ids.NewAt(ids.KindAttach, now, 72)}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 73)}
	first := blueprintunits.DesiredPlan{
		EnvironmentID: environmentID, ParentTaskID: parentID,
		Units: []blueprintunits.Unit{
			{Target: attach, Writes: []blueprintunits.ResourceKey{attach}, Fingerprint: strings.Repeat("a", 64)},
		},
	}
	prepared, err := blueprintunits.PrepareDesiredPlan(initial, first)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	if tx, err := store.Transact(ctx, prepared.Conditions(), prepared.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("publish incomplete plan = %#v, %v", tx, err)
	}
	loaded, err := ledger.Load(ctx, environmentID)
	if err != nil || loaded.Desired == nil || loaded.Desired.Record.Complete || len(loaded.Desired.Record.Units) != 1 {
		t.Fatalf("incomplete plan = %#v, %v", loaded.Desired, err)
	}
	final := first
	final.Complete = true
	final.Units = append(append([]blueprintunits.Unit(nil), first.Units...), blueprintunits.Unit{
		Target: service, Writes: []blueprintunits.ResourceKey{service}, Fingerprint: strings.Repeat("b", 64),
		After: []blueprintunits.ResourceKey{attach},
	})
	sealed, err := blueprintunits.PrepareDesiredPlan(loaded, final)
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Clear()
	if tx, err := store.Transact(ctx, sealed.Conditions(), sealed.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("seal expanded plan = %#v, %v", tx, err)
	}
	if tx, err := store.Transact(ctx, prepared.Conditions(), prepared.Mutations()); err != nil || tx.Succeeded {
		t.Fatalf("stale plan overwrote expansion = %#v, %v", tx, err)
	}
	current, err := ledger.Load(ctx, environmentID)
	if err != nil || current.Desired == nil || !current.Desired.Record.Complete ||
		len(current.Desired.Record.Units) != 2 {
		t.Fatalf("sealed plan = %#v, %v", current.Desired, err)
	}
	if _, err := blueprintunits.PrepareDesiredPlan(current, first); err == nil {
		t.Fatal("sealed plan was reopened")
	}
}
