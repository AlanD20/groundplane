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
}
